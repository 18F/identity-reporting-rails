# frozen_string_literal: true

module RedshiftMasking
  # Handles database queries for fetching column types and existing masking policies
  class DatabaseQueries
    attr_reader :logger

    # Data type normalization mappings
    DATA_TYPE_MAPPINGS = {
      /^(?:character varying|varchar|text)/i => 'VARCHAR(MAX)',
      # Lambda to preserve original length
      /^(?:character|char)$/i => ->(_, len) { "CHAR(#{len || 1})" },
      /^(?:numeric|decimal|integer|int|smallint|bigint|real|double)/i => 'NUMERIC',
      /^date$/i => 'DATE',
      /^timestamp/i => 'TIMESTAMP',
      /^(?:boolean|bool)/i => 'BOOLEAN',
    }.freeze

    def initialize(logger)
      @logger = logger
    end

    def fetch_column_types(columns)
      return {} if columns.empty?

      logger.info("fetching data types for #{columns.size} columns")

      conditions = columns.map do |col|
        "(table_schema = #{connection.quote(col.schema)} " \
        "AND table_name = #{connection.quote(col.table)} " \
        "AND column_name = #{connection.quote(col.column)})"
      end.join("\n OR ")

      sql = <<~SQL
        SELECT table_schema, table_name, column_name, data_type, character_maximum_length
        FROM information_schema.columns
        WHERE #{conditions}
      SQL

      # brakeman:ignore SQLInjection - All values are properly quoted using connection.quote()
      connection.execute(sql).to_a.each_with_object({}) do |row, hash|
        key = "#{row['table_schema']}.#{row['table_name']}.#{row['column_name']}"
        hash[key] = normalize_data_type(
          row['data_type'],
          row['character_maximum_length']&.to_i,
        )
      end
    end

    def fetch_existing_policies
      sql = <<~SQL
        SELECT policy_name, schema_name, table_name,
               JSON_EXTRACT_ARRAY_ELEMENT_TEXT(input_columns, 0) AS column_name,
               grantee, priority
        FROM svv_attached_masking_policy
      SQL

      connection.execute(sql).to_a.map do |row|
        PolicyAttachment.new(
          policy_name: row['policy_name'],
          schema: row['schema_name'],
          table: row['table_name'],
          column: row['column_name'],
          grantee: row['grantee'],
          priority: row['priority'].to_i,
        )
      end
    end

    def fetch_users
      query = 'SELECT usename FROM pg_user'
      connection.execute(query).to_a.map { |row| row['usename'] }
    end

    private

    def connection
      @connection ||= DataWarehouseApplicationRecord.connection
    end

    # Normalize a Redshift data type to a standardized format
    def normalize_data_type(data_type, char_max_length = nil)
      DATA_TYPE_MAPPINGS.each do |pattern, result|
        next unless pattern.match?(data_type)

        return result.respond_to?(:call) ? result.call(data_type, char_max_length) : result
      end

      logger.warn(
        "RedshiftMasking::DatabaseQueries: unknown data type " \
        "'#{data_type}', defaulting to VARCHAR(MAX)",
      )
      'VARCHAR(MAX)'
    end
  end

  # Executes SQL commands for creating masking policies and applying policy attachment corrections
  class SqlExecutor
    POLICY_USING_CLAUSES = {
      Configuration::PERMISSION_MASKED => "('XXXX'::%<type>s)",
      Configuration::PERMISSION_ALLOWED => '(value)',
      Configuration::PERMISSION_DENIED => '(NULL::%<type>s)',
    }.freeze

    POLICY_TEMPLATES = POLICY_USING_CLAUSES.transform_values do |using_clause|
      "CREATE MASKING POLICY %<name>s IF NOT EXISTS WITH(value %<type>s) USING #{using_clause}"
    end.freeze

    attr_reader :config, :logger

    def initialize(config, logger)
      @config = config
      @logger = logger
    end

    def create_masking_policies(column_types)
      return if column_types.empty?

      logger.info('creating masking policies')

      sql_parts = column_types.flat_map do |column_id, data_type|
        Configuration::PERMISSION_TYPES.map do |perm_type|
          build_policy_sql(perm_type, column_id, data_type)
        end
      end

      sql = "#{sql_parts.join(";\n")};"

      logger.info("created/verified policies for #{column_types.size} columns")
      # brakeman:ignore SQLInjection - Policy names/types sanitized via tr() and format() from config files
      connection.execute(sql)
    end

    def apply_corrections(drift)
      to_detach = drift[:extra] + drift[:mismatched].map { |m| m[:actual] }
      to_attach = drift[:missing] + drift[:mismatched].map { |m| m[:expected] }

      return logger.info('no changes needed') if (to_detach + to_attach).empty?

      to_detach.each do |p|
        execute_correction(detach_sql(p), "Detaching #{p.policy_name} from #{p.grantee}")
      end
      to_attach.each do |p|
        execute_correction(attach_sql(p), "Attaching #{p.policy_name} to #{p.grantee}")
      end
    end

    private

    def connection
      @connection ||= DataWarehouseApplicationRecord.connection
    end

    def build_policy_sql(permission_type, column_id, data_type)
      format(
        POLICY_TEMPLATES[permission_type],
        name: config.policy_name(permission_type, column_id),
        type: data_type,
      )
    end

    def execute_correction(sql, description)
      logger.info(description)
      connection.execute(sql)
    rescue ActiveRecord::StatementInvalid => e
      logger.warn("Failed to apply correction: #{e.message}")
      logger.debug("Failed SQL: #{sql}")
    end

    def detach_sql(policy)
      <<~SQL
        DETACH MASKING POLICY #{policy.policy_name}
        ON #{policy.schema}.#{policy.table} (#{policy.column})
        FROM #{quote_grantee(policy.grantee)};
      SQL
    end

    def attach_sql(policy)
      <<~SQL
        ATTACH MASKING POLICY #{policy.policy_name}
        ON #{policy.schema}.#{policy.table} (#{policy.column})
        TO #{quote_grantee(policy.grantee)}
        PRIORITY #{policy.priority};
      SQL
    end

    # Quote grantee with special handling for PUBLIC keyword
    def quote_grantee(grantee)
      return 'PUBLIC' if grantee.upcase == 'PUBLIC'

      connection.quote_table_name(grantee)
    end
  end
end
