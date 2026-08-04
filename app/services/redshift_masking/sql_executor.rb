# frozen_string_literal: true

module RedshiftMasking
  # Executes SQL commands for creating masking policies and applying policy attachment corrections
  class SqlExecutor
    # Use string keys to avoid Zeitwerk autoloading order issues
    POLICY_TEMPLATES = {
      'allowed' => 'CREATE MASKING POLICY %<name>s IF NOT EXISTS ' \
                   'WITH(value %<type>s) USING (value)',
      'denied' => 'CREATE MASKING POLICY %<name>s IF NOT EXISTS ' \
                  'WITH(value %<type>s) USING (NULL::%<type>s)',
    }.freeze

    attr_reader :config

    def initialize(config)
      @config = config
    end

    def create_masking_policies(column_types)
      return if column_types.empty?

      Rails.logger.info('creating masking policies')

      # Policies are created once (db-less) and referenced cross-database by name.
      # Deduplicate by unqualified column id so a column present in multiple
      # databases only produces one CREATE per policy type.
      policy_names = column_types.each_with_object({}) do |(column_id, data_type), hash|
        hash[unqualified_id(column_id)] ||= data_type
      end

      sql_parts = policy_names.flat_map do |unqualified, data_type|
        Configuration::PERMISSION_TYPES.map do |perm_type|
          build_policy_sql(perm_type, unqualified, data_type)
        end
      end

      sql = "#{sql_parts.join(";\n")};"

      Rails.logger.info("created/verified policies for #{policy_names.size} columns")
      # Safe: Policy names/types from config, sanitized via tr() and format()
      connection.execute(sql)
    end

    def apply_corrections(drift)
      to_detach = drift[:extra] + drift[:mismatched].map { |m| m[:actual] }
      to_attach = drift[:missing] + drift[:mismatched].map { |m| m[:expected] }

      return Rails.logger.info('no changes needed') if (to_detach + to_attach).empty?

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

    # Drops the leading database segment from a "db.schema.table.column" id,
    # leaving the "schema.table.column" basis used for policy names.
    def unqualified_id(column_id)
      column_id.split('.', 2).last
    end

    def build_policy_sql(permission_type, column_id, data_type)
      if permission_type == Configuration::PERMISSION_MASKED
        return build_masked_policy_sql(column_id, data_type)
      end

      format(
        POLICY_TEMPLATES[permission_type],
        name: config.policy_name(permission_type, column_id),
        type: data_type,
      )
    end

    def build_masked_policy_sql(column_id, data_type)
      format(
        'CREATE MASKING POLICY %<name>s IF NOT EXISTS WITH(value %<type>s) USING (%<masked>s)',
        name: config.policy_name(Configuration::PERMISSION_MASKED, column_id),
        type: data_type,
        masked: masked_expression_for(data_type),
      )
    end

    def masked_expression_for(data_type)
      normalized_type = data_type.to_s.upcase

      case normalized_type
      when 'TIMESTAMP'
        "'1970-01-01 00:00:00'::#{data_type}"
      when 'DATE'
        "'1970-01-01'::#{data_type}"
      when 'NUMERIC'
        "0::#{data_type}"
      when 'BOOLEAN'
        "FALSE::#{data_type}"
      when 'SUPER'
        "JSON_PARSE('\\\"XXXX\\\"')"
      else
        "'XXXX'::#{data_type}"
      end
    end

    def execute_correction(sql, description)
      Rails.logger.info(description)
      connection.execute(sql)
    rescue ActiveRecord::StatementInvalid => e
      Rails.logger.warn("Failed to apply correction: #{e.message}")
      Rails.logger.debug { "Failed SQL: #{sql}" }
    end

    def detach_sql(policy)
      <<~SQL
        DETACH MASKING POLICY #{qualified_policy_name(policy)}
        ON #{qualified_relation(policy)} (#{policy.column})
        FROM #{quote_grantee(policy.grantee)};
      SQL
    end

    def attach_sql(policy)
      <<~SQL
        ATTACH MASKING POLICY #{qualified_policy_name(policy)}
        ON #{qualified_relation(policy)} (#{policy.column})
        TO #{quote_grantee(policy.grantee)}
        PRIORITY #{policy.priority};
      SQL
    end

    # Policies are created db-less but referenced cross-database as
    # "database.policy_name".
    def qualified_policy_name(policy)
      "#{policy.database}.#{policy.policy_name}"
    end

    # "database.schema.table" for cross-database ATTACH/DETACH.
    def qualified_relation(policy)
      "#{policy.database}.#{policy.schema}.#{policy.table}"
    end

    # Quote grantee with special handling for PUBLIC keyword
    def quote_grantee(grantee)
      return 'PUBLIC' if grantee.upcase == 'PUBLIC'

      connection.quote_column_name(grantee)
    end
  end
end
