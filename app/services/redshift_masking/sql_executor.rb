# frozen_string_literal: true

module RedshiftMasking
  # Executes SQL commands for creating masking policies and applying policy attachment corrections
  class SqlExecutor
    # Use string keys to avoid Zeitwerk autoloading order issues
    POLICY_USING_CLAUSES = {
      'masked' => "('XXXX'::%<type>s)",
      'allowed' => '(value)',
      'denied' => '(NULL::%<type>s)',
    }.freeze

    POLICY_TEMPLATES = POLICY_USING_CLAUSES.transform_values do |using_clause|
      "CREATE MASKING POLICY %<name>s IF NOT EXISTS WITH(value %<type>s) USING #{using_clause}"
    end.freeze

    attr_reader :config

    def initialize(config)
      @config = config
    end

    def create_masking_policies(column_types)
      return if column_types.empty?

      Rails.logger.info('creating masking policies')

      sql_parts = column_types.flat_map do |column_id, data_type|
        POLICY_TEMPLATES.keys.map do |perm_type|
          build_policy_sql(perm_type, column_id, data_type)
        end
      end

      sql = "#{sql_parts.join(";\n")};"

      Rails.logger.info("created/verified policies for #{column_types.size} columns")
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

    def build_policy_sql(permission_type, column_id, data_type)
      format(
        POLICY_TEMPLATES[permission_type],
        name: config.policy_name(permission_type, column_id),
        type: data_type,
      )
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

      connection.quote_column_name(grantee)
    end
  end
end
