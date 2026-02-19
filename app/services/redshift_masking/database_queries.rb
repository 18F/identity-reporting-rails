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

      # Safe: All values properly escaped using connection.quote()
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
end
