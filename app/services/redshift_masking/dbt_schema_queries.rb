# frozen_string_literal: true

module RedshiftMasking
  # Catalog-only discovery. Never reads application rows or normalizes identifiers.
  class DbtSchemaQueries
    def initialize(connection)
      @connection = connection
    end

    def relations(schema, table: nil)
      @connection.execute(<<~SQL).to_a
        SELECT t.table_name, t.table_type,
               CASE WHEN m.name IS NULL THEN false ELSE true END AS materialized_view
        FROM svv_tables t
        LEFT JOIN svv_mv_info m
          ON m.database_name = t.table_catalog AND m.schema_name = t.table_schema
          AND m.name = t.table_name
        WHERE t.table_catalog = current_database()
          AND t.table_schema = #{@connection.quote(schema)}
          #{"AND t.table_name = #{@connection.quote(table)}" if table}
        ORDER BY t.table_name
      SQL
    end

    def columns(schema, table)
      @connection.execute(<<~SQL).to_a
        SELECT column_name, data_type, character_maximum_length,
               numeric_precision, numeric_scale
        FROM svv_columns
        WHERE table_catalog = current_database()
          AND table_schema = #{@connection.quote(schema)}
          AND table_name = #{@connection.quote(table)}
        ORDER BY ordinal_position
      SQL
    end

    def policy_names
      @connection.execute(<<~SQL).to_a.map { |row| row.fetch('policy_name') }.to_set
        SELECT policy_name FROM svv_masking_policy
        WHERE policy_database = current_database()
      SQL
    end

    def role_exists?(role)
      @connection.execute(<<~SQL).to_a.any?
        SELECT role_name FROM svv_roles WHERE role_name = #{@connection.quote(role)}
      SQL
    end

    def attachments(schema, table)
      rows = @connection.execute(<<~SQL).to_a
        SELECT policy_name, schema_name, table_name, output_columns, input_columns,
               grantor, grantee, grantee_type, priority
        FROM svv_attached_masking_policy
        WHERE schema_name = #{@connection.quote(schema)}
          AND table_name = #{@connection.quote(table)}
      SQL
      rows.map { |row| attachment(row) }
    end

    private

    def attachment(row)
      outputs = JSON.parse(row.fetch('output_columns'))
      inputs = JSON.parse(row.fetch('input_columns'))
      unless outputs.is_a?(Array) && outputs.size == 1 && inputs == outputs
        raise ArgumentError, 'Schema reconciliation requires single-column identity inputs'
      end

      PolicyAttachment.new(
        policy_name: row.fetch('policy_name'), schema: row.fetch('schema_name'),
        table: row.fetch('table_name'), column: outputs.first,
        grantee: row.fetch('grantee'), grantee_type: row.fetch('grantee_type').downcase,
        grantor: row.fetch('grantor'), priority: row.fetch('priority').to_i
      )
    end
  end
end
