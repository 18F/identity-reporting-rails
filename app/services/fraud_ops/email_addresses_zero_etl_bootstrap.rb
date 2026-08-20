# frozen_string_literal: true

module FraudOps
  class EmailAddressesZeroEtlBootstrap
    SCHEMA_NAME = 'fraudops'
    SOURCE_TABLE = 'frd_email_addresses'
    TARGET_TABLE = 'frd_email_addresses_zetl'
    MERGE_KEY = 'id'

    def bootstrap
      if target_table_exists?
        Rails.logger.info("#{qualified(TARGET_TABLE)} already exists, nothing to do")
        return false
      end

      DataWarehouseApplicationRecord.transaction do
        connection.execute(create_target_table_query)
        connection.execute(add_primary_key_query)
        connection.execute(seed_target_table_query)
      end

      Rails.logger.info("Created #{qualified(TARGET_TABLE)} from #{qualified(SOURCE_TABLE)}")

      true
    end

    private

    def target_table_exists?
      connection.table_exists?(qualified(TARGET_TABLE))
    end

    def create_target_table_query
      format(<<~SQL.squish, build_params)
        CREATE TABLE %{target_table} (LIKE %{source_table} INCLUDING DEFAULTS)
      SQL
    end

    def add_primary_key_query
      format(<<~SQL.squish, build_params)
        ALTER TABLE %{target_table} ADD PRIMARY KEY (%{merge_key})
      SQL
    end

    def seed_target_table_query
      format(<<~SQL.squish, build_params)
        INSERT INTO %{target_table} SELECT * FROM %{source_table}
      SQL
    end

    def build_params
      {
        source_table: qualified(SOURCE_TABLE),
        target_table: qualified(TARGET_TABLE),
        merge_key: MERGE_KEY,
      }
    end

    def qualified(table_name)
      "#{SCHEMA_NAME}.#{table_name}"
    end

    def connection
      DataWarehouseApplicationRecord.connection
    end
  end
end
