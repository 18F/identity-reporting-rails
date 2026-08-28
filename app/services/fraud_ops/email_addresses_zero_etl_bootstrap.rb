# frozen_string_literal: true

module FraudOps
  class EmailAddressesZeroEtlBootstrap
    SCHEMA_NAME = 'fraudops'
    SOURCE_TABLE = 'frd_email_addresses'
    TARGET_TABLE = 'frd_email_addresses_zetl'
    INSERT_DB_USER = 'pii_reader'

    def initialize(zetl_cutoff_datetime:)
      @zetl_cutoff_datetime = zetl_cutoff_datetime
    end

    def bootstrap
      unless target_table_exists?
        Rails.logger.info("#{qualified(TARGET_TABLE)} does not exist, nothing to do")
        return false
      end

      seed_target_table

      Rails.logger.info("Seeded #{qualified(TARGET_TABLE)} from #{qualified(SOURCE_TABLE)}")

      true
    end

    private

    attr_reader :zetl_cutoff_datetime

    def target_table_exists?
      connection.table_exists?(qualified(TARGET_TABLE))
    end

    def seed_target_table
      connection.execute(set_session_authorization_query)

      begin
        DataWarehouseApplicationRecord.transaction do
          connection.execute(seed_target_table_query)
        end
      ensure
        connection.execute(reset_session_authorization_query)
      end
    end

    def seed_target_table_query
      format(<<~SQL.squish, build_params)
        INSERT INTO %{target_table} SELECT * FROM %{source_table} WHERE dw_created_at < %{cutoff}
      SQL
    end

    def set_session_authorization_query
      format('SET SESSION AUTHORIZATION %{insert_db_user}', build_params)
    end

    def reset_session_authorization_query
      'RESET SESSION AUTHORIZATION'
    end

    def build_params
      {
        source_table: qualified(SOURCE_TABLE),
        target_table: qualified(TARGET_TABLE),
        insert_db_user: INSERT_DB_USER,
        cutoff: connection.quote(zetl_cutoff_datetime),
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
