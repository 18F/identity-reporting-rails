# frozen_string_literal: true

module FraudOps
  # One-time backfill of fraudops.frd_email_addresses_zetl from the Zero-ETL replica
  # of the IdP email_addresses table.
  class EmailAddressesZeroEtlBackfill
    SCHEMA_NAME = 'fraudops'
    SOURCE_TABLE = "#{IdentityConfig.store.redshift_database_zero_etl_name}.public.email_addresses"
    TARGET_TABLE = 'frd_email_addresses_zetl'
    MATCH_KEY = 'id'

    def initialize(zetl_cutoff_datetime:)
      @zetl_cutoff_datetime = zetl_cutoff_datetime
    end

    def backfill
      unless target_table_exists?
        Rails.logger.info("#{qualified(TARGET_TABLE)} does not exist, nothing to do")
        return false
      end

      seed_target_table

      Rails.logger.info("Seeded #{qualified(TARGET_TABLE)} from #{SOURCE_TABLE}")

      true
    end

    private

    attr_reader :zetl_cutoff_datetime

    def target_table_exists?
      connection.table_exists?(qualified(TARGET_TABLE))
    end

    def seed_target_table
      DataWarehouseApplicationRecord.transaction do
        connection.execute(insert_target_table_query)
      end
    end

    def insert_target_table_query
      format(<<~SQL.squish, build_params)
        INSERT INTO %{target_table} (id, encrypted_email, user_id, email, dw_created_at, dw_updated_at)
        SELECT
          source.id,
          source.encrypted_email,
          source.user_id,
          %{schema_name}.decrypt_udf(source.encrypted_email, source.id),
          CURRENT_TIMESTAMP,
          CURRENT_TIMESTAMP
        FROM %{source_table} AS source
        WHERE source.created_at < %{cutoff}
          AND NOT EXISTS (
            SELECT 1
            FROM %{target_table} AS target
            WHERE target.%{match_key} = source.%{match_key}
          )
      SQL
    end

    def build_params
      {
        schema_name: SCHEMA_NAME,
        source_table: SOURCE_TABLE,
        target_table: qualified(TARGET_TABLE),
        match_key: MATCH_KEY,
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
