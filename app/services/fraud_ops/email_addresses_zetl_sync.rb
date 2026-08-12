# frozen_string_literal: true

module FraudOps
  # Maintains a Zero-ETL-sourced twin of fraudops.frd_email_addresses.
  class EmailAddressesZetlSync
    SCHEMA_NAME = 'fraudops'
    TARGET_TABLE = 'frd_email_addresses_zetl'
    STAGING_TABLE = 'frd_email_addresses_zetl_staging'
    CURATED_VIEW = 'idp_curated_views.email_addresses'
    MERGE_KEY = 'id'
    DEFAULT_LOOKBACK_MINUTES = 15

    def sync
      unless target_table_exists?
        Rails.logger.info(
          "#{qualified(TARGET_TABLE)} does not exist, skipping sync. " \
          "Run rake fraudops:bootstrap_email_addresses_zetl to create it.",
        )
        return { skipped: true }
      end

      minutes = lookback_minutes
      cutoff = merge_delta(minutes)

      { skipped: false, cutoff: cutoff.iso8601, lookback_minutes: minutes }
    end

    private

    # Read at call time rather than in a constant so the value tracks config
    # without a restart.
    def lookback_minutes
      IdentityConfig.store.zero_etl_email_addresses_lookback_minutes || DEFAULT_LOOKBACK_MINUTES
    end

    def merge_delta(lookback_minutes)
      cutoff = lookback_minutes.minutes.ago.utc

      DataWarehouseApplicationRecord.transaction do
        connection.execute(drop_staging_table_query)
        connection.execute(create_staging_table_query)
        connection.execute(load_staging_table_query(cutoff))
        connection.execute(merge_staging_into_target_query)
      end

      connection.execute(drop_staging_table_query)

      cutoff
    end

    def target_table_exists?
      connection.table_exists?(qualified(TARGET_TABLE))
    end

    def drop_staging_table_query
      format(<<~SQL.squish, build_params)
        DROP TABLE IF EXISTS %{staging_table}
      SQL
    end

    def create_staging_table_query
      format(<<~SQL.squish, build_params)
        CREATE TABLE %{staging_table} (LIKE %{target_table} INCLUDING DEFAULTS)
      SQL
    end

    def load_staging_table_query(cutoff)
      params = build_params.merge(cutoff: connection.quote(cutoff))

      format(<<~SQL.squish, params)
        INSERT INTO %{staging_table}
          (id, encrypted_email, user_id, email, dw_created_at, dw_updated_at)
        SELECT
          curated.id,
          curated.encrypted_email,
          curated.user_id,
          %{schema_name}.decrypt_udf(curated.encrypted_email, curated.id),
          CURRENT_TIMESTAMP,
          CURRENT_TIMESTAMP
        FROM %{curated_view} curated
        WHERE curated.updated_at >= %{cutoff}
      SQL
    end

    def merge_staging_into_target_query
      format(<<~SQL.squish, build_params)
        MERGE INTO %{target_table}
        USING %{staging_table} AS source
          ON %{target_table}.%{merge_key} = source.%{merge_key}
        WHEN MATCHED THEN
          UPDATE SET
            encrypted_email = source.encrypted_email,
            user_id = source.user_id,
            email = source.email,
            dw_updated_at = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
          INSERT (id, encrypted_email, user_id, email, dw_created_at, dw_updated_at)
          VALUES (
            source.id,
            source.encrypted_email,
            source.user_id,
            source.email,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
          )
      SQL
    end

    def build_params
      {
        schema_name: SCHEMA_NAME,
        target_table: qualified(TARGET_TABLE),
        staging_table: qualified(STAGING_TABLE),
        curated_view: CURATED_VIEW,
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
