# frozen_string_literal: true

# Maintains a Zero-ETL-sourced twin of fraudops.frd_email_addresses.
#
# The zero-ETL replica lands IDP tables in the analytics_zetl database, which is
# exposed to analytics through the idp_curated_views schema (WITH NO SCHEMA
# BINDING cross-database views). This job creates
# fraudops.frd_email_addresses_zetl as a copy of the legacy table, then keeps it
# current by merging deltas from idp_curated_views.email_addresses every 15
# minutes, decrypting each email via the fraudops.decrypt_udf Lambda UDF.
#
# Runs against DataWarehouseApplicationRecord (the analytics database) rather
# than DataWarehouseApplicationRecordZetl: the curated view is readable from
# analytics, and the zetl connection is a read-only replica that cannot run DDL.
class FraudOpsEmailAddressesZetlJob < ApplicationJob
  include GoodJob::ActiveJobExtensions::Concurrency

  queue_as :default

  # A slow run must not overlap the next scheduled one and merge twice.
  good_job_control_concurrency_with(
    total_limit: 1,
  )

  SCHEMA_NAME = 'fraudops'
  SOURCE_TABLE = 'frd_email_addresses'
  TARGET_TABLE = 'frd_email_addresses_zetl'
  STAGING_TABLE = 'frd_email_addresses_zetl_staging'
  CURATED_VIEW = 'idp_curated_views.email_addresses'
  MERGE_KEY = 'id'

  def perform(lookback_minutes: 15)
    unless IdentityConfig.store.zero_etl_enabled
      return log_message(:info, 'zero_etl_enabled is false, skipping job.', false)
    end

    log_message(:info, 'Job started.', true, { lookback_minutes: lookback_minutes })

    bootstrap_target_table
    merge_delta(lookback_minutes)

    log_message(:info, 'Job completed.', true)
  rescue => e
    log_message(:error, 'Job failed.', false, { error: e.message })
    raise
  end

  private

  # Step 1: create the target table as a copy of the legacy table and seed it
  # with the existing rows. Only runs once subsequent runs are merge-only.
  def bootstrap_target_table
    if target_table_exists?
      log_message(:info, 'Target table already exists, skipping bootstrap.', true)
      return
    end

    DataWarehouseApplicationRecord.transaction do
      connection.execute(create_target_table_query)
      connection.execute(add_primary_key_query)
      connection.execute(seed_target_table_query)
    end

    log_message(
      :info, 'Bootstrapped target table.', true,
      { target_table: qualified(TARGET_TABLE) }
    )
  end

  # Steps 2 and 3: decrypt deltas into a staging table, then merge it
  def merge_delta(lookback_minutes)
    cutoff = lookback_minutes.minutes.ago.utc

    DataWarehouseApplicationRecord.transaction do
      connection.execute(drop_staging_table_query)
      connection.execute(create_staging_table_query)
      connection.execute(load_staging_table_query(cutoff))
      connection.execute(merge_staging_into_target_query)
    end

    connection.execute(drop_staging_table_query)

    log_message(:info, 'Merge completed.', true, { cutoff: cutoff.iso8601 })
  end

  def target_table_exists?
    connection.table_exists?(qualified(TARGET_TABLE))
  end

  # Redshift LIKE copies column names, types, compression encodings and the
  # distribution/sort keys, but not the PRIMARY KEY or FOREIGN KEY.
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
    using_redshift_adapter? ? redshift_merge_query : postgres_upsert_query
  end

  def redshift_merge_query
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

  # Local development and specs run PostgreSQL.
  def postgres_upsert_query
    format(<<~SQL.squish, build_params)
      WITH updated AS (
        UPDATE %{target_table}
        SET
          encrypted_email = source.encrypted_email,
          user_id = source.user_id,
          email = source.email,
          dw_updated_at = CURRENT_TIMESTAMP
        FROM %{staging_table} source
        WHERE %{target_table}.%{merge_key} = source.%{merge_key}
        RETURNING %{target_table}.%{merge_key}
      )
      INSERT INTO %{target_table}
        (id, encrypted_email, user_id, email, dw_created_at, dw_updated_at)
      SELECT
        source.id,
        source.encrypted_email,
        source.user_id,
        source.email,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
      FROM %{staging_table} source
      WHERE NOT EXISTS (
        SELECT 1 FROM updated WHERE updated.%{merge_key} = source.%{merge_key}
      )
    SQL
  end

  def build_params
    {
      schema_name: SCHEMA_NAME,
      source_table: qualified(SOURCE_TABLE),
      target_table: qualified(TARGET_TABLE),
      staging_table: qualified(STAGING_TABLE),
      curated_view: CURATED_VIEW,
      merge_key: MERGE_KEY,
    }
  end

  def qualified(table_name)
    "#{SCHEMA_NAME}.#{table_name}"
  end

  def using_redshift_adapter?
    connection.adapter_name.downcase.include?('redshift')
  end

  def connection
    DataWarehouseApplicationRecord.connection
  end
end
