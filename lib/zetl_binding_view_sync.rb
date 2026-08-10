# frozen_string_literal: true

require 'yaml'

# Creates/refreshes late-binding views in the analytics warehouse's
# `idp_curated_views` schema from the `public` schema of the read-only `analytics_zetl` database.
class ZetlBindingViewSync
  DEFAULT_EXCLUSION_CONFIG_PATH =
    Rails.root.join('config', 'zetl_column_config.yaml').freeze

  TARGET_SCHEMA = 'idp_curated_views'
  SOURCE_DATABASE = 'analytics_zetl'
  SOURCE_SCHEMA = 'public'

  # Redshift/Postgres unquoted identifier shape. names are lowercase snake_case.
  IDENTIFIER = /\A[a-z_][a-z0-9_]*\z/

  class InvalidIdentifierError < StandardError; end

  def initialize(
    target_schema: TARGET_SCHEMA,
    source_database: SOURCE_DATABASE,
    source_schema: SOURCE_SCHEMA,
    exclusion_config_path: DEFAULT_EXCLUSION_CONFIG_PATH
  )
    @target_schema = target_schema
    @source_database = source_database
    @source_schema = source_schema
    @exclusion_config_path = exclusion_config_path
  end

  # CREATE OR REPLACE a late-binding view per source table.
  def sync
    [target_schema, source_database, source_schema].each { |id| validate_identifier!(id) }

    excluded = excluded_columns
    tables = source_tables

    log_info(
      "Starting idp_curated_views binding view sync " \
      "(#{tables.size} source tables from #{source_database}.#{source_schema})",
    )
    create_schema

    results = { created: 0, skipped: 0, failed: 0 }
    intended_views = []
    tables.each do |table_name, all_columns|
      columns = all_columns - Array(excluded[table_name])

      if columns.empty?
        results[:skipped] += 1
        log_info("Skipping '#{table_name}': all columns excluded")
        next
      end

      create_view(table_name, columns)
      intended_views << table_name
      results[:created] += 1
    rescue StandardError => e
      results[:failed] += 1
      log_error("FAILED view '#{table_name}': #{e.class}: #{e.message}")
      raise e
    end

    results[:stale] = warn_stale_views(intended_views).size

    log_info(
      "Completed idp_curated_views binding view sync: " \
      "#{results[:created]} synced, #{results[:skipped]} skipped, #{results[:stale]} stale",
    )
    results
  end

  private

  attr_reader :target_schema, :source_database, :source_schema, :exclusion_config_path

  def warn_stale_views(intended_views)
    stale = existing_views - intended_views
    stale.each do |view_name|
      log_warning(
        "Stale view #{target_schema}.#{view_name}: no matching source table " \
        "in #{source_database}.#{source_schema} (not dropped)",
      )
    end
    stale
  end

  def existing_views
    execute_query(<<~SQL, target_schema).map { |row| row['table_name'] }
      SELECT table_name
      FROM information_schema.views
      WHERE table_schema = ?
    SQL
  end

  def source_tables
    rows = execute_query(<<~SQL, source_database, source_schema)
      SELECT table_name, column_name
      FROM svv_all_columns
      WHERE database_name = ? AND schema_name = ?
      ORDER BY table_name, ordinal_position
    SQL

    rows.each_with_object({}) do |row, acc|
      (acc[row['table_name']] ||= []) << row['column_name']
    end
  end

  def excluded_columns
    return {} unless File.exist?(exclusion_config_path)

    config = YAML.safe_load_file(exclusion_config_path) || {}
    config.fetch('exclude_columns', {}) || {}
  end

  def create_schema
    execute(%(CREATE SCHEMA IF NOT EXISTS "#{target_schema}";))
    log_info("Ensured schema exists: #{target_schema}")
  end

  def create_view(table_name, columns)
    validate_identifier!(table_name)
    columns.each { |c| validate_identifier!(c) }
    column_list = columns.map { |c| %("#{c}") }.join(', ')

    sql = <<~SQL
      CREATE OR REPLACE VIEW "#{target_schema}"."#{table_name}" AS
      SELECT #{column_list}
      FROM "#{source_database}"."#{source_schema}"."#{table_name}"
      WITH NO SCHEMA BINDING;
    SQL

    execute(sql)
    log_info("Synced view #{target_schema}.#{table_name} (#{columns.size} columns)")
  end

  def validate_identifier!(value)
    return if value.is_a?(String) && value.match?(IDENTIFIER)

    raise InvalidIdentifierError, "invalid SQL identifier: #{value.inspect}"
  end

  def execute(sql)
    DataWarehouseApplicationRecord.connection.execute(sql)
  end

  def execute_query(query, *args)
    DataWarehouseApplicationRecord.connection.exec_query(
      DataWarehouseApplicationRecord.sanitize_sql([query, *args]),
    ).to_a
  end

  def log_message(level, message)
    logger.send(level, { name: self.class.name, level => message }.to_json)
  end

  def log_info(message)
    log_message(:info, message)
  end

  def log_error(message)
    log_message(:error, message)
  end

  def log_warning(message)
    log_message(:warn, message)
  end

  def logger
    @logger ||= IdentityJobLogSubscriber.new.logger
  end
end
