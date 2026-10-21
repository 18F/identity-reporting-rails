require 'schema_table_service'

class DuplicateRowCheckerJob < ApplicationJob
  include GoodJob::ActiveJobExtensions::Concurrency
  queue_as :admin

  good_job_control_concurrency_with(
    perform_limit: 1,
    key: -> { "#{self.class.name}-#{queue_name}-#{arguments.second}-#{arguments.first}" },
  )

  ALLOWED_SCHEMAS = ['logs', 'idp'].freeze

  def perform(table_name = nil, schema_name = nil)
    if table_name.present? && schema_name.present?
      check_duplicates(table_name, schema_name)
    else
      SchemaTableService.generate_schema_table_hash.each do |schema, tables|
        next unless ALLOWED_SCHEMAS.include?(schema)

        tables.each do |table|
          next if table.start_with?('unextracted_')
          next if schema == 'logs' && !logs_duplicate_check_day?

          check_duplicates(table, schema)
        end
      end
    end
  end

  private

  def logs_duplicate_check_day?
    Time.zone.today.saturday?
  end

  def check_duplicates(table_name, schema_name)
    @table_name = DataWarehouseApplicationRecord.connection.quote_table_name(table_name)
    @schema_name = DataWarehouseApplicationRecord.connection.quote_table_name(schema_name)
    uniq_by = determine_unique_identifier(schema_name, table_name)

    log_message(
      :info,
      'Checking for duplicate rows',
      true,
      log_context,
    )

    query = build_query(uniq_by)

    duplicates = DataWarehouseApplicationRecord.connection.exec_query(query)
    log_result(duplicates)
  end

  def build_query(uniq_by)
    <<-SQL
      SELECT #{uniq_by}, COUNT(*)
      FROM #{@schema_name}.#{@table_name}
      GROUP BY #{uniq_by}
      HAVING COUNT(*) > 1
    SQL
  end

  def determine_unique_identifier(schema_name, table_name)
    columns = DataWarehouseApplicationRecord.connection.columns("#{schema_name}.#{table_name}")
    columns.any? { |c| c.name == 'id' } ? 'id' : 'uuid'
  end

  def log_result(duplicates)
    if duplicates.any?
      log_message(
        :warn,
        'Found duplicate rows',
        false,
        log_context(duplicate_count: duplicates.count),
      )
    else
      log_message(
        :info,
        'No duplicate rows found',
        true,
        log_context,
      )
    end
  end

  def log_context(extra = {})
    {
      job_id: job_id,
      schema_name: @schema_name,
      table_name: @table_name,
    }.merge(extra)
  end
end
