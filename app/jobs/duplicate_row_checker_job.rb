require 'schema_table_service'

class DuplicateRowCheckerJob < ApplicationJob
  include GoodJob::ActiveJobExtensions::Concurrency
  queue_as :default

  good_job_control_concurrency_with(
    perform_limit: 1,
    key: -> { "#{self.class.name}-#{queue_name}-#{arguments.second}-#{arguments.first}" },
  )

  ALLOWED_SCHEMAS = ['logs', 'idp'].freeze

  def perform(table_name = nil, schema_name = nil)
    if table_name.present? && schema_name.present?
      check_duplicates(table_name, schema_name)
    else
      SchemaTableService.generate_schema_table_hash.each do |sch, tables|
        next unless ALLOWED_SCHEMAS.include?(sch)

        tables.each do |tbl|
          next if tbl.start_with?('unextracted_')
          next if sch == 'logs' && !logs_duplicate_check_day?

          check_duplicates(tbl, sch)
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

    Rails.logger.info "DuplicateRowCheckerJob: Checking for duplicates in " \
    "#{@schema_name}.#{@table_name}"

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
      Rails.logger.warn "DuplicateRowCheckerJob: Found #{duplicates.count} duplicate(s) in " \
                        "#{@schema_name}.#{@table_name}"
    else
      Rails.logger.info "DuplicateRowCheckerJob: No duplicates found in " \
                        "#{@schema_name}.#{@table_name}"
    end
  end
end
