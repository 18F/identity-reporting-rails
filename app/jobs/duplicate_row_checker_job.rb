class DuplicateRowCheckerJob < ApplicationJob
  queue_as :default

  def perform(table_name, schema_name)
    @table_name = DataWarehouseApplicationRecord.connection.quote_table_name(table_name)
    @schema_name = DataWarehouseApplicationRecord.connection.quote_table_name(schema_name)
    uniq_by = determine_unique_identifier(schema_name, table_name)

    Rails.logger.info "DuplicateRowCheckerJob: Checking for duplicates in " \
    "#{@schema_name}.#{@table_name}"

    query = build_query(uniq_by)

    duplicates = DataWarehouseApplicationRecord.connection.exec_query(query)
    log_result(duplicates)
  end

  private

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
