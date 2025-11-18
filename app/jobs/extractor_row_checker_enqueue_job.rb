require 'schema_table_service'
class ExtractorRowCheckerEnqueueJob < ApplicationJob
  queue_as :default

  def perform
    schema_table_service = SchemaTableService.generate_schema_table_hash
    schema_table_service.each do |schema_name, tables|
      tables.each do |table_name|
        if schema_name == 'logs'
          PiiRowCheckerJob.perform_later(table_name)
        end
        # Only process specific schemas for duplicate checking
        allowed_schemas = ['logs', 'idp']
        next if table_name.start_with?('unextracted_') || !allowed_schemas.include?(schema_name)

        DuplicateRowCheckerJob.perform_later(table_name, schema_name)
      end
    end
  end
end
