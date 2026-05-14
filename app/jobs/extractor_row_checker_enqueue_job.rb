require 'schema_table_service'
class ExtractorRowCheckerEnqueueJob < ApplicationJob
  queue_as :admin

  def perform
    schema_table_service = SchemaTableService.generate_schema_table_hash
    schema_table_service.each do |schema_name, tables|
      tables.each do |table_name|
        if schema_name == 'logs'
          PiiRowCheckerJob.perform_later(table_name)
        end
      end
    end
  end
end
