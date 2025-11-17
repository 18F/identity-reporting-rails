require 'rails_helper'

RSpec.describe ExtractorRowCheckerEnqueueJob, type: :job do
  describe '#perform' do
    let(:schema_table_hash) do
      {
        'logs' => ['events', 'production', 'unextracted_events'],
        'idp' => ['articles'],
        'system_tables' => ['stl_sample'],
        'fraud_ops' => ['email_addresses'],
      }
    end

    before do
      allow(SchemaTableService).to receive(:generate_schema_table_hash).
        and_return(schema_table_hash)
    end

    it 'enqueues LogsColumnExtractorJob for tables in logs schema' do
      schema_table_hash['logs'].each do |table_name|
        expect(PiiRowCheckerJob).to receive(:perform_later).with(table_name)
      end

      expect { ExtractorRowCheckerEnqueueJob.new.perform }.not_to raise_error
    end

    it 'enqueues DuplicateRowCheckerJob for tables in allowed schemas only' do
      allowed_schemas = ['logs', 'idp']
      schema_table_hash.each do |schema_name, tables|
        next unless allowed_schemas.include?(schema_name)
        tables.each do |table_name|
          next if table_name.start_with?('unextracted_')
          expect(DuplicateRowCheckerJob).to receive(:perform_later).with(table_name, schema_name)
        end
      end

      expect { ExtractorRowCheckerEnqueueJob.new.perform }.not_to raise_error
    end
  end
end
