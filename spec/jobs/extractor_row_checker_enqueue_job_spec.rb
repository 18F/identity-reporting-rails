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

    it 'enqueues PiiRowCheckerJob for tables in logs schema' do
      schema_table_hash['logs'].each do |table_name|
        expect(PiiRowCheckerJob).to receive(:perform_later).with(table_name)
      end

      expect { ExtractorRowCheckerEnqueueJob.new.perform }.not_to raise_error
    end

    it 'enqueues checks on every invocation while previous checks are pending' do
      adapter = ActiveJob::QueueAdapters::TestAdapter.new
      allow(PiiRowCheckerJob).to receive(:queue_adapter).and_return(adapter)

      2.times { described_class.perform_now }

      expect(adapter.enqueued_jobs.map { |job| job[:args] }).to match_array(
        schema_table_hash['logs'].map { |table_name| [table_name] } * 2,
      )
    end
  end
end
