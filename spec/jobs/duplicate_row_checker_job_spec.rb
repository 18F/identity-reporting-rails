require 'rails_helper'
require 'factory_bot'

RSpec.describe DuplicateRowCheckerJob, type: :job do
  let(:idp_job) { DuplicateRowCheckerJob.new }
  let(:logs_job) { DuplicateRowCheckerJob.new }

  describe 'concurrency key' do
    it 'locks per schema and table' do
      job = DuplicateRowCheckerJob.new('events', 'logs')

      expect(job.good_job_concurrency_key).to eq('DuplicateRowCheckerJob-default-logs-events')
    end
  end

  describe '#perform' do
    context 'when there are duplicate articles' do
      before do
        2.times do
          FactoryBot.create(:article, id: 1, title: 'Duplicate Title', content: 'Duplicate Content')
        end
      end

      it 'logs a warning' do
        expected_message = 'DuplicateRowCheckerJob: Found 1 duplicate(s) in "idp"."articles"'
        expect(Rails.logger).to receive(:warn).with(expected_message)
        idp_job.perform('articles', 'idp')
      end
    end

    context 'when there are duplicate events' do
      before do
        2.times do
          FactoryBot.create(:event, id: 1, name: 'Duplicate Title')
        end
      end

      it 'logs a warning' do
        expected_message = 'DuplicateRowCheckerJob: Found 1 duplicate(s) in "logs"."events"'
        expect(Rails.logger).to receive(:warn).with(expected_message)
        logs_job.perform('events', 'logs')
      end
    end

    context 'when there are no duplicate articles' do
      before do
        FactoryBot.create(:article, id: 1, title: 'Unique Title', content: 'Unique Content')
      end

      it 'does not log a warning' do
        expect(Rails.logger).not_to receive(:warn)
        idp_job.perform('articles', 'idp')
      end
    end

    context 'when there are no duplicate events' do
      before do
        FactoryBot.create(:event, id: '1', name: 'Sign in page2 visited')
      end

      it 'does not log a warning' do
        expect(Rails.logger).not_to receive(:warn)
        logs_job.perform('events', 'logs')
      end
    end

    context 'when performed without table arguments' do
      before do
        allow(SchemaTableService).to receive(:generate_schema_table_hash).and_return(
          'logs' => ['events'],
          'idp' => ['articles'],
        )
        allow(DataWarehouseApplicationRecord.connection).to receive(:columns).and_return(
          [instance_double(ActiveRecord::ConnectionAdapters::Column, name: 'id')],
        )
        allow(DataWarehouseApplicationRecord.connection).to receive(:exec_query).and_return(
          ActiveRecord::Result.new([], []),
        )
      end

      it 'runs duplicate checks for each allowed table' do
        expect(DataWarehouseApplicationRecord.connection).to receive(:exec_query).twice
        described_class.new.perform
      end
    end
  end
end
