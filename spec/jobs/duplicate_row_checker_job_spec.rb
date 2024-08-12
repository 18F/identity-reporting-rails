require 'rails_helper'
require 'factory_bot'

RSpec.describe DuplicateRowCheckerJob, type: :job do
  let(:idp_job) { DuplicateRowCheckerJob.new }
  let(:logs_job) { DuplicateRowCheckerJob.new }

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
  end
end
