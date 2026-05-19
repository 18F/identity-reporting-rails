require 'rails_helper'

RSpec.describe DataFreshnessJob, type: :job do
  let(:logger) { instance_double(IdentityJobLogSubscriber) }
  let(:log_entry) { instance_double(Logger) }

  before do
    allow(IdentityJobLogSubscriber).to receive(:new).and_return(logger)
    allow(logger).to receive(:logger).and_return(log_entry)
  end

  context 'when the latest log exists' do
    context 'and is within the range of 0-31 hours' do
      it 'logs within_range status' do
        FactoryBot.create(:production, cloudwatch_timestamp: 1.hour.ago)

        expect(log_entry).to receive(:info).with(
          {
            name: 'DataFreshnessJob',
            freshness_hours: 1.0,
            status: 'within_range',
          }.to_json,
        )

        described_class.perform_now
      end
    end

    context 'and is outside the range of 0-31 hours' do
      it 'logs out_of_range status' do
        FactoryBot.create(:production, cloudwatch_timestamp: 32.hours.ago)

        expect(log_entry).to receive(:info).with(
          {
            name: 'DataFreshnessJob',
            freshness_hours: 32.0,
            status: 'out_of_range',
          }.to_json,
        )

        described_class.perform_now
      end
    end
  end

  context 'when no logs are found' do
    it 'logs an error' do
      allow(Production).to receive(:maximum).with(:cloudwatch_timestamp).and_return(nil)

      expect(log_entry).to receive(:error).with(
        {
          name: 'DataFreshnessJob',
          error: 'No logs found!',
        }.to_json,
      )

      described_class.perform_now
    end
  end
end
