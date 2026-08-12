require 'rails_helper'

RSpec.describe FraudOpsEmailAddressesZetlJob, type: :job do
  let(:job) { described_class.new }
  let(:sync) { instance_double(FraudOps::EmailAddressesZetlSync) }
  let(:sync_result) do
    { skipped: false, cutoff: '2026-08-10T00:00:00Z', lookback_minutes: 15 }
  end

  before do
    allow(FraudOps::EmailAddressesZetlSync).to receive(:new).and_return(sync)
    allow(sync).to receive(:sync).and_return(sync_result)
    allow(IdentityConfig.store).to receive(:zero_etl_enabled).and_return(true)
    allow(IdentityConfig.store).
      to receive(:zero_etl_email_addresses_lookback_minutes).and_return(15)
    allow(Rails.logger).to receive(:info)
    allow(Rails.logger).to receive(:error)
  end

  describe '#perform' do
    context 'when zero_etl_enabled is false' do
      before { allow(IdentityConfig.store).to receive(:zero_etl_enabled).and_return(false) }

      it 'logs that it skipped and does not run the sync' do
        expect(Rails.logger).to receive(:info).with(
          a_string_matching(/zero_etl_enabled is false/),
        )
        expect(sync).not_to receive(:sync)

        job.perform
      end
    end

    context 'when zero_etl_enabled is true' do
      it 'passes the configured lookback to the sync' do
        expect(sync).to receive(:sync).with(lookback_minutes: 15)

        job.perform
      end

      # The default argument is evaluated per invocation, so a config change takes
      # effect on the next run rather than needing a worker restart.
      it 'resolves the lookback from config on every run' do
        allow(IdentityConfig.store).
          to receive(:zero_etl_email_addresses_lookback_minutes).and_return(45)

        expect(sync).to receive(:sync).with(lookback_minutes: 45)

        job.perform
      end

      it 'lets an explicit lookback_minutes override the config default' do
        expect(sync).to receive(:sync).with(lookback_minutes: 60)

        job.perform(lookback_minutes: 60)
      end

      it 'logs start and completion' do
        job.perform

        expect(Rails.logger).to have_received(:info).with(a_string_matching(/Job started/))
        expect(Rails.logger).to have_received(:info).with(a_string_matching(/Job completed/))
      end

      it 'includes the sync summary in the completion log' do
        job.perform

        expect(Rails.logger).to have_received(:info).with(
          a_string_matching(/"cutoff":"2026-08-10T00:00:00Z"/),
        )
      end

      # Regression guard: the start log interpolates the resolved lookback, so a
      # signature that drops the keyword argument raises NameError here.
      it 'records the resolved lookback in the start log' do
        job.perform

        expect(Rails.logger).to have_received(:info).with(
          a_string_matching(/Job started/).and(a_string_matching(/"lookback_minutes":15/)),
        )
      end
    end

    context 'when the sync raises' do
      before do
        allow(sync).to receive(:sync).
          and_raise(ActiveRecord::StatementInvalid, 'relation does not exist')
      end

      it 'logs the error and re-raises so GoodJob marks the job failed' do
        expect(Rails.logger).to receive(:error).with(a_string_matching(/Job failed/))

        expect { job.perform }.to raise_error(ActiveRecord::StatementInvalid)
      end
    end

    describe 'PII safety' do
      it 'never logs SQL or row values' do
        logged = []
        allow(Rails.logger).to receive(:info) { |msg| logged << msg }

        job.perform

        expect(logged).not_to be_empty
        logged.each do |message|
          expect(message).not_to include('decrypt_udf')
          expect(message).not_to include('encrypted_email')
          expect(message).not_to include('@')
        end
      end
    end
  end
end
