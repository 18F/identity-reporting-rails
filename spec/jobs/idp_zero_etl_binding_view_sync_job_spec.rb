# frozen_string_literal: true

require 'rails_helper'

RSpec.describe IdpZeroEtlBindingViewSyncJob, type: :job do
  let(:idp_zero_etl_sync) { instance_double(ZetlBindingViewSync) }
  let(:logger) { instance_double(ActiveSupport::Logger) }
  let(:job_log_subscriber) { instance_double(IdentityJobLogSubscriber, logger: logger) }

  before do
    allow(ZetlBindingViewSync).to receive(:new).and_return(idp_zero_etl_sync)
    allow(IdentityJobLogSubscriber).to receive(:new).and_return(job_log_subscriber)
    allow(IdentityConfig.store).to receive(:idp_zero_etl_enabled).and_return(true)
  end

  describe '#perform' do
    context 'when idp_zero_etl_enabled is false' do
      before do
        allow(IdentityConfig.store).to receive(:idp_zero_etl_enabled).and_return(false)
        allow(idp_zero_etl_sync).to receive(:sync)
        allow(logger).to receive(:info)
      end

      it 'does not call ZetlBindingViewSync.sync' do
        subject.perform
        expect(idp_zero_etl_sync).not_to have_received(:sync)
      end

      it 'logs that it was skipped' do
        expect(logger).to receive(:info).with(
          {
            name: 'IdpZeroEtlBindingViewSyncJob',
            skipped: 'idp_zero_etl_enabled is false',
          }.to_json,
        )
        subject.perform
      end
    end

    context 'when sync succeeds' do
      let(:results) { { created: 2, skipped: 1, failed: 0, stale: 0 } }

      before do
        allow(idp_zero_etl_sync).to receive(:sync).and_return(results)
        allow(logger).to receive(:info)
      end

      it 'calls ZetlBindingViewSync.sync' do
        subject.perform
        expect(idp_zero_etl_sync).to have_received(:sync)
      end

      it 'logs success' do
        expect(logger).to receive(:info).with(
          {
            name: 'IdpZeroEtlBindingViewSyncJob',
            success: true,
            message: 'ZETL binding view sync completed successfully',
            results: results,
          }.to_json,
        )
        subject.perform
      end
    end

    context 'when sync fails' do
      let(:error_message) { 'Database connection failed' }
      let(:error) { StandardError.new(error_message) }

      before do
        allow(idp_zero_etl_sync).to receive(:sync).and_raise(error)
      end

      it 'logs error' do
        expect(logger).to receive(:error).with(
          {
            name: 'IdpZeroEtlBindingViewSyncJob',
            error: error_message,
          }.to_json,
        )
        expect { subject.perform }.to raise_error(StandardError, error_message)
      end

      it 're-raises the error' do
        allow(logger).to receive(:error)
        expect { subject.perform }.to raise_error(StandardError, error_message)
      end
    end
  end
end
