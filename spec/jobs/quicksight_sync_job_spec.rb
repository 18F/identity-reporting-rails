# frozen_string_literal: true

require 'rails_helper'

RSpec.describe QuicksightSyncJob, type: :job do
  let(:quicksight_sync) { instance_double(QuicksightSync) }
  let(:logger) { instance_double(ActiveSupport::Logger) }
  let(:job_log_subscriber) { instance_double(IdentityJobLogSubscriber, logger: logger) }

  before do
    allow(QuicksightSync).to receive(:new).and_return(quicksight_sync)
    allow(IdentityJobLogSubscriber).to receive(:new).and_return(job_log_subscriber)
    allow(IdentityConfig.store).to receive(:quicksight_sync_enabled).and_return(true)
    allow(Identity::Hostdata).to receive(:env).and_return('prod')
  end

  describe '#perform' do
    context 'when quicksight_sync_enabled is false' do
      before do
        allow(IdentityConfig.store).to receive(:quicksight_sync_enabled).and_return(false)
        allow(logger).to receive(:info)
      end

      it 'does not invoke QuicksightSync' do
        allow(quicksight_sync).to receive(:sync)
        subject.perform
        expect(quicksight_sync).not_to have_received(:sync)
      end

      it 'logs that it was skipped' do
        expect(logger).to receive(:info).with(
          {
            name: 'QuicksightSyncJob',
            skipped: 'quicksight_sync_enabled is false',
          }.to_json,
        )
        subject.perform
      end
    end

    context 'when the environment is not allowed' do
      before do
        allow(Identity::Hostdata).to receive(:env).and_return('int')
        allow(logger).to receive(:info)
      end

      it 'does not invoke QuicksightSync' do
        allow(quicksight_sync).to receive(:sync)
        subject.perform
        expect(quicksight_sync).not_to have_received(:sync)
      end

      it 'logs that it was skipped' do
        expect(logger).to receive(:info).with(
          {
            name: 'QuicksightSyncJob',
            skipped: 'environment int is not allowed',
          }.to_json,
        )
        subject.perform
      end
    end

    context 'when sync succeeds' do
      before do
        allow(quicksight_sync).to receive(:sync)
        allow(logger).to receive(:info)
      end

      it 'calls QuicksightSync.sync' do
        subject.perform
        expect(quicksight_sync).to have_received(:sync)
      end

      it 'logs success' do
        expect(logger).to receive(:info).with(
          {
            name: 'QuicksightSyncJob',
            success: true,
          }.to_json,
        )
        subject.perform
      end
    end

    context 'when sync fails' do
      let(:error_message) { 'QuickSight API failed' }
      let(:error) { StandardError.new(error_message) }

      before do
        allow(quicksight_sync).to receive(:sync).and_raise(error)
      end

      it 'logs error' do
        expect(logger).to receive(:error).with(
          {
            name: 'QuicksightSyncJob',
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
