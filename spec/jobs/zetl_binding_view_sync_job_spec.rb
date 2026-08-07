# frozen_string_literal: true

require 'rails_helper'

RSpec.describe ZetlBindingViewSyncJob, type: :job do
  let(:zetl_binding_view_sync) { instance_double(ZetlBindingViewSync) }
  let(:logger) { instance_double(ActiveSupport::Logger) }
  let(:job_log_subscriber) { instance_double(IdentityJobLogSubscriber, logger: logger) }

  before do
    allow(ZetlBindingViewSync).to receive(:new).and_return(zetl_binding_view_sync)
    allow(IdentityJobLogSubscriber).to receive(:new).and_return(job_log_subscriber)
  end

  describe '#perform' do
    context 'when sync succeeds' do
      before do
        allow(zetl_binding_view_sync).to receive(:sync)
        allow(logger).to receive(:info)
      end

      it 'calls ZetlBindingViewSync.sync' do
        subject.perform
        expect(zetl_binding_view_sync).to have_received(:sync)
      end

      it 'logs success' do
        expect(logger).to receive(:info).with(
          {
            name: 'ZetlBindingViewSyncJob',
            success: true,
          }.to_json,
        )
        subject.perform
      end
    end

    context 'when sync fails' do
      let(:error_message) { 'Database connection failed' }
      let(:error) { StandardError.new(error_message) }

      before do
        allow(zetl_binding_view_sync).to receive(:sync).and_raise(error)
      end

      it 'logs error' do
        expect(logger).to receive(:error).with(
          {
            name: 'ZetlBindingViewSyncJob',
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
