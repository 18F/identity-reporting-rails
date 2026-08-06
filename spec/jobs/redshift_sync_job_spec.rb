# frozen_string_literal: true

require 'rails_helper'

RSpec.describe RedshiftSyncJob, type: :job do
  let(:redshift_sync) { instance_double(RedshiftSync) }
  let(:logger) { instance_double(ActiveSupport::Logger) }
  let(:job_log_subscriber) { instance_double(IdentityJobLogSubscriber, logger: logger) }

  before do
    allow(RedshiftSync).to receive(:new).and_return(redshift_sync)
    allow(IdentityJobLogSubscriber).to receive(:new).and_return(job_log_subscriber)
  end

  describe '#perform' do
    context 'when the sync succeeds' do
      before do
        allow(redshift_sync).to receive(:sync)
        allow(logger).to receive(:info)
      end

      it 'delegates to RedshiftSync, which iterates the databases itself' do
        subject.perform

        expect(RedshiftSync).to have_received(:new).with(no_args)
        expect(redshift_sync).to have_received(:sync).once
      end

      it 'logs success once for the whole run' do
        expect(logger).to receive(:info).with(
          {
            name: 'RedshiftSyncJob',
            success: true,
          }.to_json,
        ).once

        subject.perform
      end
    end

    context 'when the sync raises' do
      let(:error_message) { 'Database connection failed' }

      before do
        allow(redshift_sync).to receive(:sync).and_raise(StandardError, error_message)
        allow(logger).to receive(:info)
        allow(logger).to receive(:error)
      end

      it 're-raises the error' do
        expect { subject.perform }.to raise_error(StandardError, error_message)
      end

      it 'logs the error' do
        expect(logger).to receive(:error).with(
          {
            name: 'RedshiftSyncJob',
            error: error_message,
          }.to_json,
        )

        expect { subject.perform }.to raise_error(StandardError, error_message)
      end

      it 'does not log success' do
        expect(logger).not_to receive(:info)

        expect { subject.perform }.to raise_error(StandardError, error_message)
      end
    end
  end
end
