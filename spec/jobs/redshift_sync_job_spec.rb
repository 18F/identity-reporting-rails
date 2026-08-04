# frozen_string_literal: true

require 'rails_helper'

RSpec.describe RedshiftSyncJob, type: :job do
  let(:dw_sync) { instance_double(RedshiftSync) }
  let(:zetl_sync) { instance_double(RedshiftSync) }
  let(:logger) { instance_double(ActiveSupport::Logger) }
  let(:job_log_subscriber) { instance_double(IdentityJobLogSubscriber, logger: logger) }

  before do
    allow(RedshiftSync).to receive(:new).
      with(database: 'analytics').and_return(dw_sync)
    allow(RedshiftSync).to receive(:new).
      with(database: 'analytics_zetl').and_return(zetl_sync)
    allow(IdentityJobLogSubscriber).to receive(:new).and_return(job_log_subscriber)
  end

  describe '#perform' do
    context 'when both syncs succeed' do
      before do
        allow(dw_sync).to receive(:sync)
        allow(zetl_sync).to receive(:sync)
        allow(logger).to receive(:info)
      end

      it 'calls sync on both databases' do
        subject.perform
        expect(dw_sync).to have_received(:sync)
        expect(zetl_sync).to have_received(:sync)
      end

      it 'logs success for each database' do
        expect(logger).to receive(:info).with(
          {
            name: 'RedshiftSyncJob',
            database: 'analytics',
            success: true,
          }.to_json,
        )
        expect(logger).to receive(:info).with(
          {
            name: 'RedshiftSyncJob',
            database: 'analytics_zetl',
            success: true,
          }.to_json,
        )
        subject.perform
      end
    end

    context 'when the first database fails' do
      let(:error_message) { 'Database connection failed' }
      let(:error) { StandardError.new(error_message) }

      before do
        allow(dw_sync).to receive(:sync).and_raise(error)
        allow(zetl_sync).to receive(:sync)
        allow(logger).to receive(:error)
      end

      it 'does not attempt the second database' do
        expect { subject.perform }.to raise_error(StandardError, error_message)
        expect(zetl_sync).not_to have_received(:sync)
      end

      it 'logs error' do
        expect(logger).to receive(:error).with(
          {
            name: 'RedshiftSyncJob',
            database: 'analytics',
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

    context 'when the second database fails' do
      let(:error_message) { 'Database connection failed' }
      let(:error) { StandardError.new(error_message) }

      before do
        allow(dw_sync).to receive(:sync)
        allow(zetl_sync).to receive(:sync).and_raise(error)
        allow(logger).to receive(:info)
        allow(logger).to receive(:error)
      end

      it 'still attempts the first database' do
        expect { subject.perform }.to raise_error(StandardError, error_message)
        expect(dw_sync).to have_received(:sync)
      end

      it 're-raises the error' do
        expect { subject.perform }.to raise_error(StandardError, error_message)
      end

      it 'logs the error against the database that failed' do
        expect(logger).to receive(:error).with(
          {
            name: 'RedshiftSyncJob',
            database: 'analytics_zetl',
            error: error_message,
          }.to_json,
        )
        expect { subject.perform }.to raise_error(StandardError, error_message)
      end
    end
  end
end
