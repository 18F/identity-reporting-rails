# frozen_string_literal: true

require 'rails_helper'

RSpec.describe RedshiftMaskingJob, type: :job do
  let(:job) { described_class.new }
  let(:service) { instance_double(RedshiftMaskingService) }

  describe '#perform' do
    context 'when job is disabled' do
      before do
        allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(false)
      end

      it 'logs that job is disabled and returns without performing' do
        expect(Rails.logger).to receive(:info).with('RedshiftMasking job is disabled, skipping')
        expect(RedshiftMaskingService).not_to receive(:new)

        job.perform
      end
    end

    context 'when job is enabled' do
      before do
        allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(true)
        allow(RedshiftMaskingService).to receive(:new).and_return(service)
      end

      it 'delegates to RedshiftMaskingService' do
        expect(service).to receive(:sync).with(user_filter: nil)

        job.perform
      end

      context 'with user_filter' do
        it 'passes user_filter to service' do
          user_filter = ['IAM:alice', 'IAM:bob']
          expect(service).to receive(:sync).with(user_filter: user_filter)

          job.perform(user_filter: user_filter)
        end
      end
    end
  end
end
