# frozen_string_literal: true

require 'rails_helper'

RSpec.describe RedshiftMaskingJob, type: :job do
  let(:job) { described_class.new }
  let(:sync) { instance_double(RedshiftMaskingSync) }
  let(:zetl_sync) { instance_double(RedshiftMaskingZetlSync) }

  describe '#perform' do
    context 'when job is disabled' do
      before do
        allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(false)
        allow(IdentityConfig.store).to receive(:dw_fraudops_email_enabled).and_return(false)
      end

      it 'logs that job is disabled and returns without performing' do
        expect(Rails.logger).to receive(:info).with('RedshiftMasking job is disabled, skipping')
        expect(RedshiftMaskingSync).not_to receive(:new)
        expect(RedshiftMaskingZetlSync).not_to receive(:new)

        job.perform
      end
    end

    context 'when job is enabled' do
      before do
        allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(true)
        allow(RedshiftMaskingSync).to receive(:new).and_return(sync)
        allow(RedshiftMaskingZetlSync).to receive(:new).and_return(zetl_sync)
      end

      context 'when zero_etl_enabled is false' do
        before do
          allow(IdentityConfig.store).to receive(:zero_etl_enabled).and_return(false)
        end

        it 'syncs only with RedshiftMaskingSync' do
          expect(sync).to receive(:sync).with(user_filter: nil)
          expect(RedshiftMaskingZetlSync).not_to receive(:new)

          job.perform
        end

        it 'passes user_filter to sync' do
          user_filter = ['IAM:alice', 'IAM:bob']
          expect(sync).to receive(:sync).with(user_filter: user_filter)

          job.perform(user_filter: user_filter)
        end
      end

      context 'when zero_etl_enabled is true' do
        before do
          allow(IdentityConfig.store).to receive(:zero_etl_enabled).and_return(true)
        end

        it 'syncs with both RedshiftMaskingSync and RedshiftMaskingZetlSync' do
          expect(sync).to receive(:sync).with(user_filter: nil)
          expect(zetl_sync).to receive(:sync).with(user_filter: nil)

          job.perform
        end

        it 'passes user_filter to both syncs' do
          user_filter = ['IAM:alice', 'IAM:bob']
          expect(sync).to receive(:sync).with(user_filter: user_filter)
          expect(zetl_sync).to receive(:sync).with(user_filter: user_filter)

          job.perform(user_filter: user_filter)
        end
      end
    end
  end
end
