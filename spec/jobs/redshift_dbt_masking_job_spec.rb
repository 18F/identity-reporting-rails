# frozen_string_literal: true

require 'rails_helper'

RSpec.describe RedshiftDbtMaskingJob, type: :job do
  let(:sync) { instance_double(RedshiftMaskingSync) }

  before do
    allow(RedshiftMaskingSync).to receive(:new).and_return(sync)
    allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(true)
    allow(IdentityConfig.store).to receive(:dw_fraudops_email_enabled).and_return(false)
  end

  it 'invokes each configured schema explicitly in one job' do
    expect(sync).to receive(:sync_dbt_schema).with(schema: 'fraudops_marts').ordered
    expect(sync).to receive(:sync_dbt_schema).with(schema: 'fraudops_qa_marts').ordered
    expect(sync).not_to receive(:sync_configured_columns)

    described_class.new.perform
  end

  it 'skips when both fraudops flags are disabled' do
    allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(false)
    expect(sync).not_to receive(:sync_dbt_schema)

    described_class.new.perform
  end

  it 'propagates a failed scope instead of reporting successful completion' do
    allow(sync).to receive(:sync_dbt_schema).and_raise(ActiveRecord::StatementInvalid, 'failed')

    expect { described_class.new.perform }.to raise_error(ActiveRecord::StatementInvalid)
  end
end
