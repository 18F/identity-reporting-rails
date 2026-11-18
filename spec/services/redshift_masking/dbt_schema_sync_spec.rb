# frozen_string_literal: true

require 'rails_helper'

RSpec.describe RedshiftMasking::DbtSchemaSync do
  let(:connection) { instance_double(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter) }
  let(:queries) { instance_double(RedshiftMasking::DbtSchemaQueries) }
  let(:sync) { described_class.new(connection: connection) }
  let(:schema) { 'fraudops_qa_marts' }
  let(:table) { 'orphan.table' }
  let(:column) do
    {
      'column_name' => 'email.address',
      'data_type' => 'character varying',
      'character_maximum_length' => 256,
    }
  end
  let(:policies) { Set.new(%w[dbt_mask_varchar_max dbt_unmask_varchar_max]) }
  let(:expected) do
    RedshiftMasking::DbtConfiguration.new.expected_attachments(schema, table, [column], policies)
  end
  let(:actual) { [] }
  let(:executed) { [] }

  before do
    allow(RedshiftMasking::DbtSchemaQueries).to receive(:new).with(connection).and_return(queries)
    allow(queries).to receive(:role_exists?).with('pii_viewers').and_return(true)
    allow(queries).to receive(:relations).with(schema, table: nil).and_return(
      [{ 'table_name' => table, 'table_type' => 'BASE TABLE', 'materialized_view' => false }],
    )
    allow(queries).to receive(:relations).with(schema, table: table).and_return(
      [{ 'table_name' => table, 'table_type' => 'BASE TABLE', 'materialized_view' => false }],
    )
    allow(queries).to receive(:columns).with(schema, table).and_return([column])
    allow(queries).to receive(:policy_names).and_return(policies)
    allow(queries).to receive(:attachments).with(schema, table).and_return(actual)
    allow(connection).to receive(:transaction).and_yield
    allow(connection).to receive(:quote_column_name) { |value| %("#{value.gsub('"', '""')}") }
    allow(connection).to receive(:quote, &:to_s)
    allow(connection).to receive(:execute) { |sql| executed << sql }
  end

  it 'discovers and repairs an orphan without model metadata or policy creation' do
    sync.sync(schema: schema)

    expect(executed.join).to include('LOCK TABLE "fraudops_qa_marts"."orphan.table"')
    expect(executed.join).to include('TO PUBLIC', 'TO ROLE "pii_viewers"')
    expect(executed.join).to include('("email.address")')
    expect(executed.join).not_to match(/CREATE|ALTER|DROP/)
    expect(queries).not_to have_received(:relations).with('fraudops_marts', table: nil)
    expect(queries).to have_received(:attachments).with(schema, table).twice
  end

  it 'rechecks after locking and does not execute stale corrections' do
    allow(queries).to receive(:attachments).with(schema, table).and_return([], expected)

    sync.sync(schema: schema)

    expect(executed.grep(/ATTACH|DETACH/)).to be_empty
    expect(executed.grep(/LOCK/).size).to eq(1)
  end

  it 'is idempotent after an admin-created repair and never locks healthy tables' do
    repaired = expected.map do |policy|
      RedshiftMasking::PolicyAttachment.new(**policy.to_h.merge(grantor: 'superuser'))
    end
    allow(queries).to receive(:attachments).with(schema, table).and_return([], [], repaired)

    sync.sync(schema: schema)
    first_run = executed.size
    sync.sync(schema: schema)

    expect(executed.size).to eq(first_run)
  end

  it 'leaves transitional direct-user attachments untouched' do
    user = RedshiftMasking::PolicyAttachment.new(
      **expected.last.to_h.merge(grantee: 'fraudops_marts', grantee_type: 'user'),
    )
    allow(queries).to receive(:attachments).and_return(expected + [user])

    sync.sync(schema: schema)

    expect(executed).to be_empty
  end

  it 'rejects unknown scopes before database access' do
    expect { sync.sync(schema: 'public') }.to raise_error(ArgumentError, /one configured/)
    expect(queries).not_to have_received(:relations)
  end

  it 'fails preflight without correcting anything if a required policy is absent' do
    allow(queries).to receive(:policy_names).and_return(Set.new)

    expect { sync.sync(schema: schema) }.to raise_error(ArgumentError, /Missing dbt policies/)
    expect(executed).to be_empty
  end

  it 'fails if the unmask role is missing' do
    allow(queries).to receive(:role_exists?).and_return(false)

    expect { sync.sync(schema: schema) }.to raise_error(ArgumentError, /Missing pii_viewers/)
    expect(executed).to be_empty
  end

  it 'rejects materialized views before applying repairs' do
    allow(queries).to receive(:relations).and_return(
      [{ 'table_name' => 'mv', 'table_type' => 'BASE TABLE', 'materialized_view' => true }],
    )

    expect { sync.sync(schema: schema) }.to raise_error(ArgumentError, /materialized view/)
    expect(executed).to be_empty
  end

  it 'excludes views with a warning, without claiming they are protected' do
    allow(queries).to receive(:relations).and_return(
      [{ 'table_name' => 'v', 'table_type' => 'VIEW', 'materialized_view' => false }],
    )
    expect(Rails.logger).to receive(:warn).with(/source masking is not verified/)

    sync.sync(schema: schema)

    expect(executed).to be_empty
  end

  it 'rejects external relations' do
    allow(queries).to receive(:relations).and_return(
      [{ 'table_name' => 'ext', 'table_type' => 'EXTERNAL TABLE', 'materialized_view' => false }],
    )

    expect { sync.sync(schema: schema) }.to raise_error(ArgumentError, /Unsupported relation/)
  end

  it 'does not collapse multiple managed attachments at different priorities' do
    duplicate = RedshiftMasking::PolicyAttachment.new(**expected.first.to_h.merge(priority: 20))
    allow(queries).to receive(:attachments).and_return(expected + [duplicate])

    expect { sync.sync(schema: schema) }.to raise_error(ArgumentError, /Ambiguous/)
    expect(executed).to be_empty
  end

  it 'propagates lock failure without logging completion' do
    allow(connection).to receive(:execute).with(/LOCK/).
      and_raise(ActiveRecord::StatementInvalid, 'lock timeout')
    allow(Rails.logger).to receive(:info)

    expect { sync.sync(schema: schema) }.to raise_error(ActiveRecord::StatementInvalid)
    expect(Rails.logger).not_to have_received(:info).with(/sync completed/)
  end

  it 'propagates attachment failure out of the repair transaction' do
    allow(connection).to receive(:execute).with(/ATTACH/).
      and_raise(ActiveRecord::StatementInvalid, 'attach failed')
    allow(Rails.logger).to receive(:info)

    expect { sync.sync(schema: schema) }.to raise_error(ActiveRecord::StatementInvalid)
    expect(Rails.logger).not_to have_received(:info).with(/sync completed/)
  end
end
