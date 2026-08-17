require 'rails_helper'
require 'idp_zero_etl_binding_view_sync'

RSpec.describe IdpZeroEtlBindingViewSync do
  let(:exclusion_config_path) do
    Rails.root.join('spec', 'fixtures', 'idp_zero_etl_column_config.yaml')
  end
  let(:connection) { instance_double(ActiveRecord::ConnectionAdapters::AbstractAdapter) }
  let(:source_rows) do
    [
      { 'table_name' => 'users', 'column_name' => 'id' },
      { 'table_name' => 'users', 'column_name' => 'uuid' },
      { 'table_name' => 'users', 'column_name' => 'encrypted_password_digest' },
      { 'table_name' => 'identities', 'column_name' => 'id' },
      { 'table_name' => 'identities', 'column_name' => 'user_id' },
      { 'table_name' => 'secrets', 'column_name' => 'token' },
    ]
  end

  subject(:sync) { described_class.new(exclusion_config_path:) }

  let(:existing_view_rows) { [] }

  before do
    allow(DataWarehouseApplicationRecord).to receive(:connection).and_return(connection)
    allow(DataWarehouseApplicationRecord).to receive(:sanitize_sql) { |arg| arg.first }
    allow(connection).to receive(:execute)
    # The service issues two queries: source columns (svv_all_columns) and
    # existing target views (information_schema.views). Route by SQL text.
    allow(connection).to receive(:exec_query) do |sql|
      sql.include?('information_schema.views') ? existing_view_rows : source_rows
    end
  end

  describe '#sync' do
    it 'creates the target schema' do
      sync.sync

      expect(connection).to have_received(:execute).with(
        %(CREATE SCHEMA IF NOT EXISTS "idp_core";),
      )
    end

    it 'builds a late-binding view per source table' do
      sync.sync

      expect(connection).to have_received(:execute).with(
        a_string_matching(/CREATE OR REPLACE VIEW "idp_core"\."identities"/).
          and(a_string_matching(/SELECT "id", "user_id"/)).
          and(a_string_matching(/FROM "analytics_zetl"\."public"\."identities"/)).
          and(a_string_matching(/WITH NO SCHEMA BINDING/)),
      )
    end

    it 'omits columns listed in the exclusion config' do
      sync.sync

      expect(connection).to have_received(:execute).with(
        a_string_matching(/VIEW "idp_core"\."users"/).
          and(a_string_matching(/SELECT "id", "uuid"\n/)),
      )
      expect(connection).not_to have_received(:execute).with(
        a_string_matching(/encrypted_password_digest/),
      )
    end

    it 'skips a table whose columns are entirely excluded' do
      result = sync.sync

      expect(connection).not_to have_received(:execute).with(
        a_string_matching(/VIEW "idp_core"\."secrets"/),
      )
      expect(result).to eq(created: 2, skipped: 1, stale: 0, failed: 0)
    end

    context 'when the target schema has a view with no matching source table' do
      let(:existing_view_rows) do
        [
          { 'table_name' => 'users' },
          { 'table_name' => 'identities' },
          { 'table_name' => 'removed_table' },
        ]
      end

      it 'logs a warning and reports it as stale without dropping it' do
        logger = instance_double(ActiveSupport::Logger)
        allow_any_instance_of(IdentityJobLogSubscriber).to receive(:logger).and_return(logger)
        allow(logger).to receive(:info)
        allow(logger).to receive(:warn)

        result = sync.sync

        expect(logger).to have_received(:warn).with(
          a_string_matching(/Stale view idp_core\.removed_table/),
        )
        expect(connection).not_to have_received(:execute).with(
          a_string_matching(/DROP VIEW/i),
        )
        expect(result[:stale]).to eq(1)
      end
    end

    it 'raises on an invalid identifier rather than emitting view DDL' do
      allow(connection).to receive(:exec_query).and_return(
        [{ 'table_name' => 'users; drop table foo', 'column_name' => 'id' }],
      )

      expect { sync.sync }.to raise_error(described_class::InvalidIdentifierError)
    end
  end
end
