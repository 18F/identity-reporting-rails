# frozen_string_literal: true

require 'rails_helper'

RSpec.describe RedshiftMasking::DbtSchemaQueries do
  let(:connection) { instance_double(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter) }
  let(:queries) { described_class.new(connection) }
  let(:sqls) { [] }

  before do
    allow(connection).to receive(:quote) { |value| "'#{value.gsub("'", "''")}'" }
    allow(connection).to receive(:execute) do |sql|
      sqls << sql
      []
    end
  end

  it 'scopes relations and columns to the current database and quoted schema/table values' do
    queries.relations("schema'quoted")
    queries.columns("schema'quoted", "table'quoted")

    expect(sqls.join).to include('current_database()', "'schema''quoted'", "'table''quoted'")
    expect(sqls.first).to include('svv_mv_info')
  end

  it 'decodes column identifiers without stripping punctuation' do
    row = {
      'policy_name' => 'dbt_mask_super',
      'schema_name' => 'fraudops_marts',
      'table_name' => 'orphan',
      'output_columns' => '["column.with.dot"]',
      'input_columns' => '["column.with.dot"]',
      'grantor' => 'superuser',
      'grantee' => 'PUBLIC',
      'grantee_type' => 'public',
      'priority' => 0,
    }
    allow(connection).to receive(:execute).and_return([row])

    expect(queries.attachments('fraudops_marts', 'orphan').first.column).to eq('column.with.dot')
    expect(sqls).to be_empty
  end

  it 'rejects multi-column policies instead of inspecting only the first input' do
    allow(connection).to receive(:execute).and_return(
      [{ 'output_columns' => '["a", "b"]', 'input_columns' => '["a", "b"]' }],
    )

    expect { queries.attachments('fraudops_marts', 'orphan') }.
      to raise_error(ArgumentError, /single-column/)
  end
end
