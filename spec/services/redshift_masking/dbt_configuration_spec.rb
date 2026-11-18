# frozen_string_literal: true

require 'rails_helper'

RSpec.describe RedshiftMasking::DbtConfiguration do
  let(:builder) { described_class.new }
  let(:types) do
    {
      'character varying' => 'varchar_max',
      'integer' => 'int',
      'bigint' => 'bigint',
      'numeric' => 'decimal',
      'real' => 'decimal',
      'double precision' => 'decimal',
      'boolean' => 'boolean',
      'date' => 'date',
      'timestamp without time zone' => 'timestamp',
      'super' => 'super',
    }
  end
  let(:policies) do
    types.values.flat_map { |type| ["dbt_mask_#{type}", "dbt_unmask_#{type}"] }.to_set
  end

  it 'covers every discovered supported column with PUBLIC and role attachments' do
    columns = types.keys.map do |type|
      {
        'column_name' => type,
        'data_type' => type,
        'character_maximum_length' => 256,
        'numeric_precision' => 18,
        'numeric_scale' => 0,
      }
    end
    expected = builder.expected_attachments('fraudops_marts', 'orphan', columns, policies)

    expect(expected.size).to eq(columns.size * 2)
    expected.each_slice(2) do |mask, unmask|
      expect(mask.grantee_type).to eq('public')
      expect(mask.priority).to eq(0)
      expect(unmask.grantee_type).to eq('role')
      expect(unmask.grantee).to eq('pii_viewers')
      expect(unmask.priority).to eq(100)
      expect(mask.policy_name).to eq("dbt_mask_#{types.fetch(mask.column)}")
    end
  end

  it 'rejects incompatible types and sizes instead of defaulting to varchar' do
    columns = [
      { 'data_type' => 'varbyte' }, { 'data_type' => 'smallint' },
      { 'data_type' => 'character', 'character_maximum_length' => 256 },
      { 'data_type' => 'character varying', 'character_maximum_length' => 10 },
      { 'data_type' => 'numeric', 'numeric_precision' => 10, 'numeric_scale' => 2 },
      { 'data_type' => 'timestamp with time zone' }
    ]
    columns.each do |column|
      expect do
        builder.expected_attachments(
          'fraudops_marts', 'orphan', [column.merge('column_name' => 'c')], policies
        )
      end.to raise_error(ArgumentError, /Incompatible/)
    end
  end

  it 'uses the configured unmask priority' do
    allow(IdentityConfig.store).to receive(:dbt_unmask_priority).and_return(250)
    column = { 'column_name' => 'c', 'data_type' => 'date' }

    expected = builder.expected_attachments('fraudops_marts', 'orphan', [column], policies)

    expect(expected.last.priority).to eq(250)
  end

  it 'rejects an unmask priority that does not exceed the mask priority' do
    allow(IdentityConfig.store).to receive(:dbt_unmask_priority).and_return(0)
    column = { 'column_name' => 'c', 'data_type' => 'date' }

    expect { builder.expected_attachments('fraudops_marts', 'orphan', [column], policies) }.
      to raise_error(ArgumentError, /dbt_unmask_priority/)
  end

  it 'rejects missing column metadata' do
    expect { builder.expected_attachments('fraudops_marts', 'orphan', [], policies) }.
      to raise_error(ArgumentError, /No column metadata/)
  end
end
