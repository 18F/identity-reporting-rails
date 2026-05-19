require 'rails_helper'
require 'reporting/issuer_string_to_sp_id_helper'

RSpec.describe Reporting::IssuerStringToSpIdHelper do
  let(:test_class) { Class.new { include Reporting::IssuerStringToSpIdHelper } }
  let(:helper) { test_class.new }

  let(:mock_sp_data) do
    [
      { 'issuer' => 'urn:gov:gsa:example1', 'id' => '1' },
      { 'issuer' => 'urn:gov:gsa:example2', 'id' => '2' },
      { 'issuer' => 'http://localhost:3000', 'id' => '3' },
      { 'issuer' => 'urn:gov:gsa:example4', 'id' => '4' },
    ]
  end

  before do
    # Mock the database connection
    connection = double('connection')
    allow(DataWarehouseApplicationRecord).to receive(:connection).and_return(connection)
    allow(connection).to receive(:execute).and_return(mock_sp_data)
  end

  describe '#get_issuer_sp_mapping' do
    context 'with direction: :issuer_to_id (default)' do
      it 'returns a hash mapping issuers to IDs' do
        result = helper.get_issuer_sp_mapping

        expect(result).to eq(
          {
            'urn:gov:gsa:example1' => { id: 1 },
            'urn:gov:gsa:example2' => { id: 2 },
            'http://localhost:3000' => { id: 3 },
            'urn:gov:gsa:example4' => { id: 4 },
          },
        )
      end
    end

    context 'with direction: :id_to_issuer' do
      it 'returns a hash mapping IDs to issuers' do
        result = helper.get_issuer_sp_mapping(direction: :id_to_issuer)

        expect(result).to eq(
          {
            1 => { issuer: 'urn:gov:gsa:example1' },
            2 => { issuer: 'urn:gov:gsa:example2' },
            3 => { issuer: 'http://localhost:3000' },
            4 => { issuer: 'urn:gov:gsa:example4' },
          },
        )
      end
    end
    context 'with invalid direction' do
      it 'raises ArgumentError' do
        expect do
          helper.get_issuer_sp_mapping(direction: :invalid)
        end.to raise_error(
          ArgumentError,
          'Invalid direction: invalid. Use :issuer_to_id or :id_to_issuer',
        )
      end
    end

    context 'with empty data' do
      before do
        connection = double('connection')
        allow(DataWarehouseApplicationRecord).to receive(:connection).and_return(connection)
        allow(connection).to receive(:execute).and_return([])
      end

      it 'returns empty hash and logs warning' do
        expect(Rails.logger).to receive(:warn).with(
          'No service providers found in idp.service_providers',
        )

        result = helper.get_issuer_sp_mapping
        expect(result).to eq({})
      end
    end

    context 'with duplicate issuers' do
      let(:duplicate_data) do
        [
          { 'issuer' => 'urn:gov:gsa:duplicate', 'id' => '1' },
          { 'issuer' => 'urn:gov:gsa:duplicate', 'id' => '2' },
          { 'issuer' => 'urn:gov:gsa:unique', 'id' => '3' },
        ]
      end

      before do
        connection = double('connection')
        allow(DataWarehouseApplicationRecord).to receive(:connection).and_return(connection)
        allow(connection).to receive(:execute).and_return(duplicate_data)
      end

      it 'keeps first ID and logs error' do
        expect(Rails.logger).to receive(:error).with(
          'Duplicate issuer found in idp.service_providers: '\
          'urn:gov:gsa:duplicate. Keeping first id.',
        )

        result = helper.get_issuer_sp_mapping
        expect(result['urn:gov:gsa:duplicate']).to eq({ id: 1 })
        expect(result['urn:gov:gsa:unique']).to eq({ id: 3 })
      end
    end

    context 'with invalid ID values' do
      let(:invalid_data) do
        [
          { 'issuer' => 'urn:gov:gsa:valid', 'id' => '1' },
          { 'issuer' => 'urn:gov:gsa:invalid', 'id' => 'not_a_number' },
          { 'issuer' => 'urn:gov:gsa:nil', 'id' => nil },
          { 'issuer' => 'urn:gov:gsa:empty', 'id' => '' },
        ]
      end

      before do
        connection = double('connection')
        allow(DataWarehouseApplicationRecord).to receive(:connection).and_return(connection)
        allow(connection).to receive(:execute).and_return(invalid_data)
      end

      it 'skips invalid IDs and logs errors' do
        expect(Rails.logger).to receive(:error).with(
          'Invalid id value for issuer urn:gov:gsa:invalid: "not_a_number". Skipping row.',
        )
        expect(Rails.logger).to receive(:error).with(
          'Invalid id value for issuer urn:gov:gsa:nil: nil. Skipping row.',
        )
        expect(Rails.logger).to receive(:error).with(
          'Invalid id value for issuer urn:gov:gsa:empty: "". Skipping row.',
        )

        result = helper.get_issuer_sp_mapping
        expect(result.keys).to eq(['urn:gov:gsa:valid'])
        expect(result['urn:gov:gsa:valid']).to eq({ id: 1 })
      end
    end

    context 'when inverting mapping with duplicate IDs' do
      let(:duplicate_id_data) do
        [
          { 'issuer' => 'urn:gov:gsa:first', 'id' => '1' },
          { 'issuer' => 'urn:gov:gsa:second', 'id' => '2' },
          { 'issuer' => 'urn:gov:gsa:third', 'id' => '1' },
        ]
      end

      before do
        connection = double('connection')
        allow(DataWarehouseApplicationRecord).to receive(:connection).and_return(connection)
        allow(connection).to receive(:execute).and_return(duplicate_id_data)
      end

      it 'keeps first issuer for duplicate IDs and logs error' do
        # First it processes the forward mapping (which will skip the duplicate issuer)
        expect(Rails.logger).to receive(:error).with(
          'Duplicate service provider ID found: 1. Keeping first issuer: urn:gov:gsa:first',
        )

        result = helper.get_issuer_sp_mapping(direction: :id_to_issuer)
        expect(result[1]).to eq({ issuer: 'urn:gov:gsa:first' })
        expect(result[2]).to eq({ issuer: 'urn:gov:gsa:second' })
      end
    end
  end

  describe '#get_sp_id_for_issuer' do
    it 'returns the service provider ID for a valid issuer' do
      expect(helper.get_sp_id_for_issuer('urn:gov:gsa:example2')).to eq(2)
    end

    it 'returns nil for an unknown issuer' do
      expect(helper.get_sp_id_for_issuer('unknown:issuer')).to be_nil
    end
  end

  describe '#get_issuer_for_sp_id' do
    it 'returns the issuer string for a valid service provider ID' do
      expect(helper.get_issuer_for_sp_id(3)).to eq('http://localhost:3000')
    end

    it 'returns nil for an unknown service provider ID' do
      expect(helper.get_issuer_for_sp_id(999)).to be_nil
    end
  end

  describe 'error handling' do
    context 'when database query fails' do
      before do
        connection = double('connection')
        allow(DataWarehouseApplicationRecord).to receive(:connection).and_return(connection)
        allow(connection).to receive(:execute).and_raise(StandardError, 'Database error')
      end

      it 'logs error and re-raises' do
        expect(Rails.logger).to receive(:error).with(
          'Failed to fetch service provider issuer map data: Database error',
        )

        expect { helper.get_issuer_sp_mapping }.to raise_error(StandardError, 'Database error')
      end
    end
  end

  describe 'SQL query' do
    it 'generates correct SQL query' do
      expected_query = <<~SQL
        SELECT issuer, id
        FROM idp.service_providers
        WHERE issuer IS NOT NULL
          AND TRIM(issuer) <> ''
          AND id IS NOT NULL
        ORDER BY issuer;
      SQL

      expect(helper.send(:issuer_mapping_query)).to eq(expected_query)
    end
  end
end
