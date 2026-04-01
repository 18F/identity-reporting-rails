require 'rails_helper'
require 'reporting/partner_idv_report'

RSpec.describe Reporting::PartnerIdvReport do
  let(:service_provider_id) { 42 }
  let(:month_start_calendar_id) { 202401 }

  let(:connection) { instance_double('Connection') }

  let(:result_rows) do
    [
      {
        'issuer' => 'urn:test:issuer',
        'service_provider_name' => 'Test SP',
        'count_inauthentic_doc' => 5,
      },
    ]
  end

  let(:result) { instance_double('Result', to_a: result_rows) }

  subject(:report) do
    described_class.new(
      service_provider_id: service_provider_id,
      month_start_calendar_id: month_start_calendar_id,
      connection: connection,
    )
  end

  before do
    # Quote should produce something deterministic for our SQL expectations.
    allow(connection).to receive(:quote) { |value| value.to_s }
    allow(connection).to receive(:execute).and_return(result)
  end

  describe '#fetch_results' do
    it 'returns an array of hashes' do
      expect(report.fetch_results).to eq(result_rows)
    end

    it 'executes SQL with substituted parameters' do
      expect(connection).to receive(:execute) do |sql|
        expect(sql).to include('202401')
        expect(sql).to include('42')
      end.and_return(result)

      report.fetch_results
    end
  end

  describe '#results_json' do
    it 'returns a JSON string of the fetch results' do
      json = report.results_json

      expect(json).to be_a(String)
      parsed = JSON.parse(json)
      expect(parsed).to be_an(Array)
      expect(parsed.first['issuer']).to eq('urn:test:issuer')
    end
  end
end
