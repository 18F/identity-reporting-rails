require 'rails_helper'
require 'reporting/demographics_metrics_report'

RSpec.describe Reporting::DemographicsMetricsReport do
  let(:issuer_string) { 'my:example:issuer' }
  let(:time_range) { Date.new(2026, 5, 3).in_time_zone('UTC').all_quarter }
  let(:current_year) { Date.current.year }

  let(:expected_definitions_table) do
    [
      ['Metric', 'Unit', 'Definition'],
      ['Age range/Verification Demographics', 'Count',
       'The number of users for this issuer who verified within ' \
         'the reporting period, grouped by age in ' \
         '10 year range.'],
      ['Geographic area/Verification Demographics', 'Count',
       'The number of users for this issuer who verified within ' \
         'the reporting period, grouped by state.'],
    ]
  end

  let(:expected_overview_table) do
    [
      ['Report Timeframe', "#{time_range.begin} to #{time_range.end}"],
      ['Report Generated', Date.current.to_s],
      ['Issuer', issuer_string],
    ]
  end

  let(:expected_age_metrics_table) do
    [
      ['Age Range', 'User Count'],
      ['10-19', '2'],
      ['20-29', '2'],
      ['30-39', '2'],
    ]
  end

  let(:expected_state_metrics_table) do
    [
      ['State', 'User Count'],
      ['DE', '2'],
      ['MD', '2'],
      ['VA', '2'],
    ]
  end

  let(:mock_query_results) do
    [
      { 'user_id' => 'user1', 'birth_year' => current_year - 15, 'state' => 'MD' },
      { 'user_id' => 'user2', 'birth_year' => current_year - 16, 'state' => 'MD' },
      { 'user_id' => 'user3', 'birth_year' => current_year - 25, 'state' => 'DE' },
      { 'user_id' => 'user4', 'birth_year' => current_year - 26, 'state' => 'DE' },
      { 'user_id' => 'user5', 'birth_year' => current_year - 35, 'state' => 'VA' },
      { 'user_id' => 'user6', 'birth_year' => current_year - 36, 'state' => 'VA' },
    ]
  end

  subject(:report) do
    described_class.new(
      issuer_string: issuer_string,
      time_range: time_range,
    )
  end

  before do
    # Mock the database connection and query execution
    connection = double('connection')
    allow(DataWarehouseApplicationRecord).to receive(:connection).and_return(connection)
    allow(connection).to receive(:adapter_name).and_return('PostgreSQL')
    allow(connection).to receive(:quote) { |str| "'#{str}'" }

    # Mock the exec_query to return our test data
    allow(connection).to receive(:exec_query).
      with(anything, 'DemographicsMetricsReport', anything).
      and_return(mock_query_results)
  end

  describe '#definitions_table' do
    it 'renders a definitions table' do
      expect(report.definitions_table).to eq(expected_definitions_table)
    end
  end

  describe '#overview_table' do
    it 'renders an overview table' do
      expect(report.overview_table).to eq(expected_overview_table)
    end
  end

  describe '#age_metrics_table' do
    it 'renders an age metrics table' do
      expect(report.age_metrics_table).to eq(expected_age_metrics_table)
    end

    context 'when there is an error in age_bins' do
      before do
        allow(report).to receive(:age_bins).and_raise(StandardError, 'Test error')
      end

      it 'logs the error and re-raises it' do
        expect(Rails.logger).to receive(:error).with(
          "Failed to generate age metrics table for issuer #{issuer_string}: Test error",
        )

        expect { report.age_metrics_table }.to raise_error(StandardError, 'Test error')
      end
    end
  end

  describe '#state_metrics_table' do
    it 'renders a state metrics table' do
      expect(report.state_metrics_table).to eq(expected_state_metrics_table)
    end

    context 'when there is an error in state_counts' do
      before do
        allow(report).to receive(:state_counts).and_raise(StandardError, 'Test error')
      end

      it 'logs the error and re-raises it' do
        expect(Rails.logger).to receive(:error).with(
          "Failed to generate state metrics table for issuer #{issuer_string}: Test error",
        )

        expect { report.state_metrics_table }.to raise_error(StandardError, 'Test error')
      end
    end
  end

  describe '#as_reports' do
    let(:expected_reports) do
      [
        {
          title: 'Definitions',
          table: expected_definitions_table,
          filename: 'definitions',
        },
        {
          title: 'Overview',
          table: expected_overview_table,
          filename: 'overview',
        },
        {
          title: 'Age Metrics',
          table: expected_age_metrics_table,
          filename: 'age_metrics',
        },
        {
          title: 'State Metrics',
          table: expected_state_metrics_table,
          filename: 'state_metrics',
        },
      ]
    end

    it 'returns expected reports structure' do
      expect(report.as_reports).to eq(expected_reports)
    end
  end

  describe 'data processing with edge cases' do
    let(:edge_case_results) do
      [
        { 'user_id' => 'edge_user1', 'birth_year' => current_year - 15, 'state' => 'MD' }, # Normal
        { 'user_id' => 'edge_user2', 'birth_year' => nil, 'state' => 'VA' }, # Missing birth year

        { 'user_id' => 'edge_user3',
          'birth_year' => current_year - 35,
          'state' => '' }, # Empty state

        { 'user_id' => 'edge_user4',
          'birth_year' => current_year - 35,
          'state' => nil }, # Nil state

        { 'user_id' => 'edge_user5',
          'birth_year' => current_year + 10,
          'state' => 'CA' }, # Future birth year (negative age)

        { 'user_id' => 'edge_user6',
          'birth_year' => current_year - 150,
          'state' => 'NY' }, # Age > 140
      ]
    end

    before do
      connection = double('connection')
      allow(DataWarehouseApplicationRecord).to receive(:connection).and_return(connection)
      allow(connection).to receive(:adapter_name).and_return('PostgreSQL')
      allow(connection).to receive(:quote) { |str| "'#{str}'" }
      allow(connection).to receive(:exec_query).
        with(anything, 'DemographicsMetricsReport', anything).
        and_return(edge_case_results)
    end

    describe 'age validation and data quality' do
      it 'handles missing birth years and logs warning' do
        expect(Rails.logger).to receive(:warn).with(
          'Demographics age data quality: 6 total records, '\
          '1 with nil birth_year, 2 with invalid age',
        )

        age_bins = report.send(:age_bins)
        total_count = age_bins.values.sum
        expect(total_count).to eq(3) # Only valid ages should be counted
      end

      it 'excludes invalid ages (negative and > 140)' do
        age_bins = report.send(:age_bins)
        expect(age_bins['10-19']).to eq(1) # edge_user1
        expect(age_bins['30-39']).to eq(2) # edge_user3 and edge_user4
        expect(age_bins.keys).not_to include('140-149') # Age > 140 excluded
        expect(age_bins.keys).not_to include('-10--1') # Negative age excluded
      end
    end

    describe 'state validation and data quality' do
      it 'handles empty/nil states and logs warning' do
        expect(Rails.logger).to receive(:warn).with(
          'Demographics state data quality: 6 total records, 2 with blank/nil state',
        )

        state_counts = report.send(:state_counts)
        expect(state_counts.keys).not_to include('')
        expect(state_counts.keys).not_to include(nil)
      end

      it 'counts only valid states' do
        state_counts = report.send(:state_counts)
        expect(state_counts).to eq(
          {
            'CA' => 1,
            'MD' => 1,
            'NY' => 1,
            'VA' => 1,
          },
        )
      end

      it 'uppercases state codes' do
        # Add a lowercase state to test data
        mixed_case_results = edge_case_results + [
          { 'user_id' => 'edge_user7', 'birth_year' => current_year - 25, 'state' => 'tx' },
        ]

        connection = double('connection')
        allow(DataWarehouseApplicationRecord).to receive(:connection).and_return(connection)
        allow(connection).to receive(:adapter_name).and_return('PostgreSQL')
        allow(connection).to receive(:quote) { |str| "'#{str}'" }
        allow(connection).to receive(:exec_query).and_return(mixed_case_results)

        state_counts = report.send(:state_counts)
        expect(state_counts['TX']).to eq(1)
        expect(state_counts.keys).not_to include('tx')
      end
    end
  end

  describe 'empty data handling' do
    before do
      connection = double('connection')
      allow(DataWarehouseApplicationRecord).to receive(:connection).and_return(connection)
      allow(connection).to receive(:adapter_name).and_return('PostgreSQL')
      allow(connection).to receive(:quote) { |str| "'#{str}'" }
      allow(connection).to receive(:exec_query).
        with(anything, 'DemographicsMetricsReport', anything).
        and_return([])
    end

    it 'logs info message when no data found' do
      expect(Rails.logger).to receive(:info).with(
        "No demographic data found for issuer #{issuer_string} in time range " \
        "#{time_range.begin} to #{time_range.end}. Generating empty reports.",
      )

      report.send(:user_data)
    end

    it 'returns empty age bins' do
      expect(report.send(:age_bins)).to eq({})
    end

    it 'returns empty state counts' do
      expect(report.send(:state_counts)).to eq({})
    end

    it 'generates tables with headers only' do
      expect(report.age_metrics_table).to eq([['Age Range', 'User Count']])
      expect(report.state_metrics_table).to eq([['State', 'User Count']])
    end
  end

  describe 'SQL query generation' do
    it 'generates correct query parameters' do
      params = report.send(:query_parameters)

      expect(params).to eq(
        [
          issuer_string,
          '2026-04-01T00:00:00Z',
          '2026-06-30T23:59:59Z',
          'SP redirect initiated',
          'IdV: doc auth verify proofing results',
        ],
      )
    end

    it 'formats time correctly for SQL' do
      expect(report.send(:formatted_start_time)).to eq('2026-04-01T00:00:00Z')
      expect(report.send(:formatted_end_time)).to eq('2026-06-30T23:59:59Z')
    end
  end
end
