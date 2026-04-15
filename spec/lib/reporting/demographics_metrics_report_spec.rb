require 'rails_helper'
require 'reporting/demographics_metrics_report'

RSpec.describe Reporting::DemographicsMetricsReport do
  let(:issuer) { 'my:example:issuer' }
  let(:time_range) { Date.new(2022, 1, 1).in_time_zone('UTC').all_quarter }
  let(:agency_abbreviation) { 'Test_Agency' }
  let(:current_year) { Time.zone.today.year }
  let(:expected_definitions_table) do
    [
      ['Metric', 'Unit', 'Definition'],
      ['Age range/Verification Demographics', 'Count',
       "The number of #{agency_abbreviation} users who verified within " \
         "the reporting period, grouped by age in " \
         "10 year range."],
      ['Geographic area/Verification Demographics', 'Count',
       "The number of #{agency_abbreviation} users who verified within " \
         "the reporting period, grouped by state."],
    ]
  end

  let(:expected_overview_table) do
    [
      ['Report Timeframe', "#{time_range.begin} to #{time_range.end}"],
      ['Report Generated', Time.zone.today.to_s],
      ['Issuer', issuer],
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

  subject(:report) do
    Reporting::DemographicsMetricsReport.new(
      issuers: [issuer],
      agency_abbreviation: agency_abbreviation,
      time_range: time_range,
    )
  end

  before do
    travel_to Time.zone.now.beginning_of_day

    # Create SP redirect events for 6 users
    6.times do |i|
      Event.create!(
        id: "event_sp_#{i}",
        name: 'SP redirect initiated',
        user_id: "user#{i + 1}",
        service_provider: issuer,
        cloudwatch_timestamp: time_range.begin + 1.day,
        message: {
          properties: {
            event_properties: {
              ial: 2,
            },
            sp_request: {
              facial_match: true,
            },
          },
        }.to_json,
        new_event: true,
      )
    end

    # Create doc auth success events with demographic data
    demo_data = [
      { user_id: 'user1', birth_year: current_year - 15, state: 'MD' },
      { user_id: 'user2', birth_year: current_year - 16, state: 'MD' },
      { user_id: 'user3', birth_year: current_year - 25, state: 'DE' },
      { user_id: 'user4', birth_year: current_year - 26, state: 'DE' },
      { user_id: 'user5', birth_year: current_year - 35, state: 'VA' },
      { user_id: 'user6', birth_year: current_year - 36, state: 'VA' },
    ]

    demo_data.each_with_index do |data, i|
      FactoryBot.create(
        :event,
        id: "event_doc_#{i}",
        name: 'IdV: doc auth verify proofing results',
        user_id: data[:user_id],
        service_provider: issuer,
        cloudwatch_timestamp: time_range.begin + 1.day,
        message: {
          properties: {
            event_properties: {
              success: true,
              proofing_results: {
                biographical_info: {
                  birth_year: data[:birth_year].to_s,
                  state_id_jurisdiction: data[:state],
                },
              },
            },
          },
        }.to_json,
        new_event: true,
        success: true,
      )
    end
  end

  describe '#definitions_table' do
    it 'renders a definitions table' do
      aggregate_failures do
        report.definitions_table.zip(expected_definitions_table).each do |actual, expected|
          expect(actual).to eq(expected)
        end
      end
    end
  end

  describe '#overview_table' do
    it 'renders an overview table' do
      aggregate_failures do
        report.overview_table.zip(expected_overview_table).each do |actual, expected|
          expect(actual).to eq(expected)
        end
      end
    end
  end

  describe '#age_metrics_table' do
    it 'renders an age metrics table' do
      aggregate_failures do
        report.age_metrics_table.zip(expected_age_metrics_table).each do |actual, expected|
          expect(actual).to eq(expected)
        end
      end
    end

    context 'when there is an error' do
      before do
        allow(report).to receive(:age_bins).and_raise(StandardError, 'Test error')
      end

      it 'returns an error table' do
        expect(report.age_metrics_table).to eq(
          [
            ['Error', 'Message'],
            ['StandardError', 'Test error'],
          ],
        )
      end
    end
  end

  describe '#state_metrics_table' do
    it 'renders a state metrics table' do
      aggregate_failures do
        report.state_metrics_table.zip(expected_state_metrics_table).each do |actual, expected|
          expect(actual).to eq(expected)
        end
      end
    end

    context 'when there is an error' do
      before do
        allow(report).to receive(:state_counts).and_raise(StandardError, 'Test error')
      end

      it 'returns an error table' do
        expect(report.state_metrics_table).to eq(
          [
            ['Error', 'Message'],
            ['StandardError', 'Test error'],
          ],
        )
      end
    end
  end

  describe '#as_reports' do
    let(:expected_reports) do
      [
        {
          title: 'Definitions',
          filename: 'definitions',
          table: expected_definitions_table,
        },
        {
          title: 'Overview',
          filename: 'overview',
          table: expected_overview_table,
        },
        {
          title: "#{agency_abbreviation} Age Metrics",
          filename: 'age_metrics',
          table: expected_age_metrics_table,
        },
        {
          title: "#{agency_abbreviation} State Metrics",
          filename: 'state_metrics',
          table: expected_state_metrics_table,
        },
      ]
    end

    it 'returns expected reports structure' do
      expect(report.as_reports).to eq expected_reports
    end
  end

  describe 'SQL query execution' do
    it 'executes the demographics query successfully' do
      query_results = Event.connection.execute(report.send(:demographics_query)).to_a
      expect(query_results.length).to eq(6)

      # Verify data structure
      expect(query_results.first.keys).to contain_exactly('user_id', 'birth_year', 'state')

      # Verify we have the expected users
      user_ids = query_results.map { |row| row['user_id'] }.sort
      expect(user_ids).to eq(['user1', 'user2', 'user3', 'user4', 'user5', 'user6'])
    end
  end

  describe 'data processing with edge cases' do
    before do
      # Clear existing data
      Event.delete_all

      # Create SP redirect events
      4.times do |i|
        Event.create!(
          id: "edge_sp_#{i}",
          name: 'SP redirect initiated',
          user_id: "edge_user#{i + 1}",
          service_provider: issuer,
          cloudwatch_timestamp: time_range.begin + 1.day,
          message: {
            properties: {
              event_properties: { ial: 2 },
              sp_request: { facial_match: true },
            },
          }.to_json,
          new_event: true,
        )
      end

      # Create doc auth events with edge cases
      edge_cases = [
        { user_id: 'edge_user1', birth_year: current_year - 15, state: 'MD' }, # Normal
        { user_id: 'edge_user2', birth_year: nil, state: 'VA' }, # Missing birth year
        { user_id: 'edge_user3', birth_year: current_year - 35, state: '' }, # Empty state
        { user_id: 'edge_user4', birth_year: current_year + 10, state: 'CA' }, # Future birth year
      ]

      edge_cases.each_with_index do |data, i|
        FactoryBot.create(
          :event,
          id: "edge_doc_#{i}",
          name: 'IdV: doc auth verify proofing results',
          user_id: data[:user_id],
          service_provider: issuer,
          cloudwatch_timestamp: time_range.begin + 1.day,
          message: {
            properties: {
              event_properties: {
                success: true,
                proofing_results: {
                  biographical_info: {
                    birth_year: data[:birth_year]&.to_s,
                    state_id_jurisdiction: data[:state],
                  },
                },
              },
            },
          }.to_json,
          new_event: true,
          success: true,
        )
      end
    end

    it 'handles missing birth years in age bins' do
      age_bins = report.send(:age_bins)
      total_count = age_bins.values.sum
      expect(total_count).to eq(2) # Only edge_user1 and edge_user3 should be counted
    end

    it 'handles empty states in state counts' do
      state_counts = report.send(:state_counts)
      expect(state_counts.keys).not_to include('')
      expect(state_counts.keys).to contain_exactly('CA', 'MD', 'VA')
    end
  end

  describe 'multiple issuers' do
    let(:issuers) { ['issuer1', 'issuer2', 'issuer3'] }

    subject(:report) do
      Reporting::DemographicsMetricsReport.new(
        issuers: issuers,
        agency_abbreviation: agency_abbreviation,
        time_range: time_range,
      )
    end

    it 'formats multiple issuers correctly in SQL' do
      expect(report.send(:formatted_issuers)).to eq("'issuer1', 'issuer2', 'issuer3'")
    end

    it 'displays multiple issuers in overview table' do
      overview = report.overview_table
      expect(overview[2][1]).to eq('issuer1, issuer2, issuer3')
    end
  end
end
