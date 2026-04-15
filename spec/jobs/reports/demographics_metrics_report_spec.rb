# frozen_string_literal: true

require 'rails_helper'

RSpec.describe Reports::DemographicsMetricsReport do
  subject(:report) { Reports::DemographicsMetricsReport.new }

  let(:report_date) { Time.zone.parse('2023-03-31').end_of_day }
  let(:time_range) { report_date.all_quarter }

  let(:mock_report_configs) do
    [
      {
        'agency_abbreviation' => 'SSA',
        'issuers' => ['urn:gov:gsa:openidconnect.profiles:sp:sso:ssa:sample_app'],
      },
      {
        'agency_abbreviation' => 'VA',
        'issuers' => ['urn:gov:gsa:openidconnect.profiles:sp:sso:va:sample_app'],
      },
    ]
  end

  let(:sample_reports) do
    [
      {
        title: 'Definitions',
        table: [['Metric', 'Unit', 'Definition']],
        filename: 'definitions',
      },
      {
        title: 'Overview',
        table: [['Report Timeframe', "#{time_range.begin} to #{time_range.end}"]],
        filename: 'overview',
      },
      {
        title: 'SSA Age Metrics',
        table: [['Age Range', 'User Count'], ['20-29', '5']],
        filename: 'age_metrics',
      },
      {
        title: 'SSA State Metrics',
        table: [['State', 'User Count'], ['CA', '3']],
        filename: 'state_metrics',
      },
    ]
  end

  before do
    # Mock the config store - borrowed pattern from other specs
    allow(IdentityConfig.store).to receive(:redshift_sia_v3_enabled).and_return(true)
    allow(IdentityConfig.store).to receive(:s3_reports_enabled).and_return(true)
    allow(IdentityConfig.store).to receive(:demographics_metrics_report_configs).and_return(
      mock_report_configs,
    )

    # Mock the S3 upload methods - based on BaseReport pattern from other specs
    allow(report).to receive(:bucket_name).and_return('test-bucket')
    allow(report).to receive(:generate_s3_paths).and_return(['latest_path', 'full_path'])
    allow(report).to receive(:upload_file_to_s3_bucket)
  end

  describe '#perform' do
    context 'when redshift_sia_v3 is disabled' do
      before do
        allow(IdentityConfig.store).to receive(:redshift_sia_v3_enabled).and_return(false)
      end

      it 'logs warning and returns false' do
        # Borrowed pattern from FraudMetricsReport spec
        expect(Rails.logger).to receive(:warn).with('Redshift SIA V3 is disabled')
        expect(report.perform(report_date)).to eq(false)
      end
    end

    context 'when s3_reports is disabled' do
      before do
        allow(IdentityConfig.store).to receive(:s3_reports_enabled).and_return(false)
      end

      it 'returns early without processing' do
        # Based on intuition - should exit early if S3 reports disabled
        expect(report).not_to receive(:generate_and_upload_report)
        report.perform(report_date)
      end
    end

    context 'when both redshift and s3 are enabled' do
      let(:mock_demographics_report) { instance_double(Reporting::DemographicsMetricsReport) }

      before do
        # Mock the demographics report generation - based on intuition of how the classes interact
        allow(Reporting::DemographicsMetricsReport).to receive(:new).and_return(
          mock_demographics_report,
        )
        allow(mock_demographics_report).to receive(:as_reports).and_return(sample_reports)
      end

      it 'processes all configured agencies' do
        # Based on intuition - should call the report generation for each config
        expect(Reporting::DemographicsMetricsReport).to receive(:new).with(
          issuers: ['urn:gov:gsa:openidconnect.profiles:sp:sso:ssa:sample_app'],
          agency_abbreviation: 'SSA',
          time_range: time_range,
        ).and_return(mock_demographics_report)

        expect(Reporting::DemographicsMetricsReport).to receive(:new).with(
          issuers: ['urn:gov:gsa:openidconnect.profiles:sp:sso:va:sample_app'],
          agency_abbreviation: 'VA',
          time_range: time_range,
        ).and_return(mock_demographics_report)

        report.perform(report_date)
      end

      it 'uploads reports to S3 for each agency' do
        # Based on intuition - should upload each report file to S3
        expect(report).to receive(:upload_file_to_s3_bucket).exactly(8).times

        report.perform(report_date)
      end

      it 'logs start and completion messages' do
        # Allow any number of upload messages but verify the key start/completion messages
        allow(Rails.logger).to receive(:info) # Allow any info messages

        expect(Rails.logger).to receive(:info).with('Generating demographics report for SSA')
        expect(Rails.logger).to receive(:info).with('Completed demographics report for SSA')
        expect(Rails.logger).to receive(:info).with('Generating demographics report for VA')
        expect(Rails.logger).to receive(:info).with('Completed demographics report for VA')

        report.perform(report_date)
      end
    end

    context 'when an error occurs during report generation' do
      let(:error_message) { 'Database connection failed' }

      before do
        allow(Reporting::DemographicsMetricsReport).to receive(:new).and_raise(
          StandardError.new(error_message),
        )
      end

      it 'logs error and re-raises exception' do
        # Based on the error handling pattern in the wrapper
        expect(Rails.logger).to receive(:error).with(
          "Failed to generate demographics report for SSA: #{error_message}",
        )

        expect { report.perform(report_date) }.to raise_error(StandardError, error_message)
      end
    end
  end

  describe '#csv_file' do
    let(:test_array) do
      [
        ['Header 1', 'Header 2'],
        ['Value 1', 'Value 2'],
        ['Value 3', 'Value 4'],
      ]
    end

    it 'converts array to CSV format' do
      # Borrowed test pattern from other report specs - testing CSV generation
      csv_output = report.send(:csv_file, test_array)
      parsed_csv = CSV.parse(csv_output)

      expect(parsed_csv).to eq(test_array)
    end
  end

  describe 'private methods' do
    describe '#report_configs' do
      it 'returns demographics metrics report configs from IdentityConfig' do
        # Based on intuition - simple delegation to config store
        expect(report.send(:report_configs)).to eq(mock_report_configs)
      end
    end

    describe '#demographics_reports' do
      let(:mock_demographics_report) { instance_double(Reporting::DemographicsMetricsReport) }
      let(:issuers) { ['test_issuer'] }
      let(:agency) { 'TEST' }

      before do
        report.instance_variable_set(:@report_date, report_date)
        allow(Reporting::DemographicsMetricsReport).to receive(:new).and_return(
          mock_demographics_report,
        )
        allow(mock_demographics_report).to receive(:as_reports).and_return(sample_reports)
      end

      it 'creates DemographicsMetricsReport with correct parameters' do
        # Based on intuition - testing the parameter passing
        expect(Reporting::DemographicsMetricsReport).to receive(:new).with(
          issuers: issuers,
          agency_abbreviation: agency,
          time_range: time_range,
        )

        report.send(:demographics_reports, issuers, agency)
      end
    end
  end
end
