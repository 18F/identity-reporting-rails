# frozen_string_literal: true

require 'rails_helper'
require 'reporting/partner_report_default_monthly'

RSpec.describe Reports::PartnerReportDefaultMonthly do
  subject(:job) { described_class.new }

  let(:report_date) { Time.zone.parse('2024-02-03').end_of_day } # 3rd of month
  let(:time_range) { report_date.all_month }
  let(:issuer1) { 'urn:gov:gsa:openidconnect.profiles:sp:sso:agency1' }
  let(:issuer2) { 'urn:gov:gsa:openidconnect.profiles:sp:sso:agency2' }
  let(:issuer3) { 'urn:gov:gsa:openidconnect.profiles:sp:sso:agency3' }
  let(:bucket_name) { 'test-partner-reports-bucket' }

  let(:sample_report_data) do
    {
      issuer1 => {
        '2024-01-01' => {
          issuer: issuer1,
          provider_information: {
            service_provider_name: 'Agency 1 App',
            agency_name: 'Test Agency 1',
          },
          data: { total_active_users: 1000 },
        },
      },
      issuer2 => {
        '2024-01-01' => {
          issuer: issuer2,
          provider_information: {
            service_provider_name: 'Agency 2 App',
            agency_name: 'Test Agency 2',
          },
          data: { total_active_users: 500 },
        },
      },
    }
  end

  # Data where some SPs have nil reports (missing key columns)
  let(:sample_report_data_with_missing_sp) do
    {
      issuer1 => {
        '2024-01-01' => {
          issuer: issuer1,
          provider_information: {
            service_provider_name: 'Agency 1 App',
            agency_name: 'Test Agency 1',
          },
          data: { total_active_users: 1000 },
        },
      },
      issuer2 => nil, # This SP had missing required fields
      issuer3 => {
        '2024-01-01' => nil, # This SP-month combination had issues
      },
    }
  end

  let(:mock_partner_report) { instance_double(Reporting::PartnerReportDefaultMonthly) }

  before do
    allow(IdentityConfig.store).to receive(:redshift_sia_v3_enabled).and_return(true)
    allow(IdentityConfig.store).to receive(:s3_reports_enabled).and_return(true)
    allow(job).to receive(:bucket_name).and_return(bucket_name)
    allow(job).to receive(:upload_file_to_s3_bucket)

    allow(Reporting::PartnerReportDefaultMonthly).to receive(:new).and_return(mock_partner_report)
    allow(mock_partner_report).to receive(:generate_reports).and_return(sample_report_data)
  end

  describe '#initialize' do
    it 'sets report_date when provided' do
      job_with_date = described_class.new(report_date)
      expect(job_with_date.report_date).to eq(report_date)
    end

    it 'allows report_date to be nil' do
      expect(job.report_date).to be_nil
    end
  end

  describe '#perform' do
    context 'when redshift_sia_v3 is disabled' do
      before do
        allow(IdentityConfig.store).to receive(:redshift_sia_v3_enabled).and_return(false)
      end

      it 'logs warning and returns false' do
        expect(Rails.logger).to receive(:warn).with('Redshift SIA V3 is disabled')
        expect(job.perform(report_date)).to eq(false)
      end
    end

    context 'when s3_reports is disabled' do
      before do
        allow(IdentityConfig.store).to receive(:s3_reports_enabled).and_return(false)
      end

      it 'returns early without processing' do
        expect(job).not_to receive(:generate_and_upload_reports)
        job.perform(report_date)
      end
    end

    context 'when both redshift and s3 are enabled' do
      it 'sets report_date from parameter' do
        job.perform(report_date)
        expect(job.report_date).to eq(report_date)
      end

      it 'uses default date when none provided' do
        freeze_time = Time.zone.parse('2024-02-05')
        expected_default = freeze_time - 3.days

        travel_to freeze_time do
          job.perform
          expect(job.report_date).to be_within(1.second).of(expected_default.end_of_day)
        end
      end

      it 'creates PartnerReportDefaultMonthly with correct time_range' do
        expect(Reporting::PartnerReportDefaultMonthly).to receive(:new).with(
          time_range: time_range,
        ).and_return(mock_partner_report)

        job.perform(report_date)
      end

      it 'logs start message with month and year' do
        # Allow all other log messages
        allow(Rails.logger).to receive(:info)

        expect(Rails.logger).to receive(:info).with(
          'Generating partner default monthly reports for February 2024',
        )

        job.perform(report_date)
      end

      it 'logs completion message' do
        allow(Rails.logger).to receive(:info)
        expect(Rails.logger).to receive(:info).with('Completed partner default monthly report')

        job.perform(report_date)
      end

      it 'uploads reports for each issuer and month' do
        expect(job).to receive(:upload_to_s3).with(
          sample_report_data[issuer1]['2024-01-01'],
          issuer: issuer1,
          month: '2024-01-01',
        )
        expect(job).to receive(:upload_to_s3).with(
          sample_report_data[issuer2]['2024-01-01'],
          issuer: issuer2,
          month: '2024-01-01',
        )

        job.perform(report_date)
      end
    end

    context 'when report generation fails' do
      let(:error_message) { 'Database connection failed' }

      before do
        allow(mock_partner_report).to receive(:generate_reports).and_raise(
          StandardError.new(error_message),
        )
      end

      it 'logs error and re-raises exception' do
        expect(Rails.logger).to receive(:error).with(
          "Failed to generate partner default monthly reports: #{error_message}",
        )
        expect { job.perform(report_date) }.to raise_error(StandardError, error_message)
      end
    end

    context 'when some SPs have missing data' do
      let(:sample_report_data_with_missing_sp) do
        {
          issuer1 => {
            '2024-01-01' => {
              issuer: issuer1,
              provider_information: { service_provider_name: 'Agency 1 App' },
              data: { total_active_users: 1000 },
            },
          },
          issuer2 => nil, # Entire issuer missing
          issuer3 => {
            '2024-01-01' => nil, # Specific month missing
          },
        }
      end

      before do
        allow(mock_partner_report).to receive(:generate_reports).and_return(
          sample_report_data_with_missing_sp,
        )
      end

      it 'skips nil issuer reports and logs warnings' do
        allow(Rails.logger).to receive(:info)

        expect(Rails.logger).to receive(:warn).with(
          "Skipping upload for #{issuer2}: no report data available",
        )
        expect(Rails.logger).to receive(:warn).with(
          "Skipping upload for #{issuer3} month 2024-01-01: no report data available",
        )
        expect(Rails.logger).to receive(:info).with('Upload summary: 1 successful, 2 skipped')

        # Should only upload issuer1
        expect(job).to receive(:upload_to_s3).once

        job.perform(report_date)
      end
    end
  end

  describe '#upload_to_s3' do
    let(:sample_json_data) do
      {
        issuer: issuer1,
        provider_information: { service_provider_name: 'Test App' },
        data: { total_active_users: 1000 },
      }
    end

    it 'uploads to correct S3 path structure' do
      expected_path = "#{issuer1}/monthly/2024-01-01.json"
      expect(job).to receive(:upload_file_to_s3_bucket).with(
        path: expected_path,
        body: JSON.pretty_generate(sample_json_data),
        content_type: 'application/json',
        bucket: bucket_name,
      )

      job.send(:upload_to_s3, sample_json_data, issuer: issuer1, month: '2024-01-01')
    end

    it 'logs successful upload' do
      expect(Rails.logger).to receive(:info).with(
        "Uploaded partner report to S3: #{issuer1}/monthly/2024-01-01.json",
      )

      job.send(:upload_to_s3, sample_json_data, issuer: issuer1, month: '2024-01-01')
    end

    context 'when bucket_name is not present' do
      before do
        allow(job).to receive(:bucket_name).and_return(nil)
      end

      it 'skips upload without error' do
        expect(job).not_to receive(:upload_file_to_s3_bucket)
        job.send(:upload_to_s3, sample_json_data, issuer: issuer1, month: '2024-01-01')
      end
    end
  end

  describe '#json_file' do
    let(:test_data) { { test: 'data', number: 123 } }

    it 'converts data to pretty JSON' do
      result = job.send(:json_file, test_data)
      expect(result).to eq(JSON.pretty_generate(test_data))
    end

    it 'produces valid JSON' do
      result = job.send(:json_file, test_data)
      expect { JSON.parse(result) }.not_to raise_error
    end
  end

  describe 'private method #generate_and_upload_reports' do
    it 'processes nested report structure correctly' do
      expect(job).to receive(:upload_to_s3).exactly(2).times
      job.send(:generate_and_upload_reports, time_range)
    end

    it 'logs processing message for each issuer' do
      allow(job).to receive(:upload_to_s3)
      allow(Rails.logger).to receive(:info)

      # Expect the specific processing logs in order
      expect(Rails.logger).to receive(:info).with(
        "Processing reports for issuer: #{issuer1}",
      ).ordered
      expect(Rails.logger).to receive(:info).with(
        "Processing reports for issuer: #{issuer2}",
      ).ordered

      job.send(:generate_and_upload_reports, time_range)
    end
  end

  describe 'integration with PartnerReportDefaultMonthly' do
    it 'passes time_range correctly to report generator' do
      expect(Reporting::PartnerReportDefaultMonthly).to receive(:new).with(
        time_range: time_range,
      )
      job.perform(report_date)
    end
  end
end
