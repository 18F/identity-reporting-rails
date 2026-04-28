# frozen_string_literal: true

require 'rails_helper'
require 'reporting/partner_report_default'

RSpec.describe Reports::PartnerReportDefault do
  # Testing focused primarily on monthly report cadence for now
  subject(:job) { described_class.new }

  let(:report_date) { Time.zone.parse('2026-04-15').end_of_day }
  let(:calendar_id) { 20260401 }
  let(:period_date) { '2026-04-01' }
  let(:issuer1) { 'urn:gov:gsa:openidconnect.profiles:sp:sso:agency1' }
  let(:issuer2) { 'urn:gov:gsa:openidconnect.profiles:sp:sso:agency2' }
  let(:issuer3) { 'urn:gov:gsa:openidconnect.profiles:sp:sso:agency3' }
  let(:bucket_name) { 'test-partner-reports-bucket' }

  # Flat structure: { issuer => data }
  let(:sample_report_data) do
    {
      issuer1 => {
        issuer: issuer1,
        provider_information: {
          service_provider_name: 'Agency 1 App',
          agency_name: 'Test Agency 1',
        },
        data: { total_active_users: 1000 },
      },
      issuer2 => {
        issuer: issuer2,
        provider_information: {
          service_provider_name: 'Agency 2 App',
          agency_name: 'Test Agency 2',
        },
        data: { total_active_users: 500 },
      },
    }
  end

  # Data with some nil issuers (failed data integrity)
  let(:sample_report_data_with_missing_sp) do
    {
      issuer1 => {
        issuer: issuer1,
        provider_information: { service_provider_name: 'Agency 1 App' },
        data: { total_active_users: 1000 },
      },
      issuer2 => nil, # This issuer had duplicate/integrity issues
      issuer3 => nil, # This issuer also had issues
    }
  end

  let(:mock_partner_report) { instance_double(Reporting::PartnerReportDefault) }

  before do
    allow(IdentityConfig.store).to receive(:redshift_sia_v3_enabled).and_return(true)
    allow(IdentityConfig.store).to receive(:s3_reports_enabled).and_return(true)
    allow(job).to receive(:bucket_name).and_return(bucket_name)
    allow(job).to receive(:upload_file_to_s3_bucket)
    allow(Reporting::PartnerReportDefault).to receive(:new).and_return(mock_partner_report)
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
        freeze_time = Time.zone.parse('2026-02-05')
        expected_default = freeze_time - described_class::REPORT_DELAY_DAYS.days
        travel_to freeze_time do
          job.perform
          expect(job.report_date).to be_within(1.second).of(expected_default.end_of_day)
        end
      end

      it 'creates PartnerReportDefault with correct parameters' do
        expect(Reporting::PartnerReportDefault).to receive(:new).with(
          calendar_id: calendar_id,
          target_date: report_date,
          report_cadence: 'monthly',
        ).and_return(mock_partner_report)
        job.perform(report_date)
      end

      it 'logs start message with calendar_id and period_date' do
        allow(Rails.logger).to receive(:info)
        expect(Rails.logger).to receive(:info).with(
          "Generating partner default monthly reports for calendar_id: "\
          "#{calendar_id} (#{period_date})",
        )
        job.perform(report_date)
      end

      it 'logs completion message' do
        allow(Rails.logger).to receive(:info)
        expect(Rails.logger).to receive(:info).with('Completed partner default monthly report')
        job.perform(report_date)
      end

      it 'uploads reports for each issuer' do
        expect(job).to receive(:upload_to_s3).with(
          sample_report_data[issuer1],
          issuer: issuer1,
          period_date: period_date,
        )
        expect(job).to receive(:upload_to_s3).with(
          sample_report_data[issuer2],
          issuer: issuer2,
          period_date: period_date,
        )
        job.perform(report_date)
      end

      it 'logs upload summary' do
        allow(job).to receive(:upload_to_s3)
        allow(Rails.logger).to receive(:info)
        expect(Rails.logger).to receive(:info).with('Upload summary: 2 successful, 0 skipped')
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

    context 'when some issuers have nil data' do
      before do
        allow(mock_partner_report).to receive(:generate_reports).and_return(
          sample_report_data_with_missing_sp,
        )
      end

      it 'skips nil issuer reports and logs warnings' do
        allow(Rails.logger).to receive(:info)
        expect(Rails.logger).to receive(:warn).with(
          "Skipping upload for #{issuer2}: report generation failed and returned nil",
        )
        expect(Rails.logger).to receive(:warn).with(
          "Skipping upload for #{issuer3}: report generation failed and returned nil",
        )
        expect(Rails.logger).to receive(:info).with('Upload summary: 1 successful, 2 skipped')

        # Should only upload issuer1
        expect(job).to receive(:upload_to_s3).once
        job.perform(report_date)
      end
    end
  end

  describe '#calculate_calendar_id' do
    it 'calculates monthly calendar_id correctly' do
      test_date = Time.zone.parse('2026-03-15')
      result = job.send(:calculate_calendar_id, test_date)
      expect(result).to eq(20260301)
    end
  end

  it 'calculates daily calendar_id correctly' do
    # REPORT_CADENCE currently hardcoded as monthly, but eventually we will implement
    # daily and weekly
    allow(job.class).to receive(:const_get).with(:REPORT_CADENCE).and_return('daily')
    test_date = Time.zone.parse('2026-03-15')
    result = job.send(:calculate_calendar_id, test_date)
    expect(result).to eq(20260315)
  end

  describe '#calculate_period_start_date' do
    it 'calculates monthly period start date correctly' do
      test_date = Time.zone.parse('2026-03-15')
      result = job.send(:calculate_period_start_date, test_date)
      expect(result).to eq('2026-03-01')
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
        expected_path = "#{issuer1}/monthly/2026-04-01.json"
        expect(job).to receive(:upload_file_to_s3_bucket).with(
          path: expected_path,
          body: JSON.pretty_generate(sample_json_data),
          content_type: 'application/json',
          bucket: bucket_name,
        )
        job.send(:upload_to_s3, sample_json_data, issuer: issuer1, period_date: '2026-04-01')
      end

      it 'logs successful upload' do
        expect(Rails.logger).to receive(:info).with(
          "Uploaded partner report to S3: #{issuer1}/monthly/2026-04-01.json",
        )
        job.send(:upload_to_s3, sample_json_data, issuer: issuer1, period_date: '2026-04-01')
      end

      context 'when bucket_name is not present' do
        before do
          allow(job).to receive(:bucket_name).and_return(nil)
        end

        it 'skips upload without error' do
          expect(job).not_to receive(:upload_file_to_s3_bucket)
          job.send(:upload_to_s3, sample_json_data, issuer: issuer1, period_date: '2026-04-01')
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

    describe 'integration with PartnerReportDefault' do
      it 'passes correct parameters to report generator' do
        expect(Reporting::PartnerReportDefault).to receive(:new).with(
          calendar_id: calendar_id,
          target_date: report_date,
          report_cadence: 'monthly',
        )
        job.perform(report_date)
      end
    end
  end
end
