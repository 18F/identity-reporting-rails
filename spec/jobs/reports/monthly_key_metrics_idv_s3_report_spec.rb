# frozen_string_literal: true

require 'rails_helper'

RSpec.describe Reports::MonthlyKeyMetricsIdvS3Report do
  subject(:report) { described_class.new(report_date) }

  let(:report_date) { Date.new(2026, 3, 15).in_time_zone('UTC') }
  let(:normalized_date) { report_date }

  let(:monthly_range) do
    normalized_date.beginning_of_month.beginning_of_day..normalized_date.end_of_month.end_of_day
  end
  let(:trailing_range) do
    (normalized_date - 30.days).beginning_of_day..normalized_date.end_of_day
  end

  let(:mock_monthly_report) do
    instance_double(
      Reporting::IdentityVerificationReport,
      time_range: monthly_range,
      idv_started: 100,
      successfully_verified_users: 70,
      blanket_proofing_rate: 0.7,
      idv_final_resolution: 80,
      idv_final_resolution_rate: 0.8,
      verified_user_count: 1234,
    )
  end

  let(:mock_trailing_report) do
    instance_double(
      Reporting::IdentityVerificationReport,
      time_range: trailing_range,
      idv_started: 200,
      idv_doc_auth_welcome_submitted: 180,
      idv_doc_auth_image_vendor_submitted: 150,
      idv_doc_auth_socure_verification_data_requested: 10,
      successfully_verified_users: 140,
      idv_doc_auth_rejected: 20,
      idv_fraud_rejected: 5,
      blanket_proofing_rate: 0.7,
      intent_proofing_rate: 0.78,
      actual_proofing_rate: 0.875,
      industry_proofing_rate: 0.875,
    )
  end

  before do
    allow(IdentityConfig.store).to receive(:redshift_sia_v3_enabled).and_return(true)
    allow(IdentityConfig.store).to receive(:s3_reports_enabled).and_return(true)

    allow(report).to receive(:bucket_name).and_return('test-bucket')
    allow(report).to receive(:generate_base_s3_path).and_return('s3-base-path/')
    allow(report).to receive(:upload_file_to_s3_bucket)

    allow(Reporting::IdentityVerificationReport).to receive(:new).
      with(time_range: monthly_range).and_return(mock_monthly_report)

    allow(Reporting::IdentityVerificationReport).to receive(:new).
      with(time_range: trailing_range).and_return(mock_trailing_report)
  end

  describe '#perform' do
    context 'when redshift_sia_v3 is disabled' do
      before do
        allow(IdentityConfig.store).to receive(:redshift_sia_v3_enabled).and_return(false)
      end

      it 'logs a warning and returns false' do
        expect(Rails.logger).to receive(:warn).with(
          "#{described_class::REPORT_NAME}: Redshift SIA V3 is disabled",
        )
        expect(report.perform(report_date)).to eq(false)
      end

      it 'does not upload anything' do
        expect(report).not_to receive(:upload_file_to_s3_bucket)
        report.perform(report_date)
      end
    end

    context 'when s3_reports is disabled' do
      before do
        allow(IdentityConfig.store).to receive(:s3_reports_enabled).and_return(false)
      end

      it 'logs a warning and returns false' do
        expect(Rails.logger).to receive(:warn).with(
          "#{described_class::REPORT_NAME}: S3 reports are disabled",
        )
        expect(report.perform(report_date)).to eq(false)
      end

      it 'does not upload anything' do
        expect(report).not_to receive(:upload_file_to_s3_bucket)
        report.perform(report_date)
      end
    end

    context 'when report_date is blank' do
      it 'raises an ArgumentError' do
        allow(Time.zone).to receive(:yesterday).and_return(nil)
        expect { described_class.new.perform(nil) }.to raise_error(
          ArgumentError, 'report_date must be a valid Date or Time object'
        )
      end
    end

    context 'when report_date is in the future' do
      let(:report_date) { Time.zone.tomorrow.end_of_day }

      it 'raises an ArgumentError' do
        expect { report.perform(report_date) }.to raise_error(
          ArgumentError, 'report_date cannot be in the future'
        )
      end
    end

    context 'when report_date is a non-UTC end-of-day time' do
      # 2026-03-15 23:59:59 in a negative-offset zone would roll to 2026-03-16
      # in UTC if not normalized via .to_date first.
      let(:report_date) do
        Time.use_zone('America/Puerto_Rico') { Time.zone.parse('2026-03-15').end_of_day }
      end

      let(:expected_monthly_range) do
        d = Date.new(2026, 3, 15).in_time_zone('UTC')
        d.beginning_of_month.beginning_of_day..d.end_of_month.end_of_day
      end

      let(:expected_trailing_range) do
        d = Date.new(2026, 3, 15).in_time_zone('UTC')
        (d - 30.days).beginning_of_day..d.end_of_day
      end

      it 'normalizes to the UTC calendar date before building windows and paths' do
        expect(Reporting::IdentityVerificationReport).to receive(:new).
          with(time_range: expected_monthly_range).and_return(mock_monthly_report)
        expect(Reporting::IdentityVerificationReport).to receive(:new).
          with(time_range: expected_trailing_range).and_return(mock_trailing_report)

        expect(report).to receive(:upload_file_to_s3_bucket).with(
          hash_including(path: a_string_including('20260315_monthly_condensed_idv.csv')),
        )
        allow(report).to receive(:upload_file_to_s3_bucket)

        report.perform(report_date)
      end
    end

    context 'when both redshift and s3 are enabled' do
      it 'builds the monthly and trailing 30 day reports' do
        expect(Reporting::IdentityVerificationReport).to receive(:new).
          with(time_range: monthly_range).and_return(mock_monthly_report)
        expect(Reporting::IdentityVerificationReport).to receive(:new).
          with(time_range: trailing_range).and_return(mock_trailing_report)

        report.perform(report_date)
      end

      it 'uploads both the condensed idv and proofing rate reports' do
        expect(report).to receive(:upload_file_to_s3_bucket).twice

        report.perform(report_date)
      end

      it 'uploads the condensed idv report with the expected path and content type' do
        expect(report).to receive(:upload_file_to_s3_bucket).with(
          hash_including(
            path: 's3-base-path/MonthlyKeyMetricsIdvS3Report/2026/03/' \
              '20260315_monthly_condensed_idv.csv',
            content_type: 'text/csv',
            bucket: 'test-bucket',
          ),
        )
        allow(report).to receive(:upload_file_to_s3_bucket) # allow the proofing rate upload

        report.perform(report_date)
      end

      it 'logs start and completion messages' do
        allow(Rails.logger).to receive(:info)

        expect(Rails.logger).to receive(:info).with(
          "#{described_class::REPORT_NAME}: finished uploading reports to S3",
        )

        report.perform(report_date)
      end

      context 'when the bucket name is blank' do
        before { allow(report).to receive(:bucket_name).and_return(nil) }

        it 'logs a warning and skips upload' do
          expect(Rails.logger).to receive(:warn).with(
            "#{described_class::REPORT_NAME}: bucket_name is blank, skipping upload",
          ).twice
          expect(report).not_to receive(:upload_file_to_s3_bucket)

          report.perform(report_date)
        end
      end
    end
  end

  describe '#condensed_idv_table' do
    it 'builds the expected condensed table' do
      expect(report.send(:condensed_idv_table)).to eq(
        [
          ['Metric', 'Mar 2026'],
          ['IDV started', 100],
          ['# of successfully verified users', 70],
          ['% IDV started to successfully verified', 0.7],
          ['# of workflow completed', 80],
          ['% rate of workflow completed', 0.8],
          ['# of users verified (total)', 1234],
        ],
      )
    end
  end

  describe '#proofing_rate_table' do
    it 'builds the expected proofing rate table' do
      table = report.send(:proofing_rate_table)

      expect(table.first).to eq(['Metric', 'Trailing 30d'])
      expect(table).to include(['IDV Started', 200])
      expect(table).to include(['Welcome Submitted', 180])
      expect(table).to include(['Successfully Verified', 140])
      expect(table).to include(['IDV Rejected (Non-Fraud)', 20])
      expect(table).to include(['IDV Rejected (Fraud)', 5])
    end
  end

  describe '#csv_file' do
    let(:test_array) do
      [
        ['Header 1', 'Header 2'],
        ['Value 1', 'Value 2'],
      ]
    end

    it 'converts an array to CSV format' do
      expect(CSV.parse(report.send(:csv_file, test_array))).to eq(test_array)
    end
  end

  describe '#paths_for' do
    it 'builds a year/month partitioned path with a date prefix' do
      expect(report.send(:paths_for, 'condensed_idv')).to eq(
        ['s3-base-path/MonthlyKeyMetricsIdvS3Report/2026/03/20260315_monthly_condensed_idv.csv'],
      )
    end
  end
end
