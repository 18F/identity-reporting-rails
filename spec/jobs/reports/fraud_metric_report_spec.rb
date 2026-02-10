require 'rails_helper'

RSpec.describe Reports::FraudMetricsReport do
  let(:report_date) { Date.new(2021, 3, 2).in_time_zone('UTC').end_of_day }
  let(:time_range) { report_date.all_month }
  subject(:report) { Reports::FraudMetricsReport.new(report_date) }

  let(:name) { 'fraud-metrics-report' }
  let(:s3_report_bucket_prefix) { 'reports-bucket' }
  let(:report_folder) do
    'int/idp/fraud-metrics-report/2021/2021-03-02.fraud-metrics-report'
  end

  let(:expected_s3_paths) do
    [
      "#{report_folder}/lg99_metrics.csv",
      "#{report_folder}/suspended_metrics.csv",
      "#{report_folder}/reinstated_metrics.csv",
    ]
  end

  let(:s3_metadata) do
    {
      body: anything,
      content_type: 'text/csv',
      bucket: 'reports-bucket-1234-us-west-1',
    }
  end

  let(:mock_identity_verification_lg99_data) do
    [
      ['Metric', 'Total', 'Range Start', 'Range End'],
      ['Unique users seeing LG-99', 5, time_range.begin.to_s,
       time_range.end.to_s],
    ]
  end
  let(:mock_suspended_metrics_table) do
    [
      ['Metric', 'Total', 'Range Start', 'Range End'],
      ['Unique users suspended', 2, time_range.begin.to_s,
       time_range.end.to_s],
      ['Average Days Creation to Suspension', 1.5, time_range.begin.to_s,
       time_range.end.to_s],
      ['Average Days Proofed to Suspension', 2.0, time_range.begin.to_s,
       time_range.end.to_s],
    ]
  end
  let(:mock_reinstated_metrics_table) do
    [
      ['Metric', 'Total', 'Range Start', 'Range End'],
      ['Unique users reinstated', 1, time_range.begin.to_s,
       time_range.end.to_s],
      ['Average Days to Reinstatement', 3.0, time_range.begin.to_s,
       time_range.end.to_s],
    ]
  end

  before do
    allow(Identity::Hostdata).to receive(:env).and_return('int')
    allow(Identity::Hostdata).to receive(:aws_account_id).and_return('1234')
    allow(Identity::Hostdata).to receive(:aws_region).and_return('us-west-1')
    allow(IdentityConfig.store).to receive(:s3_report_bucket_prefix).
      and_return(s3_report_bucket_prefix)

    Aws.config[:s3] = {
      stub_responses: {
        put_object: {},
      },
    }

    allow(report.fraud_metrics_lg99_report).to receive(:lg99_metrics_table).
      and_return(mock_identity_verification_lg99_data)

    allow(report.fraud_metrics_lg99_report).to receive(:suspended_metrics_table).
      and_return(mock_suspended_metrics_table)

    allow(report.fraud_metrics_lg99_report).to receive(:reinstated_metrics_table).
      and_return(mock_reinstated_metrics_table)
  end

  it 'uploads a file to S3 based on the report date' do
    expected_s3_paths.each do |path|
      expect(subject).to receive(:upload_file_to_s3_bucket).with(
        path: path,
        **s3_metadata,
      ).exactly(1).time.and_call_original
    end

    report.perform(report_date)
  end
end
