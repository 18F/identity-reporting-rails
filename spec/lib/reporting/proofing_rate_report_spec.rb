require 'rails_helper'
require 'reporting/proofing_rate_report'

RSpec.describe Reporting::ProofingRateReport do
  let(:end_date) { Time.zone.parse('2026-05-31 23:59:59 UTC') }
  subject(:report) { described_class.new(end_date: end_date) }

  let(:subreport) do
    instance_double(
      Reporting::IdentityVerificationReport,
      time_range: (end_date - 30.days).beginning_of_day..end_date.end_of_day,
      idv_started: 10,
      idv_doc_auth_welcome_submitted: 8,
      idv_doc_auth_image_vendor_submitted: 7,
      idv_doc_auth_socure_verification_data_requested: 1,
      successfully_verified_users: 5,
      idv_doc_auth_rejected: 2,
      idv_fraud_rejected: 1,
      blanket_proofing_rate: 0.5,
      intent_proofing_rate: 0.625,
      actual_proofing_rate: 0.625,
      industry_proofing_rate: 0.625,
    )
  end

  before do
    allow(report).to receive(:trailing_days_subreports).and_return([subreport])
  end

  describe '#as_csv' do
    it 'renders the detailed proofing metrics table' do
      table = report.as_csv
      blanket_row = table.find do |row|
        row.first == 'Blanket Proofing Rate (IDV Started to Successfully Verified)'
      end

      aggregate_failures do
        expect(table[0]).to eq(['Metric', 'Trailing 30d'])
        expect(table[3]).to eq(['IDV Started', 10])
        expect(table[7]).to eq(['Successfully Verified', 5])
        expect(blanket_row).to eq(
          ['Blanket Proofing Rate (IDV Started to Successfully Verified)', 0.5],
        )
      end
    end
  end

  describe '#as_reports' do
    it 'returns report metadata with CSV table payload' do
      reports = report.as_reports

      expect(reports).to eq(
        [
          {
            title: 'Proofing Rate Metrics',
            subtitle: 'Detail',
            float_as_percent: true,
            precision: 2,
            table: report.as_csv,
            filename: 'proofing_rate_metrics',
          },
        ],
      )
    end
  end
end
