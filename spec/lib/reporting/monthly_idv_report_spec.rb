require 'rails_helper'
require 'reporting/monthly_idv_report'

RSpec.describe Reporting::MonthlyIdvReport do
  let(:end_date) { Time.zone.local(2026, 5, 15, 12, 0, 0) }
  subject(:report) { described_class.new(end_date: end_date) }

  let(:subreport) do
    instance_double(
      Reporting::IdentityVerificationReport,
      time_range: end_date.all_month,
      idv_started: 10,
      successfully_verified_users: 5,
      blanket_proofing_rate: 0.5,
      idv_final_resolution: 6,
      idv_final_resolution_rate: 0.6,
      verified_user_count: 100,
    )
  end

  before do
    allow(report).to receive(:monthly_subreports).and_return([subreport])
  end

  describe '#as_csv' do
    it 'renders the condensed monthly IDV metrics table' do
      table = report.as_csv

      aggregate_failures do
        expect(table[0]).to eq(['Metric', subreport.time_range.begin.strftime('%b %Y')])
        expect(table[1]).to eq(['IDV started', 10])
        expect(table[2]).to eq(['# of successfully verified users', 5])
        expect(table[6]).to eq(['# of users verified (total)', 100])
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
            subtitle: 'Condensed (NEW)',
            float_as_percent: true,
            precision: 2,
            table: report.as_csv,
            filename: 'condensed_idv',
          },
        ],
      )
    end
  end
end
