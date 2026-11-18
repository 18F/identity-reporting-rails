# frozen_string_literal: true

require 'reporting/partner_report_period'

RSpec.describe Reporting::PartnerReportPeriod do
  it 'returns inclusive ends for a leap month, a cross-year week, and a day' do
    expect(described_class.end_date('2024-02-01', 'monthly').to_s).to eq('2024-02-29')
    expect(described_class.end_date('2025-12-29', 'weekly').to_s).to eq('2026-01-04')
    expect(described_class.end_date('2026-03-15', 'daily').to_s).to eq('2026-03-15')
  end

  it 'advances from the period end rather than adding a month to an arbitrary date' do
    expect(described_class.next_start('2026-01-01', 'monthly').to_s).to eq('2026-02-01')
  end

  it 'rejects unsupported cadences' do
    expect { described_class.end_date('2026-01-01', 'yearly') }.to raise_error(ArgumentError)
  end
end
