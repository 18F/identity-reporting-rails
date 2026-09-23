# frozen_string_literal: true

require 'rails_helper'

RSpec.describe Reports::VerificationFunnelReport do
  subject(:report) { described_class.new }

  let(:frozen_date) { Time.zone.parse('2026-05-20 06:00:00') }
  around do |example|
    travel_to frozen_date do
      example.run
    end
  end

  let(:issuer) { 'urn:gov:gsa:openidconnect.profiles:sp:sso:irs:test' }
  let(:mock_report_configs) { [{ 'issuer_string' => issuer }] }

  let(:sample_reports) do
    [
      { title: 'Definitions', table: [['Metric', 'Definition']], filename: 'definitions' },
      { title: 'Overview', table: [['Report Timeframe', 'x']], filename: 'overview' },
      {
        title: 'Verification Funnel Metrics',
        table: [['Metric', 'Count', 'Rate']],
        filename: 'verification_funnel_metrics',
      },
    ]
  end

  let(:mock_funnel_report) { instance_double(Reporting::VerificationFunnelReport) }

  before do
    allow(IdentityConfig.store).to receive(:verification_funnel_s3_report_configs).and_return(
      mock_report_configs,
    )
    allow(report).to receive(:bucket_name).and_return('test-bucket')
    allow(report).to receive(:upload_file_to_s3_bucket)
    allow(report).to receive(:get_sp_id_for_issuer).with(issuer).and_return(123)
    allow(Reporting::VerificationFunnelReport).to receive(:new).and_return(mock_funnel_report)
    allow(mock_funnel_report).to receive(:as_reports).and_return(sample_reports)
  end

  describe '#initialize' do
    it 'defaults to monthly with the default look-back' do
      expect(report.time_frame).to eq('monthly')
      expect(report.days_back_for_time_period).to eq(described_class::DEFAULT_LOOK_BACK_DAYS)
    end

    it 'rejects unsupported time frames' do
      expect { described_class.new(Time.zone.now, 2, 'yearly') }.to raise_error(
        ArgumentError, /yearly time frame not supported/
      )
    end

    it 'rejects days_back out of range' do
      expect { described_class.new(Time.zone.now, 95, 'monthly') }.to raise_error(
        ArgumentError, /days_back_for_time_period must be between 0 and 90/
      )
    end
  end

  describe '#perform' do
    it 'raises when there are no configured issuers' do
      allow(IdentityConfig.store).to receive(:verification_funnel_s3_report_configs).and_return([])
      expect { report.perform }.to raise_error(ArgumentError, /No issuer configurations/)
    end

    it 'builds the funnel report for the configured issuer over the computed range' do
      # monthly, days_back 2 from May 20 -> May 18 -> all of May 2026
      expect(Reporting::VerificationFunnelReport).to receive(:new).with(
        issuer_string: issuer,
        time_range: frozen_date.prev_day(2).all_month,
      ).and_return(mock_funnel_report)

      report.perform
    end

    it 'continues past a failing issuer and logs it' do
      configs = [{ 'issuer_string' => 'bad' }, { 'issuer_string' => issuer }]
      allow(IdentityConfig.store).to receive(:verification_funnel_s3_report_configs).and_return(
        configs,
      )
      allow(report).to receive(:get_sp_id_for_issuer).with('bad').and_return(nil)

      expect(Rails.logger).to receive(:error).with(/Failed to generate.*bad/)
      expect(Rails.logger).to receive(:warn).with(/completed with 1 failures/)
      expect { report.perform }.not_to raise_error
    end
  end

  describe 'S3 layout' do
    # 3 reports x (specific + latest_internal [+ latest_external])
    it 'writes specific + latest_internal only for an in-progress (internal) period' do
      # May 2026 has not ended as of May 20 -> internal
      expect(report).to receive(:upload_file_to_s3_bucket).exactly(6).times
      report.perform
    end

    it 'also writes latest_external once the period has fully ended' do
      # Target April 2026 (ended) via an April run_date -> external
      external = described_class.new(Time.zone.parse('2026-04-15'), 2, 'monthly')
      allow(external).to receive(:bucket_name).and_return('test-bucket')
      allow(external).to receive(:get_sp_id_for_issuer).with(issuer).and_return(123)

      expect(external).to receive(:upload_file_to_s3_bucket).exactly(9).times
      external.perform
    end
  end

  describe 'period label + folder in the S3 path' do
    def path_for(run_date, time_frame, filename: 'verification_funnel_metrics')
      captured = nil
      job = described_class.new(run_date, 2, time_frame)
      allow(job).to receive(:bucket_name).and_return('test-bucket')
      allow(job).to receive(:get_sp_id_for_issuer).with(issuer).and_return(123)
      allow(job).to receive(:upload_file_to_s3_bucket) do |args|
        captured ||= args[:path] if args[:path].include?("latest_SP123_#{filename}")
      end
      job.perform
      captured
    end

    it 'uses a daily folder + Mon-day label' do
      expect(path_for(Time.zone.parse('2026-03-04'), 'daily')).to include(
        '/VerificationFunnelReport/123/daily/Mar02/',
      )
    end

    it 'uses a weekly folder + start_end label (Sunday-Saturday)' do
      # Mar 4 2026 is a Wednesday; days_back 2 -> Mar 2 (Monday); that week is
      # Sun Mar 1 .. Sat Mar 7.
      expect(path_for(Time.zone.parse('2026-03-04'), 'weekly')).to include(
        '/VerificationFunnelReport/123/weekly/20260301_20260307/',
      )
    end

    it 'uses a monthly folder + Mon-year label' do
      expect(path_for(Time.zone.parse('2026-03-15'), 'monthly')).to include(
        '/VerificationFunnelReport/123/monthly/Mar2026/',
      )
    end

    it 'uses a quarterly folder + Q label' do
      expect(path_for(Time.zone.parse('2026-05-15'), 'quarterly')).to include(
        '/VerificationFunnelReport/123/quarterly/Q22026/',
      )
    end
  end
end
