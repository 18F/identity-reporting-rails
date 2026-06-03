# frozen_string_literal: true

require 'rails_helper'

RSpec.describe Reports::DemographicsMetricsReport do
  subject(:report) { described_class.new }

  # Freeze time to ensure consistent behavior
  let(:frozen_date) { Time.zone.parse('2026-05-03 6:00:00') }
  around do |example|
    travel_to frozen_date do
      example.run
    end
  end

  let(:report_date) { Time.zone.parse('2026-05-03 6:00:00') }
  let(:days_back) { 4 }
  let(:time_frame) { 'quarterly' }
  let(:time_range) { report_date.prev_day(days_back).all_quarter }
  let(:mock_report_configs) do
    [
      { 'issuer_string' => 'urn:gov:gsa:openidconnect.profiles:sp:sso:ssa:sample_app' },
      { 'issuer_string' => 'urn:gov:gsa:openidconnect.profiles:sp:sso:va:sample_app' },
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
        title: 'Age Metrics',
        table: [['Age Range', 'User Count'], ['20-29', '5'], ['30-39', '20']],
        filename: 'age_metrics',
      },
      {
        title: 'State Metrics',
        table: [['State', 'User Count'], ['CA', '3'], ['FL', '5']],
        filename: 'state_metrics',
      },

    ]
  end

  before do
    allow(IdentityConfig.store).to receive(:redshift_sia_v3_enabled).and_return(true)
    allow(IdentityConfig.store).to receive(:s3_reports_enabled).and_return(true)
    allow(IdentityConfig.store).to receive(:demographics_metrics_s3_report_configs).and_return(
      mock_report_configs,
    )
    allow(report).to receive(:bucket_name).and_return('test-bucket')
    allow(report).to receive(:upload_file_to_s3_bucket)
    allow(report).to receive(:get_sp_id_for_issuer).with(
      'urn:gov:gsa:openidconnect.profiles:sp:sso:ssa:sample_app',
    ).and_return(123)
    allow(report).to receive(:get_sp_id_for_issuer).with(
      'urn:gov:gsa:openidconnect.profiles:sp:sso:va:sample_app',
    ).and_return(456)
  end

  describe '#initialize' do
    it 'sets default values when no parameters provided' do
      report = described_class.new
      expect(report.run_date).to be_within(1.second).of(Time.zone.now)
      expect(report.days_back_for_time_period).to eq(described_class::DEFAULT_LOOK_BACK_DAYS)
      expect(report.time_frame).to eq('quarterly')
    end

    it 'accepts custom initialization parameters' do
      custom_date = Time.zone.parse('2026-01-15')
      report = described_class.new(custom_date, 10, 'quarterly')
      expect(report.run_date).to eq(custom_date)
      expect(report.days_back_for_time_period).to eq(10)
      expect(report.time_frame).to eq('quarterly')
    end

    it 'raises error for invalid time frame' do
      expect { described_class.new(Time.zone.now, 4, 'monthly') }.to raise_error(
        ArgumentError, "monthly time frame not yet implemented - must be 'quarterly'"
      )
    end

    it 'raises error for days_back out of range' do
      expect { described_class.new(Time.zone.now, 95, 'quarterly') }.to raise_error(
        ArgumentError, /days_back_for_time_period must be between 0 and 90/
      )
    end

    it 'logs warning for dates before schema cutoff' do
      old_date = Time.zone.parse('2025-09-15')
      expect(Rails.logger).to receive(:warn).with(/Running demographics report for 2025-09-15/)
      described_class.new(old_date, 4, 'quarterly')
    end
  end

  describe '#perform' do
    context 'when redshift_sia_v3 is disabled' do
      before do
        allow(IdentityConfig.store).to receive(:redshift_sia_v3_enabled).and_return(false)
      end

      it 'logs warning and returns false' do
        expect(Rails.logger).to receive(:warn).with('Redshift SIA V3 is disabled')
        expect(report.perform).to eq(false)
      end
    end

    context 'when s3_reports is disabled' do
      before do
        allow(IdentityConfig.store).to receive(:s3_reports_enabled).and_return(false)
      end

      it 'returns early without processing' do
        expect(report).not_to receive(:generate_and_upload_report_for_issuer)
        report.perform
      end
    end

    context 'when configs are empty' do
      before do
        allow(IdentityConfig.store).to receive(
          :demographics_metrics_s3_report_configs,
        ).and_return([])
      end

      it 'logs error and raises ArgumentError' do
        expect(Rails.logger).to receive(:error).with(
          'demographics_metrics_s3_report_configs is empty or nil - no work to do',
        )

        expect { report.perform }.to raise_error(
          ArgumentError, /No issuer configurations found/
        )
      end
    end

    context 'when both redshift and s3 are enabled' do
      let(:mock_demographics_report) { instance_double(Reporting::DemographicsMetricsReport) }

      before do
        allow(Reporting::DemographicsMetricsReport).to receive(:new).and_return(
          mock_demographics_report,
        )
        allow(mock_demographics_report).to receive(:as_reports).and_return(sample_reports)
      end

      it 'processes all configured issuers' do
        expect(Reporting::DemographicsMetricsReport).to receive(:new).with(
          issuer_string: 'urn:gov:gsa:openidconnect.profiles:sp:sso:ssa:sample_app',
          time_range: time_range,
        ).and_return(mock_demographics_report)

        expect(Reporting::DemographicsMetricsReport).to receive(:new).with(
          issuer_string: 'urn:gov:gsa:openidconnect.profiles:sp:sso:va:sample_app',
          time_range: time_range,
        ).and_return(mock_demographics_report)

        report.perform(report_date, days_back, time_frame)
      end

      it 'uploads reports to S3 with correct file naming' do
        # For external reports (quarter ended + lag passed)

        external_report = described_class.new(frozen_date.to_date - 4.months, 4, 'quarterly')
        allow(external_report).to receive(:bucket_name).and_return('test-bucket')
        allow(external_report).to receive(:get_sp_id_for_issuer).with(
          'urn:gov:gsa:openidconnect.profiles:sp:sso:ssa:sample_app',
        ).and_return(123)
        allow(external_report).to receive(:get_sp_id_for_issuer).with(
          'urn:gov:gsa:openidconnect.profiles:sp:sso:va:sample_app',
        ).and_return(456)
        allow(Reporting::DemographicsMetricsReport).to receive(:new).and_return(
          mock_demographics_report,
        )
        allow(mock_demographics_report).to receive(:as_reports).and_return(sample_reports)

        # 2 issuers  4 reports  3 files each (specific, latest_internal, latest_external)
        # = 24 files total
        expect(external_report).to receive(:upload_file_to_s3_bucket).exactly(24).times
        external_report.perform
      end

      it 'uploads only internal files when quarter not ended' do
        # For internal reports (quarter not ended)
        internal_report = described_class.new(
          frozen_date.to_date.beginning_of_month + 1.day, 2,
          'quarterly'
        )
        allow(internal_report).to receive(:bucket_name).and_return('test-bucket')
        allow(internal_report).to receive(:get_sp_id_for_issuer).with(
          'urn:gov:gsa:openidconnect.profiles:sp:sso:ssa:sample_app',
        ).and_return(123)
        allow(internal_report).to receive(:get_sp_id_for_issuer).with(
          'urn:gov:gsa:openidconnect.profiles:sp:sso:va:sample_app',
        ).and_return(456)
        allow(Reporting::DemographicsMetricsReport).to receive(:new).and_return(
          mock_demographics_report,
        )
        allow(mock_demographics_report).to receive(:as_reports).and_return(sample_reports)

        # 2 issuers  4 reports  2 files each (specific, latest_internal) = 16 files total
        expect(internal_report).to receive(:upload_file_to_s3_bucket).exactly(16).times
        internal_report.perform
      end

      it 'logs start and completion messages' do
        allow(Rails.logger).to receive(:info)

        # Internal because current date is May 20th, which is in the middle of Q2
        expect(Rails.logger).to receive(:info).with(
          /Starting internal-facing quarterly demographics/,
        )
        expect(Rails.logger).to receive(:info).with(
          'Generating demographics report for issuer: '\
          'urn:gov:gsa:openidconnect.profiles:sp:sso:ssa:sample_app',
        )
        expect(Rails.logger).to receive(:info).with(
          'Completed demographics report for issuer: '\
          'urn:gov:gsa:openidconnect.profiles:sp:sso:ssa:sample_app',
        )
        expect(Rails.logger).to receive(:info).with(
          'Completed demographics metrics report generation for all issuers successfully',
        )

        report.perform(report_date, days_back, time_frame)
      end

      it 'accepts perform parameters that override initialization' do
        new_date = Time.zone.parse('2026-05-15')
        new_days = 7

        expect(Reporting::DemographicsMetricsReport).to receive(:new).with(
          issuer_string: anything,
          time_range: new_date.prev_day(new_days).all_quarter,
        ).twice.and_return(mock_demographics_report)

        report.perform(new_date, new_days, 'quarterly')
      end
    end

    context 'when an error occurs during report generation' do
      let(:error_message) { 'Database connection failed' }
      let(:mock_demographics_report) { instance_double(Reporting::DemographicsMetricsReport) }
      before do
        # First issuer fails, second succeeds
        allow(Reporting::DemographicsMetricsReport).to receive(:new).with(
          issuer_string: 'urn:gov:gsa:openidconnect.profiles:sp:sso:ssa:sample_app',
          time_range: anything,
        ).and_raise(StandardError.new(error_message))
        allow(Reporting::DemographicsMetricsReport).to receive(:new).with(
          issuer_string: 'urn:gov:gsa:openidconnect.profiles:sp:sso:va:sample_app',
          time_range: anything,
        ).and_return(mock_demographics_report)
        allow(mock_demographics_report).to receive(:as_reports).and_return(sample_reports)
      end

      it 'logs error and continues processing other issuers' do
        expect(Rails.logger).to receive(:error).with(
          /Failed to generate demographics report for issuer.*ssa:sample_app/,
        )
        expect(Rails.logger).to receive(:warn).with(
          /Demographics report generation completed with 1 failures/,
        )
        # Should not raise error
        expect { report.perform(report_date) }.not_to raise_error
      end
    end

    context 'when no SP ID found for issuer' do
      before do
        allow(report).to receive(:get_sp_id_for_issuer).with(
          'urn:gov:gsa:openidconnect.profiles:sp:sso:ssa:sample_app',
        ).and_return(nil)
      end

      it 'raises StandardError and logs failure' do
        expect(Rails.logger).to receive(:error).with(
          /Failed to generate demographics report for issuer.*No service provider ID found/,
        )

        expect(Rails.logger).to receive(:warn).with(
          /Demographics report generation completed with 1 failures/,
        )
        report.perform(report_date)
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
      csv_output = report.send(:csv_file, test_array)
      parsed_csv = CSV.parse(csv_output)
      expect(parsed_csv).to eq(test_array)
    end
  end

  describe 'private methods' do
    describe '#external_report?' do
      it 'returns true when quarter ended and lag passed' do
        # Q1 2026 ended March 31, report date April 3 to trigger Q1 report
        # (current date is May 20th)
        external_report = described_class.new(Time.zone.parse('2026-04-03'), 4, 'quarterly')
        expect(external_report.send(:external_report?)).to be true
      end

      it 'returns false when within lag period' do
        # Q2 2026 ends June 30, current date May 20 (still in Q2)
        internal_report = described_class.new(frozen_date.to_date, 0, 'quarterly')
        expect(internal_report.send(:external_report?)).to be false
      end

      it 'returns false when exactly at lag boundary' do
        quarter_end_date = Time.zone.parse('2026-03-31')
        boundary_report_date = quarter_end_date + described_class::DATA_LAG_DAYS.days
        # Calculate days to travel back from frozen date to boundary date
        days_to_travel_back = (frozen_date.to_date - boundary_report_date.to_date).to_i

        travel(-days_to_travel_back.days)

        boundary_report = described_class.new(
          boundary_report_date,
          described_class::DATA_LAG_DAYS, 'quarterly'
        )
        expect(boundary_report.send(:external_report?)).to be false

        travel_back
      end
    end

    describe '#report_time_range_label' do
      it 'formats quarterly label correctly' do
        q1_report = described_class.new(Time.zone.parse('2026-04-03'), 4, 'quarterly')
        expect(q1_report.send(:report_time_range_label)).to eq('Q12026')
        q4_report = described_class.new(Time.zone.parse('2026-01-03'), 4, 'quarterly')
        expect(q4_report.send(:report_time_range_label)).to eq('Q42025')
      end
    end

    describe '#upload_to_s3' do
      let(:test_report) { described_class.new }
      let(:sp_id) { 123 }
      let(:report_body) { [['header'], ['data']] }
      before do
        allow(test_report).to receive(:bucket_name).and_return('test-bucket')
        allow(test_report).to receive(:generate_base_s3_path).with(
          directory: 'idp',
        ).and_return('base/path/')
      end

      it 'uploads correct files for external report' do
        formatted_current_date = frozen_date.strftime('%Y%m%d')
        allow(test_report).to receive(:external_report?).and_return(true)
        expect(test_report).to receive(:upload_file_to_s3_bucket).with(
          hash_including(
            path: /SP123_#{formatted_current_date}_external_definitions\.csv$/,
            content_type: 'text/csv',
            bucket: 'test-bucket',
          ),
        )

        expect(test_report).to receive(:upload_file_to_s3_bucket).with(
          hash_including(
            path: /latest_SP123_definitions\.csv$/,
            content_type: 'text/csv',
            bucket: 'test-bucket',
          ),
        )

        expect(test_report).to receive(:upload_file_to_s3_bucket).with(
          hash_including(
            path: /latest_external_SP123_definitions\.csv$/,
            content_type: 'text/csv',
            bucket: 'test-bucket',
          ),
        )

        test_report.send(:upload_to_s3, report_body, sp_id: sp_id, filename: 'definitions')
      end

      it 'uploads correct files for internal report' do
        formatted_current_date = frozen_date.to_date.strftime('%Y%m%d')
        allow(test_report).to receive(:external_report?).and_return(false)

        expect(test_report).to receive(:upload_file_to_s3_bucket).with(
          hash_including(
            path: /SP123_#{formatted_current_date}_internal_definitions\.csv$/,
            content_type: 'text/csv',
            bucket: 'test-bucket',
          ),
        )

        expect(test_report).to receive(:upload_file_to_s3_bucket).with(
          hash_including(
            path: /latest_SP123_definitions\.csv$/,
            content_type: 'text/csv',
            bucket: 'test-bucket',
          ),
        )

        expect(test_report).not_to receive(:upload_file_to_s3_bucket).with(
          hash_including(
            path: /latest_external_SP123_definitions\.csv$/,
          ),
        )

        test_report.send(:upload_to_s3, report_body, sp_id: sp_id, filename: 'definitions')
      end
    end
  end
end
