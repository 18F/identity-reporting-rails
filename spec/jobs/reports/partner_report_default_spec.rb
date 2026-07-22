# frozen_string_literal: true

require 'rails_helper'
require 'reporting/partner_report_default_v2'

RSpec.describe Reports::PartnerReportDefault do
  # v1 is being deprecated; these tests focus on the v2 default.
  subject(:job) { described_class.new }

  let(:report_date) { Time.zone.parse('2026-04-15').end_of_day }
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
          service_provider_id: 123,
        },
        report_information: {
          period_start_date: '2026-04-01',
          period_calendar_id: 20260401,
          report_cadence: 'monthly',
        },
        data: { count_active_users: 1000, count_authentications: 500 },
      },
      issuer2 => {
        issuer: issuer2,
        provider_information: {
          service_provider_name: 'Agency 2 App',
          agency_name: 'Test Agency 2',
          service_provider_id: 456,
        },
        report_information: {
          period_start_date: '2026-04-01',
          period_calendar_id: 20260401,
          report_cadence: 'monthly',
        },
        data: { count_active_users: 500, count_authentications: 250 },
      },
    }
  end

  # Data with some nil issuers (failed data integrity)
  let(:sample_report_data_with_missing_sp) do
    {
      issuer1 => {
        issuer: issuer1,
        provider_information: { service_provider_name: 'Agency 1 App', service_provider_id: 123 },
        data: { count_active_users: 1000 },
      },
      issuer2 => nil, # This issuer had duplicate/integrity issues
      issuer3 => nil, # This issuer also had issues
    }
  end

  let(:mock_partner_report) { instance_double(Reporting::PartnerReportDefaultV2) }
  let(:sample_issuer_mapping) do
    {
      issuer1 => { id: 123 },
      issuer2 => { id: 456 },
      issuer3 => { id: 789 },
    }
  end

  before do
    allow(IdentityConfig.store).to receive(:redshift_sia_v3_enabled).and_return(true)
    allow(IdentityConfig.store).to receive(:s3_reports_enabled).and_return(true)
    allow_any_instance_of(described_class).to receive(:bucket_name).and_return(bucket_name)
    allow_any_instance_of(described_class).to receive(:upload_file_to_s3_bucket)
    allow(job).to receive(:generate_base_s3_path).with(directory: 'portal').and_return('')
    allow(Reporting::PartnerReportDefaultV2).to receive(:get_period_date_from_report_date).
      with(report_date: anything, cadence: 'monthly').
      and_return(period_date)
    allow(Reporting::PartnerReportDefaultV2).to receive(:new).and_return(mock_partner_report)
    allow(mock_partner_report).to receive(:generate_reports).and_return(sample_report_data)
    allow(mock_partner_report).to receive(
      :generate_issuer_mapping,
    ).and_return(sample_issuer_mapping)
    allow_any_instance_of(described_class).to receive(:sleep)
  end

  describe '#initialize' do
    it 'sets report_date when provided' do
      job_with_date = described_class.new(report_date)
      expect(job_with_date.report_date).to eq(report_date)
    end

    it 'allows report_date to be nil' do
      expect(job.report_date).to be_nil
    end

    context 'with report_version' do
      it 'defaults to v2' do
        expect(described_class::DEFAULT_VERSION).to eq('v2')
      end

      it 'accepts v1' do
        expect { described_class.new(nil, report_version: 'v1') }.not_to raise_error
      end

      it 'accepts v2' do
        expect { described_class.new(nil, report_version: 'v2') }.not_to raise_error
      end
    end

    context 'with issuer filtering' do
      it 'accepts included_issuers as a single string' do
        job = described_class.new(nil, included_issuers: issuer1)
        expect(job.included_issuers).to eq([issuer1])
      end

      it 'accepts included_issuers as an array' do
        job = described_class.new(nil, included_issuers: [issuer1, issuer2])
        expect(job.included_issuers).to eq([issuer1, issuer2])
      end

      it 'accepts excluded_issuers as a single string' do
        job = described_class.new(nil, excluded_issuers: issuer3)
        expect(job.excluded_issuers).to eq([issuer3])
      end

      it 'accepts excluded_issuers as an array' do
        job = described_class.new(nil, excluded_issuers: [issuer2, issuer3])
        expect(job.excluded_issuers).to eq([issuer2, issuer3])
      end

      it 'raises error when both included and excluded issuers are provided' do
        expect do
          described_class.new(nil, included_issuers: [issuer1], excluded_issuers: [issuer2])
        end.to raise_error(
          ArgumentError, 'Cannot specify both included_issuers and excluded_issuers'
        )
      end

      it 'normalizes empty arrays to nil' do
        job = described_class.new(nil, included_issuers: [], excluded_issuers: [])
        expect(job.included_issuers).to be_nil
        expect(job.excluded_issuers).to be_nil
      end

      it 'removes nil and empty string values from arrays' do
        job = described_class.new(nil, included_issuers: [issuer1, nil, '', issuer2])
        expect(job.included_issuers).to eq([issuer1, issuer2])
      end

      it 'raises error for non-string/array issuer values' do
        expect do
          described_class.new(nil, included_issuers: { invalid: 'hash' })
        end.to raise_error(ArgumentError, /Issuers must be a string or array of strings, got Hash/)
      end
    end
  end

  describe '#perform' do
    context 'when redshift_sia_v3 is disabled' do
      before do
        allow(IdentityConfig.store).to receive(:redshift_sia_v3_enabled).and_return(false)
      end

      it 'logs warning and returns false' do
        expect(Rails.logger).to receive(:warn).with('Redshift SIA V3 is disabled')
        # Don't expect any error logs since this should return early
        expect(Rails.logger).not_to receive(:error)
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

    context 'with an invalid report_version' do
      subject(:job) { described_class.new(nil, report_version: 'v3') }

      it 'logs error and returns false' do
        expect(Rails.logger).to receive(:error).with(
          'Failed to generate partner reports: Invalid report_version: v3. '\
          'Must be one of: v1, v2',
        )
        expect(Rails.logger).to receive(:error).with(
          a_string_starting_with('Backtrace:'),
        )
        expect(job.perform(report_date)).to eq(false)
      end
    end

    context 'when period_date cannot be retrieved' do
      before do
        allow(Reporting::PartnerReportDefaultV2).to receive(:get_period_date_from_report_date).
          with(report_date: report_date, cadence: 'monthly').
          and_raise(StandardError, 'No calendar entry found')
      end

      it 'logs error and backtrace, then returns false' do
        expect(Rails.logger).to receive(:error).with(
          'Failed to generate partner reports: No calendar entry found',
        )
        # Also expect backtrace logging
        expect(Rails.logger).to receive(:error).with(
          a_string_starting_with('Backtrace:'),
        )
        expect(job.perform(report_date)).to eq(false)
      end
    end

    context 'when both redshift and s3 are enabled' do
      before do
        allow(Identity::Hostdata).to receive(:bucket_name).and_return(bucket_name)
        mock_s3_client = Aws::S3::Client.new(stub_responses: true)
        mock_s3_helper = instance_double(JobHelpers::S3Helper, s3_client: mock_s3_client)
        allow(JobHelpers::S3Helper).to receive(:new).and_return(mock_s3_helper)
        allow(job).to receive(:upload_file_to_s3_bucket).and_return(true)
      end
      it 'sets report_date from parameter' do
        job.perform(report_date)
        expect(job.report_date).to eq(report_date)
      end

      it 'uses default date when none provided and no constructor date set' do
        freeze_time = Time.zone.parse('2026-02-05')
        expected_default = freeze_time - described_class::REPORT_DELAY_DAYS.days
        travel_to freeze_time do
          job.perform # No date provided, no constructor date
          expect(job.report_date).to be_within(1.second).of(expected_default.end_of_day)
        end
      end

      it 'uses constructor date when no parameter provided' do
        job_with_constructor_date = described_class.new(report_date)
        allow(job_with_constructor_date).to receive(:period_date).and_return(period_date)
        job_with_constructor_date.perform # No parameter
        expect(job_with_constructor_date.report_date).to eq(report_date)
      end
      it 'creates PartnerReportDefaultV2 with correct parameters' do
        expect(Reporting::PartnerReportDefaultV2).to receive(:new).with(
          report_date: report_date,
          report_cadence: 'monthly',
          included_issuers: nil,
          excluded_issuers: nil,
        ).and_return(mock_partner_report)

        job.perform(report_date)
      end

      it 'logs start message with report_date, report_version, and period_date' do
        allow(Rails.logger).to receive(:info)
        expect(Rails.logger).to receive(:info).with(
          "Generating partner default monthly reports "\
          "(#{described_class::DEFAULT_VERSION} version) "\
          "for report date: #{report_date} (monthly report period starting on #{period_date})",
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
          service_provider_id: 123,
          period_date: period_date,
        )
        expect(job).to receive(:upload_to_s3).with(
          sample_report_data[issuer2],
          service_provider_id: 456,
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
      it 'generates issuer mapping before reports' do
        expect(mock_partner_report).to receive(:generate_issuer_mapping).ordered
        expect(mock_partner_report).to receive(:generate_reports).ordered
        job.perform(report_date)
      end

      it 'uploads issuer mapping to S3' do
        expect(job).to receive(:upload_issuer_mapping_to_s3).with(sample_issuer_mapping)
        job.perform(report_date)
      end

      it 'validates service provider IDs against mapping' do
        expect(job).to receive(:validate_service_provider_ids).with(
          sample_report_data, sample_issuer_mapping
        )
        job.perform(report_date)
      end
    end

    context 'with report_version v1' do
      subject(:job) { described_class.new(nil, report_version: 'v1') }

      let(:mock_v1_report) { instance_double(Reporting::PartnerReportDefault) }

      before do
        allow(job).to receive(:period_date).and_return(period_date)
        allow(job).to receive(:upload_file_to_s3_bucket).and_return(true)
        allow(Reporting::PartnerReportDefault).to receive(:new).and_return(mock_v1_report)
        allow(mock_v1_report).to receive(:generate_reports).and_return(sample_report_data)
        allow(mock_v1_report).to receive(:generate_issuer_mapping).
          and_return(sample_issuer_mapping)
      end

      it 'creates PartnerReportDefault (v1) with correct parameters' do
        expect(Reporting::PartnerReportDefault).to receive(:new).with(
          report_date: report_date,
          report_cadence: 'monthly',
          included_issuers: nil,
          excluded_issuers: nil,
        ).and_return(mock_v1_report)

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

      it 'logs error and returns false' do
        expect(Rails.logger).to receive(:error).with(
          "Failed to generate partner reports: #{error_message}",
        )
        expect(Rails.logger).to receive(:error).with(
          a_string_starting_with('Backtrace:'),
        )
        expect(job.perform(report_date)).to eq(false)
      end
    end

    context 'when issuer mapping generation fails' do
      let(:error_message) { 'Failed to fetch issuer mapping' }

      before do
        allow(mock_partner_report).to receive(:generate_issuer_mapping).and_raise(
          StandardError.new(error_message),
        )
      end

      it 'logs error and returns false without generating reports' do
        expect(Rails.logger).to receive(:error).with(
          "Failed to generate partner reports: #{error_message}",
        )
        expect(Rails.logger).to receive(:error).with(
          a_string_starting_with('Backtrace:'),
        )
        expect(mock_partner_report).not_to receive(:generate_reports)
        expect(job.perform(report_date)).to eq(false)
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
        allow(Rails.logger).to receive(:warn)

        expect(Rails.logger).to receive(:info).with('Upload summary: 1 successful, 2 skipped')

        expect(Rails.logger).to receive(:warn).with(
          "Skipped uploads:\n" \
          "- #{issuer2}: report generation failed and returned nil\n" \
          "- #{issuer3}: report generation failed and returned nil",
        )

        # Should only upload issuer1
        expect(job).to receive(:upload_to_s3).once

        job.perform(report_date)
      end
    end

    context 'when service_provider_id is missing from report data' do
      let(:sample_report_data_missing_id) do
        {
          issuer1 => {
            issuer: issuer1,
            provider_information: {
              service_provider_name: 'Agency 1 App',
              # service_provider_id is missing
            },
            data: { count_active_users: 1000 },
          },
        }
      end

      before do
        allow(mock_partner_report).to receive(:generate_reports).and_return(
          sample_report_data_missing_id,
        )
      end

      it 'skips upload and logs error when service_provider_id is missing' do
        allow(Rails.logger).to receive(:info)
        expect(Rails.logger).to receive(:info).with('Upload summary: 0 successful, 1 skipped')
        expect(Rails.logger).to receive(:warn).with(
          "Skipped uploads:\n" \
          "- #{issuer1}: missing service_provider_id",
        )
        expect(job).not_to receive(:upload_to_s3)
        job.perform(report_date)
      end
    end

    context 'with issuer filtering' do
      let(:job_with_included) { described_class.new(nil, included_issuers: [issuer1, issuer2]) }
      let(:job_with_excluded) { described_class.new(nil, excluded_issuers: [issuer3]) }

      before do
        allow(job_with_included).to receive(:period_date).and_return(period_date)
        allow(job_with_excluded).to receive(:period_date).and_return(period_date)
        allow(job_with_included).to receive(:upload_file_to_s3_bucket).and_return(true)
        allow(job_with_excluded).to receive(:upload_file_to_s3_bucket).and_return(true)
      end

      it 'passes included_issuers to PartnerReportDefaultV2' do
        expect(Reporting::PartnerReportDefaultV2).to receive(:new).with(
          report_date: report_date,
          report_cadence: 'monthly',
          included_issuers: [issuer1, issuer2],
          excluded_issuers: nil,
        ).and_return(mock_partner_report)

        job_with_included.perform(report_date)
      end

      it 'passes excluded_issuers to PartnerReportDefaultV2' do
        expect(Reporting::PartnerReportDefaultV2).to receive(:new).with(
          report_date: report_date,
          report_cadence: 'monthly',
          included_issuers: nil,
          excluded_issuers: [issuer3],
        ).and_return(mock_partner_report)

        job_with_excluded.perform(report_date)
      end

      it 'logs included issuers when provided' do
        allow(Rails.logger).to receive(:info)
        expect(Rails.logger).to receive(:info).with(
          "Filtering to include only issuers: #{issuer1}, #{issuer2}",
        )

        job_with_included.perform(report_date)
      end

      it 'logs excluded issuers when provided' do
        allow(Rails.logger).to receive(:info)
        expect(Rails.logger).to receive(:info).with(
          "Filtering to exclude issuers: #{issuer3}",
        )

        job_with_excluded.perform(report_date)
      end
    end
  end

  describe '#period_date' do
    context 'when report_date is nil' do
      before do
        job.instance_variable_set(:@report_date, nil)
      end
      it 'raises argument error' do
        expect { job.send(:period_date) }.to raise_error(
          ArgumentError, 'report_date must be set before calling period_date'
        )
      end
    end

    context 'when period_date can be retrieved' do
      before do
        job.instance_variable_set(:@report_date, report_date)
      end

      it 'returns the period date string from marts.calendar' do
        expect(job.send(:period_date)).to eq(period_date)
      end

      it 'memoizes the result' do
        expect(Reporting::PartnerReportDefaultV2).to receive(:get_period_date_from_report_date).
          with(hash_including(report_date: report_date, cadence: 'monthly')).
          once.
          and_return(period_date)

        2.times { job.send(:period_date) }
      end
    end

    context 'when period_date cannot be retrieved' do
      before do
        job.instance_variable_set(:@report_date, report_date)
        allow(Reporting::PartnerReportDefaultV2).to receive(:get_period_date_from_report_date).
          with(report_date: report_date, cadence: 'monthly').
          and_raise(StandardError, 'No calendar entry found')
      end

      it 'raises the error' do
        expect { job.send(:period_date) }.to raise_error(StandardError, 'No calendar entry found')
      end
    end
  end

  describe '#upload_to_s3' do
    let(:sample_json_data) do
      {
        issuer: issuer1,
        provider_information: {
          service_provider_name: 'Test App',
          service_provider_id: 123,
        },
        data: { count_active_users: 1000 },
      }
    end

    it 'uploads to correct S3 path structure including report version' do
      expected_path = 'v2/123/monthly/2026-04-01.json'
      expect(job).to receive(:upload_file_to_s3_bucket).with(
        path: expected_path,
        body: JSON.pretty_generate(sample_json_data),
        content_type: 'application/json',
        bucket: bucket_name,
      )
      job.send(:upload_to_s3, sample_json_data, service_provider_id: 123, period_date: period_date)
    end

    it 'logs successful upload' do
      expect(Rails.logger).to receive(:info).with(
        'Uploaded partner report to S3: v2/123/monthly/2026-04-01.json',
      )
      job.send(:upload_to_s3, sample_json_data, service_provider_id: 123, period_date: period_date)
    end

    context 'when bucket_name is not present' do
      before do
        allow(job).to receive(:bucket_name).and_return(nil)
      end

      it 'skips upload without error' do
        expect(job).not_to receive(:upload_file_to_s3_bucket)
        job.send(
          :upload_to_s3, sample_json_data, service_provider_id: 123,
                                           period_date: period_date
        )
      end
    end
    context 'with report_version v1' do
      let(:v1_job) { described_class.new(nil, report_version: 'v1') }

      before do
        allow(v1_job).to receive(:generate_base_s3_path).
          with(directory: 'portal').
          and_return('')
      end

      it 'uploads to both the legacy path and the versioned v1 path' do
        body = JSON.pretty_generate(sample_json_data)

        expect(v1_job).to receive(:upload_file_to_s3_bucket).with(
          path: '123/monthly/2026-04-01.json',
          body: body,
          content_type: 'application/json',
          bucket: bucket_name,
        )

        expect(v1_job).to receive(:upload_file_to_s3_bucket).with(
          path: 'v1/123/monthly/2026-04-01.json',
          body: body,
          content_type: 'application/json',
          bucket: bucket_name,
        )

        v1_job.send(
          :upload_to_s3,
          sample_json_data,
          service_provider_id: 123,
          period_date: period_date,
        )
      end

      it 'logs both v1 upload paths' do
        allow(v1_job).to receive(:upload_file_to_s3_bucket).and_return(true)

        expect(Rails.logger).to receive(:info).with(
          'Uploaded partner report to S3: 123/monthly/2026-04-01.json',
        )

        expect(Rails.logger).to receive(:info).with(
          'Uploaded partner report to S3: v1/123/monthly/2026-04-01.json',
        )

        v1_job.send(
          :upload_to_s3,
          sample_json_data,
          service_provider_id: 123,
          period_date: period_date,
        )
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
  describe '#validate_service_provider_ids' do
    let(:complete_mapping) do
      {
        issuer1 => { id: 123 },
        issuer2 => { id: 456 },
      }
    end

    context 'when all service provider IDs exist in mapping' do
      it 'does not log any warnings' do
        expect(Rails.logger).not_to receive(:warn)
        job.send(:validate_service_provider_ids, sample_report_data, complete_mapping)
      end
    end

    context 'when some service provider IDs are missing from mapping' do
      let(:incomplete_mapping) do
        {
          issuer1 => { id: 123 },
          # Missing issuer2 (id: 456)
        }
      end

      it 'logs warning for missing service provider IDs' do
        expect(Rails.logger).to receive(:warn).with(
          "Service provider ID 456 for issuer '#{issuer2}' not found in issuer mapping",
        )
        job.send(:validate_service_provider_ids, sample_report_data, incomplete_mapping)
      end
    end

    context 'with nil report data' do
      let(:report_data_with_nils) do
        {
          issuer1 => sample_report_data[issuer1],
          issuer2 => nil,
        }
      end

      it 'skips nil entries without error' do
        # Note - this is logged at the data fetch/format level when data is missing/corrupted
        # We are not logging it again during the validate_service_provider_ids method, which has a
        # different scope
        expect(Rails.logger).not_to receive(:warn)
        job.send(:validate_service_provider_ids, report_data_with_nils, complete_mapping)
      end
    end

    context 'with missing service_provider_id in report data' do
      let(:report_data_missing_id) do
        {
          issuer1 => {
            issuer: issuer1,
            provider_information: {
              service_provider_name: 'Test App',
              # service_provider_id is missing
            },
          },
        }
      end

      it 'skips entries with missing service_provider_id' do
        expect(Rails.logger).not_to receive(:warn)
        job.send(:validate_service_provider_ids, report_data_missing_id, complete_mapping)
      end
    end
  end

  describe '#upload_issuer_mapping_to_s3' do
    let(:mapping_data) do
      {
        issuer1 => { id: 123 },
        issuer2 => { id: 456 },
      }
    end

    it 'uploads mapping to correct S3 path' do
      expected_path = 'issuers_service_provider_id.json'
      expect(job).to receive(:upload_file_to_s3_bucket).with(
        path: expected_path,
        body: JSON.pretty_generate(mapping_data),
        content_type: 'application/json',
        bucket: bucket_name,
      )
      job.send(:upload_issuer_mapping_to_s3, mapping_data)
    end

    it 'logs successful upload' do
      expect(Rails.logger).to receive(:info).with(
        'Uploaded issuer mapping to S3: issuers_service_provider_id.json',
      )
      job.send(:upload_issuer_mapping_to_s3, mapping_data)
    end

    context 'when bucket_name is not present' do
      before do
        allow(job).to receive(:bucket_name).and_return(nil)
      end

      it 'skips upload without error' do
        expect(job).not_to receive(:upload_file_to_s3_bucket)
        job.send(:upload_issuer_mapping_to_s3, mapping_data)
      end
    end
  end
  describe 'integration with PartnerReportDefaultV2' do
    it 'passes correct parameters to report generator' do
      expect(Reporting::PartnerReportDefaultV2).to receive(:new).with(
        report_date: report_date,
        report_cadence: 'monthly',
        included_issuers: nil,
        excluded_issuers: nil,
      )

      job.perform(report_date)
    end

    it 'calls get_period_date_from_report_date with correct parameters' do
      job.instance_variable_set(:@report_date, report_date)

      expect(Reporting::PartnerReportDefaultV2).to receive(:get_period_date_from_report_date).with(
        report_date: report_date,
        cadence: 'monthly',
      )

      job.send(:period_date)
    end
  end
  describe '#normalize_issuer_list' do
    it 'returns nil for nil input' do
      expect(job.send(:normalize_issuer_list, nil)).to be_nil
    end

    it 'converts single string to array' do
      expect(job.send(:normalize_issuer_list, 'test')).to eq(['test'])
    end

    it 'returns array as-is' do
      expect(job.send(:normalize_issuer_list, ['a', 'b'])).to eq(['a', 'b'])
    end

    it 'removes nil and empty values' do
      expect(job.send(:normalize_issuer_list, ['a', nil, '', 'b'])).to eq(['a', 'b'])
    end

    it 'returns nil for empty array after filtering' do
      expect(job.send(:normalize_issuer_list, [nil, '', ''])).to be_nil
    end

    it 'converts non-string values to strings' do
      expect(
        job.send(
          :normalize_issuer_list,
          [1, :symbol, 'string'],
        ),
      ).to eq(['1', 'symbol', 'string'])
    end
  end
end
