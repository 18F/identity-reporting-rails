# frozen_string_literal: true

require 'rails_helper'
require 'reporting/partner_report_default_v2'

RSpec.describe Reporting::PartnerReportDefaultV2 do
  let(:report_date) { '2026-03-15' }
  let(:period_date) { '2026-03-01' }
  let(:report_cadence) { 'monthly' }
  let(:issuer1) { 'urn:gov:gsa:openidconnect.profiles:sp:test:agency1' }
  let(:issuer2) { 'urn:gov:gsa:openidconnect.profiles:sp:test:agency2' }
  let(:issuer3) { 'urn:gov:gsa:openidconnect.profiles:sp:test:agency3' }

  # Sample complete row data using V2 column names (new mapped fields)
  let(:complete_row_data) do
    {
      'issuer' => issuer1,
      'service_provider_name' => 'Agency 1 Application',
      'agency_name' => 'Test Agency 1',
      'service_provider_id' => 123,
      'period_date_id' => 20260301,
      'period_date' => period_date,
      'count_active_users' => 1000,
      'count_blocked_attempted_fraud' => 10,
      'count_newly_created_accounts' => 50,
      'count_existing_accounts' => 950,
      'count_identity_verified_users' => 60,
      'count_newly_proofed_users' => 30,
      'count_preverified_users' => 20,
      'count_authentications' => 500,
      'count_registered_blocked_fraud' => 2,
      'count_blocked_authentic_drivers_license' => 5,
      'count_facial_mismatch' => 8,
      'count_invalid_attributes_dl_dos' => 7,
      'count_blocked_identity_not_found' => 4,
      'count_fraud_alert' => 3,
      'count_suspicious_phone' => 9,
      'count_lack_phone_ownership' => 6,
      'count_wrong_phone_type' => 2,
      'count_blocked_by_ipp_fraud' => 1,
      'count_device_behavior_fraud_signals' => 11,
      'count_pass_via_lg99' => 12,
      'count_creation_successful' => 45,
      'count_total_creations' => 50,
      'count_auth_successful' => 50,
      'count_total_auths' => 55,
      'count_desktop_successful' => 1000,
      'count_mobile_successful' => 25,
      'count_webauthn_platform_successful' => 100,
      'count_totp_successful' => 200,
      'count_piv_cac_successful' => 50,
      'count_sms_successful' => 300,
      'count_voice_successful' => 25,
      'count_backup_code_successful' => 15,
      'count_webauthn_successful' => 75,
      'count_personal_key_successful' => 10,
      'count_pass_sum' => 4800,
      'count_sum_outcomes' => 5000,
      'count_deadend_sum' => 50,
      'count_stage_onboarding' => 40,
      'count_skip_preverified_finalization' => 20,
      'count_pass_online_finalization' => 1000,
      'count_pass_ipp' => 30,
      'count_pass_via_letter' => 1000,
      'count_blocked_document_upload_ux' => 15,
      'count_selfie_ux' => 30,
      'count_identity_resolution_attribute_mismatch' => 22,
      'count_phone_number_record_check_failure' => 18,
      'count_temporary_technical_issues' => 14,
    }
  end

  # Row missing required fields
  let(:incomplete_row_data) do
    {
      'issuer' => issuer2,
      'service_provider_name' => nil, # Missing required field
      'agency_name' => 'Test Agency 2',
      'service_provider_id' => 456,
      'period_date' => period_date,
      'period_date_id' => 20260301,
      'count_active_users' => 500,
    }
  end

  # Row with nil values for numeric fields
  let(:nil_values_row) do
    complete_row_data.merge(
      'issuer' => issuer3,
      'service_provider_name' => 'Agency 3 Application',
      'count_active_users' => nil,
      'count_authentications' => '',
      'count_auth_successful' => '   ',
    )
  end

  let(:issuer_mapping_data) do
    [
      { 'issuer' => issuer1, 'id' => 123 },
      { 'issuer' => issuer2, 'id' => 456 },
      { 'issuer' => issuer3, 'id' => 789 },
    ]
  end

  let(:duplicate_issuer_mapping_data) do
    [
      { 'issuer' => issuer1, 'id' => 123 },
      { 'issuer' => issuer1, 'id' => 999 }, # duplicate issuer
      { 'issuer' => issuer2, 'id' => 456 },
    ]
  end

  let(:invalid_id_mapping_data) do
    [
      { 'issuer' => issuer1, 'id' => 123 },
      { 'issuer' => issuer2, 'id' => 'invalid_id' },
      { 'issuer' => issuer3, 'id' => nil },
    ]
  end

  subject(:report) do
    described_class.new(
      report_date: report_date,
      report_cadence: report_cadence,
      included_issuers: nil,
      excluded_issuers: [],
    )
  end

  before do
    allow(Reports::BaseReport).to receive(:transaction_with_timeout).and_yield
    allow(DataWarehouseApplicationRecord.connection).to receive(:execute).and_return(
      [complete_row_data, incomplete_row_data, nil_values_row],
    )
    allow(described_class).to receive(:get_period_date_from_report_date).and_return(period_date)
  end

  describe '.get_period_date_from_report_date' do
    let(:calendar_query_result) do
      double('query_result', first: { 'period_date_actual' => period_date })
    end

    before do
      allow(described_class).to receive(:get_period_date_from_report_date).and_call_original
      allow(DataWarehouseApplicationRecord.connection).to receive(
        :exec_query,
      ).and_return(calendar_query_result)
    end

    it 'returns period date for valid report date and cadence' do
      result = described_class.get_period_date_from_report_date(
        report_date: report_date,
        cadence: 'monthly',
      )
      expect(result).to eq(period_date)
    end

    it 'raises error for invalid report_cadence' do
      expect do
        described_class.new(
          report_date: report_date,
          report_cadence: 'invalid',
        )
      end.to raise_error(ArgumentError, /Invalid report_cadence/)
    end

    context 'when no calendar entry exists' do
      before do
        allow(DataWarehouseApplicationRecord.connection).to receive(:exec_query).and_return(
          double('query_result', first: nil),
        )
      end

      it 'raises StandardError' do
        expect do
          described_class.get_period_date_from_report_date(
            report_date: report_date,
            cadence: 'monthly',
          )
        end.to raise_error(StandardError, "No calendar entry found for report_date: #{report_date}")
      end
    end

    context 'when database error occurs' do
      before do
        allow(DataWarehouseApplicationRecord.connection).to receive(:exec_query).
          and_raise(PG::UndefinedTable.new('ERROR: relation "marts.calendar" does not exist'))
      end

      it 'allows error to bubble up' do
        expect do
          described_class.get_period_date_from_report_date(
            report_date: report_date,
            cadence: 'monthly',
          )
        end.to raise_error(PG::UndefinedTable)
      end
    end

    context 'when period_date_actual is nil in result' do
      before do
        allow(DataWarehouseApplicationRecord.connection).to receive(:exec_query).and_return(
          double('query_result', first: { 'period_date_actual' => nil }),
        )
      end

      it 'raises StandardError' do
        expect do
          described_class.get_period_date_from_report_date(
            report_date: report_date,
            cadence: 'monthly',
          )
        end.to raise_error(
          StandardError,
          "No period_date_actual found for report_date: #{report_date}",
        )
      end
    end
  end

  describe '#initialize' do
    it 'sets parameters correctly' do
      expect(report.report_date).to eq(report_date)
      expect(report.report_cadence).to eq('monthly')
    end

    it 'converts report_date to string' do
      date_report = described_class.new(
        report_date: Date.parse(report_date),
        report_cadence: 'monthly',
      )
      expect(date_report.report_date).to eq(report_date)
    end

    it 'validates report_cadence' do
      expect do
        described_class.new(
          report_date: report_date,
          report_cadence: 'invalid',
        )
      end.to raise_error(ArgumentError, /Invalid report_cadence/)
    end

    context 'with custom filters' do
      it 'sets filters correctly' do
        # Test with included_issuers only
        included_report = described_class.new(
          report_date: report_date,
          report_cadence: report_cadence,
          included_issuers: [issuer1, issuer2],
          excluded_issuers: [],
        )
        expect(included_report.included_issuers).to eq([issuer1, issuer2])
        expect(included_report.excluded_issuers).to eq([])

        # Test with excluded_issuers only
        excluded_report = described_class.new(
          report_date: report_date,
          report_cadence: report_cadence,
          included_issuers: nil,
          excluded_issuers: [issuer3],
        )
        expect(excluded_report.included_issuers).to be_nil
        expect(excluded_report.excluded_issuers).to eq([issuer3])

        # Test that both together raises error
        expect do
          described_class.new(
            report_date: report_date,
            report_cadence: report_cadence,
            included_issuers: [issuer1],
            excluded_issuers: [issuer3],
          )
        end.to raise_error(
          ArgumentError, 'Cannot specify both included_issuers and excluded_issuers'
        )
      end
    end
  end

  describe '#generate_reports' do
    context 'with complete valid data' do
      before do
        allow(DataWarehouseApplicationRecord.connection).to receive(:execute).and_return(
          [complete_row_data],
        )
      end

      it 'returns flat hash structure' do
        result = report.generate_reports
        expect(result).to be_a(Hash)
        expect(result[issuer1]).to be_a(Hash)
        expect(result[issuer1][:issuer]).to eq(issuer1)
      end

      it 'formats data correctly with V2 column names' do
        result = report.generate_reports
        data = result[issuer1]

        expect(data[:issuer]).to eq(issuer1)
        expect(data[:provider_information][:service_provider_name]).to eq('Agency 1 Application')
        expect(data[:provider_information][:service_provider_id]).to eq(123)
        expect(data[:report_information][:period_start_date]).to eq(period_date)
        expect(data[:report_information][:period_calendar_id]).to eq(20260301)
        expect(data[:report_information][:report_cadence]).to eq('monthly')

        # Verify V2 column names in data section
        expect(data[:data][:count_active_users]).to eq(1000)
        expect(data[:data][:count_newly_created_accounts]).to eq(50)
        expect(data[:data][:count_auth_successful]).to eq(50)
        expect(data[:data][:count_creation_successful]).to eq(45)
      end
    end

    context 'with missing required fields' do
      before do
        allow(DataWarehouseApplicationRecord.connection).to receive(:execute).and_return(
          [incomplete_row_data],
        )
      end

      it 'raises error for missing required fields' do
        expect { report.generate_reports }.to raise_error(
          /Missing required fields: service_provider_name/,
        )
      end
    end

    context 'with nil and empty string values' do
      before do
        allow(DataWarehouseApplicationRecord.connection).to receive(:execute).and_return(
          [nil_values_row],
        )
      end

      it 'preserves nil and empty strings as nil in output' do
        result = report.generate_reports
        data = result[issuer3][:data]

        expect(data[:count_active_users]).to be_nil
        expect(data[:count_authentications]).to be_nil
        expect(data[:count_auth_successful]).to be_nil
      end
    end

    context 'when integer conversion fails' do
      let(:invalid_integer_row) do
        complete_row_data.merge('count_active_users' => 'invalid_number')
      end

      before do
        allow(DataWarehouseApplicationRecord.connection).to receive(:execute).and_return(
          [invalid_integer_row],
        )
      end

      it 'logs error and sets field to nil' do
        expect(Rails.logger).to receive(:error).with(
          /Failed to convert 'invalid_number' to integer for field count_active_users/,
        )

        result = report.generate_reports
        expect(result[issuer1][:data][:count_active_users]).to be_nil
      end
    end

    context 'with duplicate issuers' do
      let(:duplicate_row_data) do
        complete_row_data.merge('count_active_users' => 2000)
      end

      before do
        allow(DataWarehouseApplicationRecord.connection).to receive(:execute).and_return(
          [complete_row_data, duplicate_row_data],
        )
      end

      it 'sets duplicate entries to nil and logs error' do
        expect(Rails.logger).to receive(:error).with(
          /Duplicate data detected for #{Regexp.escape(issuer1)} - setting to nil/,
        )
        expect(Rails.logger).to receive(:error).with(
          "Found 1 unexpected duplicate issuers: #{issuer1}",
        )

        result = report.generate_reports
        expect(result[issuer1]).to be_nil
      end
    end

    context 'with empty results' do
      before do
        allow(DataWarehouseApplicationRecord.connection).to receive(:execute).and_return([])
      end

      it 'returns empty hash and logs warning' do
        expect(Rails.logger).to receive(:warn).with(
          "No data returned for monthly report with report_date: #{report_date}",
        )
        result = report.generate_reports
        expect(result).to eq({})
      end
    end
  end

  describe '#generate_issuer_mapping' do
    context 'with valid mapping data' do
      before do
        allow(DataWarehouseApplicationRecord.connection).to receive(:execute).
          with(anything).and_call_original
        allow(DataWarehouseApplicationRecord.connection).to receive(:execute).
          with(match(/FROM idp\.service_providers/)).and_return(issuer_mapping_data)
      end

      it 'returns correctly formatted mapping' do
        result = report.generate_issuer_mapping
        expect(result).to be_a(Hash)
        expect(result[issuer1]).to eq({ id: 123 })
        expect(result[issuer2]).to eq({ id: 456 })
        expect(result[issuer3]).to eq({ id: 789 })
      end

      it 'converts string IDs to integers' do
        mapping_data_with_string_ids = [
          { 'issuer' => issuer1, 'id' => '123' },
          { 'issuer' => issuer2, 'id' => '456' },
        ]
        allow(DataWarehouseApplicationRecord.connection).to receive(:execute).
          with(match(/FROM idp\.service_providers/)).and_return(mapping_data_with_string_ids)

        result = report.generate_issuer_mapping
        expect(result[issuer1][:id]).to eq(123)
        expect(result[issuer2][:id]).to eq(456)
      end
    end

    context 'with empty mapping data' do
      before do
        allow(DataWarehouseApplicationRecord.connection).to receive(:execute).
          with(match(/FROM idp\.service_providers/)).and_return([])
      end

      it 'returns empty hash and logs warning' do
        expect(Rails.logger).to receive(:warn).with(
          'No service providers found in idp.service_providers',
        )
        result = report.generate_issuer_mapping
        expect(result).to eq({})
      end
    end

    context 'with duplicate issuers' do
      before do
        allow(DataWarehouseApplicationRecord.connection).to receive(:execute).
          with(match(/FROM idp\.service_providers/)).and_return(duplicate_issuer_mapping_data)
      end

      it 'keeps first ID and logs error for duplicates' do
        expect(Rails.logger).to receive(:error).with(
          "Duplicate issuer found in idp.service_providers: #{issuer1}. Keeping first id.",
        )
        result = report.generate_issuer_mapping
        expect(result[issuer1][:id]).to eq(123) # First occurrence
        expect(result[issuer2][:id]).to eq(456)
      end
    end

    context 'with invalid ID values' do
      before do
        allow(DataWarehouseApplicationRecord.connection).to receive(:execute).
          with(match(/FROM idp\.service_providers/)).and_return(invalid_id_mapping_data)
      end

      it 'skips rows with invalid IDs and logs errors' do
        expect(Rails.logger).to receive(:error).with(
          "Invalid id value for issuer #{issuer2}: \"invalid_id\". Skipping row.",
        )
        expect(Rails.logger).to receive(:error).with(
          "Invalid id value for issuer #{issuer3}: nil. Skipping row.",
        )
        result = report.generate_issuer_mapping
        expect(result[issuer1][:id]).to eq(123)
        expect(result).not_to have_key(issuer2)
        expect(result).not_to have_key(issuer3)
      end
    end

    context 'when database query fails' do
      before do
        allow(DataWarehouseApplicationRecord.connection).to receive(:execute).
          with(match(/FROM idp\.service_providers/)).
          and_raise(StandardError.new('Database connection failed'))
      end

      it 'allows error to bubble up' do
        expect { report.generate_issuer_mapping }.to raise_error(
          StandardError, 'Database connection failed'
        )
      end
    end
  end

  describe 'SQL query methods' do
    describe '#bulk_query' do
      it 'includes correct table reference and subquery for monthly cadence' do
        query = report.send(:bulk_query)
        expect(query).to include('FROM marts.sp_partner_report_metrics_monthly')
        expect(query).to include('WHERE period_date =')
        expect(query).to include('AND issuer IN (')
        expect(query).to include('SELECT issuer')
        expect(query).to include('FROM marts.service_providers')
        expect(query).to include("iaa_end_date > '#{report_date}'::date")
        expect(query).to include("'#{report_date}'::date >= launch_date")
      end

      context 'with weekly cadence' do
        let(:report_cadence) { 'weekly' }
        it 'uses weekly table' do
          query = report.send(:bulk_query)
          expect(query).to include('FROM marts.sp_partner_report_metrics_weekly')
        end
      end

      context 'with daily cadence' do
        let(:report_cadence) { 'daily' }
        it 'uses daily table' do
          query = report.send(:bulk_query)
          expect(query).to include('FROM marts.sp_partner_report_metrics_daily')
        end
      end
    end

    describe '#issuer_filter_clause' do
      context 'with included issuers' do
        subject(:filtered_report) do
          described_class.new(
            report_date: report_date,
            included_issuers: [issuer1, issuer2],
          )
        end

        it 'generates IN clause' do
          clause = filtered_report.send(:issuer_filter_clause)
          expect(clause).to include('IN (')
          expect(clause).to include(issuer1)
          expect(clause).to include(issuer2)
        end
      end

      context 'with excluded issuers' do
        subject(:filtered_report) do
          described_class.new(
            report_date: report_date,
            excluded_issuers: [issuer3],
          )
        end

        it 'generates NOT IN clause' do
          clause = filtered_report.send(:issuer_filter_clause)
          expect(clause).to include('NOT IN (')
          expect(clause).to include(issuer3)
        end
      end

      context 'with no filters' do
        it 'returns empty string' do
          clause = report.send(:issuer_filter_clause)
          expect(clause).to eq('')
        end
      end
    end
  end

  describe 'error handling' do
    context 'when database query fails' do
      before do
        allow(DataWarehouseApplicationRecord.connection).to receive(:execute).and_raise(
          StandardError.new('Database connection failed'),
        )
      end

      it 'allows error to bubble up' do
        expect { report.generate_reports }.to raise_error(
          StandardError, 'Database connection failed'
        )
      end
    end

    context 'when integer conversion fails' do
      context 'with completely invalid non-numeric string' do
        let(:invalid_integer_row) do
          complete_row_data.merge('count_active_users' => 'invalid_number')
        end

        before do
          allow(DataWarehouseApplicationRecord.connection).to receive(:execute).and_return(
            [invalid_integer_row],
          )
        end

        it 'logs error and sets field to nil for unexpected alphanumeric values' do
          expect(Rails.logger).to receive(:error).with(
            /Failed to convert 'invalid_number' to integer for field count_active_users/,
          )

          result = report.generate_reports
          expect(result[issuer1][:data][:count_active_users]).to be_nil
        end
      end
    end
  end

  describe 'integration with different report cadences' do
    %w[monthly weekly daily].each do |cadence|
      context "with #{cadence} cadence" do
        let(:report_cadence) { cadence }

        it 'generates valid SQL query' do
          query = report.send(:bulk_query)
          expect(query).to be_a(String)
          expect(query).to include("FROM marts.sp_partner_report_metrics_#{cadence}")
          expect(query).to include('AND issuer IN (')
        end

        it 'handles data correctly' do
          allow(DataWarehouseApplicationRecord.connection).to receive(:execute).and_return(
            [complete_row_data],
          )

          result = report.generate_reports
          expect(result[issuer1][:report_information][:report_cadence]).to eq(cadence)
        end
      end
    end
  end

  describe '#format_row_as_json' do
    it 'includes all expected top-level keys' do
      result = report.send(:format_row_as_json, complete_row_data)

      expect(result.keys).to match_array(
        [:issuer, :provider_information, :report_information,
         :data],
      )
    end

    it 'includes correct provider information fields' do
      result = report.send(:format_row_as_json, complete_row_data)
      provider_info = result[:provider_information]

      expect(provider_info[:service_provider_name]).to eq('Agency 1 Application')
      expect(provider_info[:agency_name]).to eq('Test Agency 1')
      expect(provider_info[:service_provider_id]).to eq(123)
    end

    it 'includes correct report information fields' do
      result = report.send(:format_row_as_json, complete_row_data)
      report_info = result[:report_information]

      expect(report_info[:period_start_date]).to eq(period_date)
      expect(report_info[:period_calendar_id]).to eq(20260301)
      expect(report_info[:report_cadence]).to eq('monthly')
    end

    it 'includes all data fields with correct values' do
      result = report.send(:format_row_as_json, complete_row_data)
      data = result[:data]

      # Spot check a few fields from each category
      expect(data[:count_active_users]).to eq(1000)
      expect(data[:count_pass_sum]).to eq(4800)
      expect(data[:count_auth_successful]).to eq(50)
      expect(data[:count_creation_successful]).to eq(45)

      # Verify all expected fields are present
      expect(data.keys.size).to eq(Reporting::PartnerReportDefaultV2::INTEGER_DATA_FIELDS.size)

      # Verify all fields from constant are present
      Reporting::PartnerReportDefaultV2::INTEGER_DATA_FIELDS.each do |field|
        expect(data).to have_key(field.to_sym)
      end
    end
  end
end
