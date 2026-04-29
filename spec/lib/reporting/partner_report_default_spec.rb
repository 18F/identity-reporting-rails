# frozen_string_literal: true

require 'rails_helper'
require 'reporting/partner_report_default'

RSpec.describe Reporting::PartnerReportDefault do
  let(:report_date) { '2026-03-15' }
  let(:period_date) { '2026-03-01' }
  let(:report_cadence) { 'monthly' }
  let(:issuer1) { 'urn:gov:gsa:openidconnect.profiles:sp:test:agency1' }
  let(:issuer2) { 'urn:gov:gsa:openidconnect.profiles:sp:test:agency2' }
  let(:issuer3) { 'urn:gov:gsa:openidconnect.profiles:sp:test:agency3' }

  # Sample complete row data with new column names
  let(:complete_row_data) do
    {
      'issuer' => issuer1,
      'service_provider_name' => 'Agency 1 Application',
      'agency_name' => 'Test Agency 1',
      'service_provider_id' => 123,
      'report_date' => report_date,
      'period_date_id' => 20260301,
      'period_date_actual' => period_date,
      # Usage metrics
      'count_active_users' => 1000,
      'count_newly_created_accounts' => 50,
      'count_existing_accounts' => 950,
      'count_newly_proofed_users' => 30,
      'count_preverified_users' => 20,
      'count_authentications' => 50,
      # Identity verification outcomes
      'count_pass_sum' => 4800,
      'count_newly_verified_sum' => 1000,
      'count_deadend_sum' => 50,
      'count_friction_sum' => 1000,
      'count_abandon_sum' => 50,
      'count_fraud_sum' => 5000,
      'count_inauthentic_doc' => 30,
      'count_facial_mismatch' => 5000,
      'count_invalid_attributes_dl_dos' => 50,
      'count_ssn_dob_deceased' => 5000,
      'count_address_other_not_found' => 50,
      'count_pending_lg99_likely_fraud' => 1000,
      'count_stayed_blocked' => 50,
      'count_fraud_alert' => 30,
      'count_suspicious_phone' => 5000,
      'count_lack_phone_ownership' => 30,
      'count_wrong_phone_type' => 50,
      'count_blocked_by_ipp_fraud' => 30,
      'count_pass_via_lg99' => 30,
      'count_pass_online_finalization' => 1000,
      'count_pass_ipp_online_portion' => 50,
      'count_pass_via_letter' => 1000,
      'count_doc_auth_ux' => 30,
      'count_selfie_ux' => 30,
      'count_dob_incorrect' => 50,
      'count_ssn_incorrect' => 30,
      'count_identity_not_found' => 30,
      'count_friction_during_otp' => 5000,
      'count_doc_auth_technical_issue' => 50,
      'count_resolution_technical_issues' => 30,
      'count_doc_auth_processing_issue' => 30,
      # Authentication metrics
      'count_auth_successful' => 50,
      'count_auth_failure' => 50,
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
      # Account creation metrics
      'count_creation_successful' => 45,
      'count_creation_failed' => 5,
      'count_registered_blocked_fraud' => 2,
    }
  end

  # Row missing required fields
  let(:incomplete_row_data) do
    {
      'issuer' => issuer2,
      'service_provider_name' => nil, # Missing required field
      'agency_name' => 'Test Agency 2',
      'service_provider_id' => 456,
      'period_date_actual' => period_date,
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
  end

  describe '.get_period_date_from_report_date' do
    let(:calendar_query_result) { [{ 'period_date_actual' => period_date }] }

    before do
      allow(DataWarehouseApplicationRecord.connection).to receive(:execute).with(
        anything,
      ).and_return(calendar_query_result)
    end

    it 'returns period date for valid report date and cadence' do
      result = described_class.get_period_date_from_report_date(
        report_date: report_date,
        cadence: 'monthly',
      )
      expect(result).to eq(period_date)
    end

    it 'validates cadence parameter' do
      # The method logs an error and returns nil rather than raising
      expect(Rails.logger).to receive(:error).with(/Failed to get period_date/)

      result = described_class.get_period_date_from_report_date(
        report_date: report_date,
        cadence: 'invalid',
      )
      expect(result).to be_nil
    end

    context 'when no calendar entry exists' do
      before do
        allow(DataWarehouseApplicationRecord.connection).to receive(:execute).
          and_return([])
      end

      it 'returns nil and logs error' do
        expect(Rails.logger).to receive(:error).with(
          "No calendar entry found for report_date: #{report_date}",
        )
        result = described_class.get_period_date_from_report_date(
          report_date: report_date,
          cadence: 'monthly',
        )
        expect(result).to be_nil
      end
    end

    context 'when database error occurs' do
      before do
        allow(DataWarehouseApplicationRecord.connection).to receive(:execute).
          and_raise(StandardError.new('Database error'))
      end

      it 'returns nil and logs error' do
        expect(Rails.logger).to receive(:error).with(
          /Failed to get period_date for #{report_date}, monthly: Database error/,
        )
        result = described_class.get_period_date_from_report_date(
          report_date: report_date,
          cadence: 'monthly',
        )
        expect(result).to be_nil
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
      subject(:filtered_report) do
        described_class.new(
          report_date: report_date,
          report_cadence: report_cadence,
          included_issuers: [issuer1, issuer2],
          excluded_issuers: [issuer3],
        )
      end

      it 'sets filters correctly' do
        expect(filtered_report.included_issuers).to eq([issuer1, issuer2])
        expect(filtered_report.excluded_issuers).to eq([issuer3])
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

      it 'formats data correctly with new column names' do
        result = report.generate_reports
        data = result[issuer1]

        expect(data[:issuer]).to eq(issuer1)
        expect(data[:provider_information][:service_provider_name]).to eq('Agency 1 Application')
        expect(data[:provider_information][:start_service_provider_id]).to eq(123)
        expect(data[:report_information][:period_start_date]).to eq(period_date)
        expect(data[:report_information][:period_calendar_id]).to eq(20260301)
        expect(data[:report_information][:report_cadence]).to eq('monthly')

        # Verify new column names in data section
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

  describe 'SQL query methods' do
    describe '#bulk_query' do
      it 'includes correct table references for monthly cadence' do
        query = report.send(:bulk_query)
        expect(query).to include('marts.sp_usage_metrics_monthly')
        expect(query).to include('marts.sp_idv_outcomes_monthly')
        expect(query).to include('marts.sp_auth_metrics_monthly')
        expect(query).to include('marts.sp_account_creation_metrics_monthly')
      end

      context 'with weekly cadence' do
        let(:report_cadence) { 'weekly' }

        it 'uses weekly tables' do
          query = report.send(:bulk_query)
          expect(query).to include('marts.sp_usage_metrics_weekly')
          expect(query).to include('week_start_calendar_id')
          expect(query).to include('week_start_date_actual')
        end
      end

      context 'with daily cadence' do
        let(:report_cadence) { 'daily' }

        it 'uses daily tables' do
          query = report.send(:bulk_query)
          expect(query).to include('marts.sp_usage_metrics_daily')
          expect(query).to include('calendar_id')
          expect(query).to include('date_actual')
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

      it 'logs error and re-raises' do
        expect(Rails.logger).to receive(:error).with(
          /Failed to fetch monthly partner report data: Database connection failed/,
        )
        expect { report.generate_reports }.to raise_error(StandardError)
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
          expect(query).to include("'#{cadence}' AS cadence")
          expect(query).to include("marts.sp_usage_metrics_#{cadence}")
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
      expect(provider_info[:start_service_provider_id]).to eq(123)
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
      expect(data.keys.size).to eq(52) # All integer fields
    end
  end
end
