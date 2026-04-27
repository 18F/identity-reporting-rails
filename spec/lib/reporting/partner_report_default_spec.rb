# frozen_string_literal: true

require 'rails_helper'
require 'reporting/partner_report_default'

RSpec.describe Reporting::PartnerReportDefault do
  let(:calendar_id) { 20240101 }
  let(:report_date) { '2024-01-15' }
  let(:report_cadence) { 'monthly' }
  let(:issuer1) { 'urn:gov:gsa:openidconnect.profiles:sp:test:agency1' }
  let(:issuer2) { 'urn:gov:gsa:openidconnect.profiles:sp:test:agency2' }
  let(:issuer3) { 'urn:gov:gsa:openidconnect.profiles:sp:test:agency3' }

  # Sample complete row data
  let(:complete_row_data) do
    {
      'issuer' => issuer1,
      'service_provider_name' => 'Agency 1 Application',
      'agency_name' => 'Test Agency 1',
      'start_service_provider_id' => 123,
      'period_start_date' => '2024-01-01',
      'period_calendar_id' => 20240101,
      'total_active_users' => 1000,
      'newly_created_accounts' => 50,
      'existing_accounts' => 950,
      'newly_proofed_users' => 30,
      'preverified_users' => 20,
      'total_authentications' => 50,
      'total_pass_sum' => 4800,
      'total_newly_verified_sum' => 1000,
      'total_deadend_sum' => 50,
      'total_friction_sum' => 1000,
      'total_abandon_sum' => 50,
      'total_fraud_sum' => 5000,
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
      'successful_auth_count' => 50,
      'failure_auth_count' => 50,
      'desktop_successful_count' => 1000,
      'mobile_successful_count' => 25,
      'webauthn_platform_successful_count' => 100,
      'totp_successful_count' => 200,
      'piv_cac_successful_count' => 50,
      'sms_successful_count' => 300,
      'voice_successful_count' => 25,
      'backup_code_successful_count' => 15,
      'webauthn_successful_count' => 75,
      'personal_key_successful_count' => 10,
      'successful_creation_count' => 45,
      'failed_creation_count' => 5,
      'registered_blocked_fraud_count' => 2,
    }
  end

  # Row missing required fields
  let(:incomplete_row_data) do
    {
      'issuer' => issuer2,
      'service_provider_name' => nil, # Missing required field
      'agency_name' => 'Test Agency 2',
      'start_service_provider_id' => 456,
      'period_start_date' => '2024-01-01',
      'period_calendar_id' => 20240101,
      'total_active_users' => 500,
    }
  end

  # Row with invalid date
  let(:invalid_date_row) do
    {
      'issuer' => issuer3,
      'service_provider_name' => 'Agency 3 Application',
      'agency_name' => 'Test Agency 3',
      'start_service_provider_id' => 789,
      'period_start_date' => 'invalid-date', # Invalid date
      'period_calendar_id' => 20240101,
      'total_active_users' => 200,
    }
  end

  subject(:report) do
    described_class.new(
      calendar_id: calendar_id,
      report_date: report_date,
      report_cadence: report_cadence,
      included_issuers: nil,
      excluded_issuers: [],
    )
  end

  before do
    allow(Reports::BaseReport).to receive(:transaction_with_timeout).and_yield
    allow(DataWarehouseApplicationRecord.connection).to receive(:execute).and_return(
      [complete_row_data, incomplete_row_data, invalid_date_row],
    )
  end

  describe '#initialize' do
    it 'sets parameters correctly' do
      expect(report.calendar_id).to eq(calendar_id)
      expect(report.report_date).to eq(report_date)
      expect(report.report_cadence).to eq('monthly')
    end

    it 'validates report_cadence' do
      expect do
        described_class.new(
          calendar_id: calendar_id,
          report_date: report_date,
          report_cadence: 'invalid',
        )
      end.to raise_error(ArgumentError, /Invalid report_cadence/)
    end

    it 'validates calendar_id' do
      expect do
        described_class.new(
          calendar_id: 'invalid',
          report_date: report_date,
          report_cadence: 'monthly',
        )
      end.to raise_error(ArgumentError, /calendar_id must be a positive integer/)
    end

    context 'with custom filters' do
      subject(:filtered_report) do
        described_class.new(
          calendar_id: calendar_id,
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

      it 'formats data correctly' do
        result = report.generate_reports
        data = result[issuer1]
        expect(data[:issuer]).to eq(issuer1)
        expect(data[:provider_information][:service_provider_name]).to eq('Agency 1 Application')
        expect(data[:report_information][:period_start_date]).to eq('2024-01-01')
        expect(data[:report_information][:report_cadence]).to eq('monthly')
        expect(data[:data][:total_active_users]).to eq(1000)
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

    context 'with duplicate issuers' do
      let(:duplicate_row_data) do
        complete_row_data.merge('total_active_users' => 2000)
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
          /No data returned for monthly report with calendar_id: #{calendar_id}/,
        )
        result = report.generate_reports
        expect(result).to eq({})
      end
    end
  end

  describe '#should_exclude_issuer?' do
    context 'with included_issuers specified' do
      subject(:filtered_report) do
        described_class.new(
          calendar_id: calendar_id,
          report_date: report_date,
          included_issuers: [issuer1, issuer2],
        )
      end

      it 'excludes issuers not in included list' do
        expect(filtered_report.send(:should_exclude_issuer?, issuer3)).to be true
      end

      it 'includes issuers in included list' do
        expect(filtered_report.send(:should_exclude_issuer?, issuer1)).to be false
      end
    end

    context 'with excluded_issuers specified' do
      subject(:filtered_report) do
        described_class.new(
          calendar_id: calendar_id,
          report_date: report_date,
          excluded_issuers: [issuer3],
        )
      end

      it 'excludes issuers in excluded list' do
        expect(filtered_report.send(:should_exclude_issuer?, issuer3)).to be true
      end

      it 'includes issuers not in excluded list' do
        expect(filtered_report.send(:should_exclude_issuer?, issuer1)).to be false
      end
    end

    context 'with no filters' do
      it 'includes all issuers' do
        expect(report.send(:should_exclude_issuer?, issuer1)).to be false
        expect(report.send(:should_exclude_issuer?, issuer2)).to be false
        expect(report.send(:should_exclude_issuer?, issuer3)).to be false
      end
    end
  end

  describe 'SQL query methods' do
    describe '#issuer_filter_clause' do
      context 'with included issuers' do
        subject(:filtered_report) do
          described_class.new(
            calendar_id: calendar_id,
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
            calendar_id: calendar_id,
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
      let(:invalid_integer_row) do
        complete_row_data.merge('total_active_users' => 'invalid_number')
      end

      before do
        allow(DataWarehouseApplicationRecord.connection).to receive(:execute).and_return(
          [invalid_integer_row],
        )
      end

      it 'logs warning and sets field to nil with strict conversion' do
        expect(Rails.logger).to receive(:warn).with(
          /Failed to convert field total_active_users with value invalid_number/,
        )
        result = report.generate_reports
        expect(result[issuer1][:data][:total_active_users]).to be_nil
      end
    end
  end
end
