# frozen_string_literal: true

module Reporting
  class PartnerReportDefaultMonthly
    attr_reader :time_range, :included_issuers, :excluded_issuers

    def initialize(
      time_range:,
      included_issuers: nil,
      excluded_issuers: []
    )
      @time_range = time_range
      @included_issuers = included_issuers
      @excluded_issuers = excluded_issuers
    end

    # Returns nested hash: { issuer => { month_start_date => json_data } }
    def generate_reports
      raw_data = fetch_bulk_data
      slice_by_issuer_and_month(raw_data)
    end

    def generate_report_for_issuer(issuer)
      generate_reports[issuer]
    end

    def generate_report_for_issuer_and_month(issuer, month_start_date)
      generate_reports.dig(issuer, month_start_date)
    end

    private

    def slice_by_issuer_and_month(raw_data)
      results = {}
      duplicate_entries = []

      raw_data.each do |row|
        issuer = row['issuer']
        next if should_exclude_issuer?(issuer) # Should never happen, already filtered in SQL

        month_start_date = format_month_start_date(row['month_start_date_actual'])
        results[issuer] ||= {}

        # Note: With cartesian product approach, duplicates should be impossible
        # but keeping this as a safety check
        if results[issuer][month_start_date]
          duplicate_entries << "#{issuer}/#{month_start_date}"
          Rails.logger.error "Unexpected duplicate data detected for "\
                             "#{issuer} / #{month_start_date} - setting to nil"
          results[issuer][month_start_date] = nil
          next
        end

        results[issuer][month_start_date] = format_row_as_json(row)
      end

      if duplicate_entries.any?
        Rails.logger.error "Found #{duplicate_entries.size} unexpected duplicate combinations: #{duplicate_entries.join(', ')}"
      end

      results
    end

    def format_row_as_json(row)
      required_fields = %w[issuer service_provider_name month_start_date_actual]
      missing_fields = required_fields.select { |field| row[field].nil? }

      if missing_fields.any?
        raise "Missing required fields: #{missing_fields.join(', ')}"
      end

      {
        issuer: row['issuer'],
        provider_information: {
          service_provider_name: row['service_provider_name'],
          agency_name: row['agency_name'],
          service_provider_id: row['service_provider_id'], # Updated field name
        },
        report_information: {
          month_start_date_actual: row['month_start_date_actual'],
          month_start_calendar_id: row['month_start_calendar_id'],
        },
        data: build_data_section(row),
      }
    end

    def build_data_section(row)
      integer_fields = %w[
        total_active_users
        newly_created_accounts
        existing_accounts
        newly_proofed_users
        preverified_users
        total_authentications
        total_pass_sum
        total_newly_verified_sum
        total_deadend_sum
        total_friction_sum
        total_abandon_sum
        total_fraud_sum
        count_inauthentic_doc
        count_facial_mismatch
        count_invalid_attributes_dl_dos
        count_ssn_dob_deceased
        count_address_other_not_found
        count_pending_lg99_likely_fraud
        count_stayed_blocked
        count_fraud_alert
        count_suspicious_phone
        count_lack_phone_ownership
        count_wrong_phone_type
        count_blocked_by_ipp_fraud
        count_pass_via_lg99
        count_pass_online_finalization
        count_pass_ipp_online_portion
        count_pass_via_letter
        count_doc_auth_ux
        count_selfie_ux
        count_dob_incorrect
        count_ssn_incorrect
        count_identity_not_found
        count_friction_during_otp
        count_doc_auth_technical_issue
        count_resolution_technical_issues
        count_doc_auth_processing_issue
        successful_auth_count
        failure_auth_count
        desktop_successful_count
        mobile_successful_count
        webauthn_platform_successful_count
        totp_successful_count
        piv_cac_successful_count
        sms_successful_count
        voice_successful_count
        backup_code_successful_count
        webauthn_successful_count
        personal_key_successful_count
        successful_creation_count
        failed_creation_count
        registered_blocked_fraud_count
      ]

      integer_fields.each_with_object({}) do |field, hash|
        value = row[field]
        if value.nil? || value.to_s.strip.empty?
          hash[field.to_sym] = nil
        else
          begin
            converted_value = Integer(value)
            hash[field.to_sym] = converted_value
          rescue ArgumentError, TypeError => e
            Rails.logger.warn "Failed to convert field #{field} with value #{value}: #{e.message}"
            hash[field.to_sym] = nil
          end
        end
      end
    end

    def format_month_start_date(date_actual)
      return nil if date_actual.nil?
      Date.parse(date_actual.to_s).strftime('%Y-%m-%d')
    rescue Date::Error => e
      Rails.logger.error "Invalid date format for month_start_date_actual: #{date_actual}"
      raise e
    end

    def fetch_bulk_data
      Reports::BaseReport.transaction_with_timeout do
        DataWarehouseApplicationRecord.connection.execute(bulk_query).to_a
      end
    rescue StandardError => e
      Rails.logger.error "Failed to fetch monthly partner report data: #{e.message}"
      raise e
    end

    def bulk_query
      <<~SQL
        -- Purpose: Generate comprehensive partner monthly metrics for all active service providers
        --          using cartesian product approach to ensure exactly one row per (SP, month) combination
        
        WITH 
        -- Get all active service providers with IAA end date in reporting range
        active_service_providers AS (
            SELECT sp.service_provider_id,
                  sp.service_provider_name,
                  sp.issuer,
                  sp.is_active, 
                  sp.agency_name,
                  sp.agency_abbreviation,
                  sp.launch_date,
                  sp.launch_date_calendar_id,
                  sp.iaa_end_date
            FROM marts.service_providers sp
            WHERE sp.iaa_end_date > TO_DATE('#{end_calendar_id}', 'YYYYMMDD')
              #{issuer_filter_clause}
        ),
        
        -- Find the calendar IDs within reporting range
        date_range AS (
            SELECT DISTINCT usage.period_date_id as month_calendar_id,
                   usage.period_date as month_start_date
            FROM marts.sp_usage_metrics_monthly usage
            WHERE usage.period_date_id >= #{start_calendar_id}
              AND usage.period_date_id <= #{end_calendar_id}
        ),
        
        -- Create the cartesian product as base for guaranteed uniqueness of (SP, month_calendar_id)
        base_combinations AS (
            SELECT sp.service_provider_id,
                   sp.service_provider_name,
                   sp.issuer,
                   sp.agency_name,
                   dr.month_calendar_id,
                   dr.month_start_date
            FROM active_service_providers sp
            CROSS JOIN date_range dr
            -- Filter out combinations before service provider launch
            WHERE (sp.launch_date_calendar_id IS NULL 
                   OR dr.month_calendar_id >= sp.launch_date_calendar_id)
        )

        SELECT 
            -- Service Provider Information
            bc.issuer,
            bc.service_provider_name,
            bc.agency_name,
            bc.service_provider_id,
            
            -- Report Period
            bc.month_start_date as month_start_date_actual,
            bc.month_calendar_id as month_start_calendar_id,
          
            -- ==============================================
            -- USAGE METRICS
            -- ==============================================

            -- Users That Accessed Services Via Login.gov
            usage_data.total_active_users,

            -- Active Users Breakdown
            usage_data.newly_created_accounts,
            usage_data.existing_accounts,

            -- Identity Verified Users
            usage_data.newly_proofed_users,
            usage_data.preverified_users,

            -- Authentications
            usage_data.total_authentications,

            -- ==============================================
            -- IDENTITY VERIFICATION OUTCOMES
            -- ==============================================

            -- Total Sum Counts
            sp_data.total_pass_sum,
            sp_data.total_newly_verified_sum,
            sp_data.total_deadend_sum,
            sp_data.total_friction_sum,
            sp_data.total_abandon_sum,
            sp_data.total_fraud_sum,

            -- Fraud Prevention: Document Fraud
            sp_data.count_inauthentic_doc,
            sp_data.count_facial_mismatch,
            sp_data.count_invalid_attributes_dl_dos,

            -- Fraud Prevention: Identity Fraud
            sp_data.count_ssn_dob_deceased,
            sp_data.count_address_other_not_found,
            sp_data.count_pending_lg99_likely_fraud,
            sp_data.count_stayed_blocked,
            sp_data.count_fraud_alert,

            -- Fraud Prevention: Phone Fraud
            sp_data.count_suspicious_phone,
            sp_data.count_lack_phone_ownership,
            sp_data.count_wrong_phone_type,

            -- Fraud Prevention: IPP Fraud
            sp_data.count_blocked_by_ipp_fraud,

            -- Fraud Prevention: Redress
            sp_data.count_pass_via_lg99,

            -- Identity Verification: Channels
            sp_data.count_pass_online_finalization,
            sp_data.count_pass_ipp_online_portion,
            sp_data.count_pass_via_letter,

            -- Identity Verification: UX Friction
            sp_data.count_doc_auth_ux,
            sp_data.count_selfie_ux,

            -- Identity Verification: Data Mismatch Friction
            sp_data.count_dob_incorrect,
            sp_data.count_ssn_incorrect,
            sp_data.count_identity_not_found,

            -- Identity Verification: Phone Friction
            sp_data.count_friction_during_otp,

            -- Identity Verification: Technical Issues
            sp_data.count_doc_auth_technical_issue,
            sp_data.count_resolution_technical_issues,
            sp_data.count_doc_auth_processing_issue,

            -- ==============================================
            -- AUTHENTICATION METRICS
            -- ==============================================

            -- Authentication Success Counts
            auth_data.successful_auth_count,
            auth_data.failure_auth_count,

            -- Device Type Counts
            auth_data.desktop_successful_count,
            auth_data.mobile_successful_count,

            -- MFA Type Counts
            auth_data.webauthn_platform_successful_count,  -- Face / Touch
            auth_data.totp_successful_count,               -- Authenticator App
            auth_data.piv_cac_successful_count,            -- PIV / CAC
            auth_data.sms_successful_count,                -- SMS
            auth_data.voice_successful_count,              -- Voice
            auth_data.backup_code_successful_count,        -- Backup Code
            auth_data.webauthn_successful_count,           -- Security Key
            auth_data.personal_key_successful_count,       -- Personal Key

            -- ==============================================
            -- ACCOUNT CREATION METRICS
            -- ==============================================

            -- Account Creation Success Rate Components
            acct_data.successful_creation_count,
            acct_data.failed_creation_count,

            -- Account Creation Fraud Prevention
            acct_data.registered_blocked_fraud_count

          
        FROM base_combinations bc
        LEFT JOIN marts.sp_usage_metrics_monthly usage_data
            ON usage_data.service_provider_id = bc.service_provider_id
            AND usage_data.period_date_id = bc.month_calendar_id
        LEFT JOIN marts.sp_idv_outcomes_monthly sp_data
            ON sp_data.start_service_provider_id = bc.service_provider_id
            AND sp_data.month_start_calendar_id = bc.month_calendar_id
        LEFT JOIN marts.sp_auth_metrics_monthly auth_data
            ON auth_data.service_provider_id = bc.service_provider_id
            AND auth_data.period_date_id = bc.month_calendar_id
        LEFT JOIN marts.sp_account_creation_metrics_monthly acct_data
            ON acct_data.service_provider_id = bc.service_provider_id
            AND acct_data.period_date_id = bc.month_calendar_id
        ORDER BY bc.issuer, bc.month_calendar_id
      SQL
    end

    def should_exclude_issuer?(issuer)
      if included_issuers&.any?
        return !included_issuers.include?(issuer)
      end
      if excluded_issuers.any?
        return excluded_issuers.include?(issuer)
      end
      false
    end

    def start_calendar_id
      time_range.begin.strftime('%Y%m01')
    end

    def end_calendar_id
      time_range.end.strftime('%Y%m01')
    end

    # Helper to optionally only retrieve some issuers or filter some issuers
    # Default - return all issuers within valid IAA / report time range
    def issuer_filter_clause
      if included_issuers&.any?
        sanitized = included_issuers.map { |i| ActiveRecord::Base.connection.quote(i) }
        "AND sp.issuer IN (#{sanitized.join(', ')})"
      elsif excluded_issuers.any?
        sanitized = excluded_issuers.map { |i| ActiveRecord::Base.connection.quote(i) }
        "AND sp.issuer NOT IN (#{sanitized.join(', ')})"
      else
        ''
      end
    end
  end
end
