# frozen_string_literal: true

module Reporting
  # Generates partner reports for all relevant/active service providers for single reporting period
  #
  # Class expects the caller (job wrapper) to handle report_date to calendar_id conversion:
  # - Monthly: Date -> YYYYMM01 (e.g., '2026-03-15' -> 20260301)
  # - Daily: Date -> YYYYMMDD (e.g., '2026-03-15' -> 20260315)
  # - Weekly: Date -> Week start calendar_id (logic not yet implemented in job wrapper)
  #
  # Usage:
  #   reporter = PartnerReportDefaultMonthly.new(
  #     calendar_id: 20260301,
  #     report_date: '2026-03-15',
  #     report_cadence: 'monthly'
  #   )
  #   reports = reporter.generate_reports  # Returns { issuer => json_data }
  #
  class PartnerReportDefault
    # Marts table mappings for different report cadences
    CADENCE_TABLES = {
      'monthly' => {
        usage: 'marts.sp_usage_metrics_monthly',
        idv: 'marts.sp_idv_outcomes_monthly',
        auth: 'marts.sp_auth_metrics_monthly',
        account: 'marts.sp_account_creation_metrics_monthly',
      },
      'weekly' => {
        usage: 'marts.sp_usage_metrics_weekly',
        idv: 'marts.sp_idv_outcomes_weekly',
        auth: 'marts.sp_auth_metrics_weekly',
        account: 'marts.sp_account_creation_metrics_weekly',
      },
      'daily' => {
        usage: 'marts.sp_usage_metrics_daily',
        idv: 'marts.sp_idv_outcomes_daily',
        auth: 'marts.sp_auth_metrics_daily',
        account: 'marts.sp_account_creation_metrics_daily',
      },
    }.freeze

    attr_reader :calendar_id, :report_date, :report_cadence, :included_issuers, :excluded_issuers

    def initialize(
      calendar_id:,
      report_date:,
      report_cadence: 'monthly',
      included_issuers: nil,
      excluded_issuers: []
    )
      unless ['monthly', 'weekly', 'daily'].include?(report_cadence)
        raise ArgumentError, "Invalid report_cadence: #{report_cadence}. "\
                             "Must be one of: monthly, weekly, daily"
      end

      unless calendar_id.is_a?(Integer) && calendar_id > 0
        raise ArgumentError, "calendar_id must be a positive integer, got: #{calendar_id}"
      end

      @calendar_id = calendar_id
      @report_date = report_date
      @report_cadence = report_cadence
      @included_issuers = included_issuers
      @excluded_issuers = excluded_issuers
    end

    # Returns data hash: { issuer => json_data }
    def generate_reports
      raw_data = fetch_bulk_data
      format_by_issuer(raw_data)
    end

    private

    def format_by_issuer(raw_data)
      if raw_data.empty?
        Rails.logger.warn "No data returned for #{report_cadence} "\
                          "report with calendar_id: #{calendar_id}"
        return {}
      end
      results = {}
      duplicate_issuers = []

      raw_data.each do |row|
        issuer = row['issuer']

        # With single date approach, no duplicates should be possible
        if results[issuer]
          duplicate_issuers << issuer
          Rails.logger.error "Duplicate data detected for #{issuer} - setting to"\
                             " nil for failed data integrity assumptions."
          results[issuer] = nil
          next
        end

        results[issuer] = format_row_as_json(row)
      end

      if duplicate_issuers.any?
        Rails.logger.error "Found #{duplicate_issuers.size} "\
                           "unexpected duplicate issuers: #{duplicate_issuers.join(', ')}"
      end

      results
    end

    def format_row_as_json(row)
      required_fields = %w[issuer service_provider_name period_start_date]
      missing_fields = required_fields.select { |field| row[field].nil? }

      if missing_fields.any?
        raise "Missing required fields: #{missing_fields.join(', ')}"
      end

      {
        issuer: row['issuer'],
        provider_information: {
          service_provider_name: row['service_provider_name'],
          agency_name: row['agency_name'],
          start_service_provider_id: row['service_provider_id'],
        },
        report_information: {
          period_start_date: row['period_start_date'],
          period_calendar_id: row['period_calendar_id'],
          report_cadence: report_cadence,
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

    def fetch_bulk_data
      Reports::BaseReport.transaction_with_timeout do
        DataWarehouseApplicationRecord.connection.execute(bulk_query).to_a
      end
    rescue StandardError => e
      Rails.logger.error "Failed to fetch #{report_cadence} partner report data: #{e.message}"
      raise e
    end

    def bulk_query
      tables = CADENCE_TABLES[report_cadence]

      # Map cadence to date truncation logic
      idv_date_field = case report_cadence
                      when 'monthly' then 'month_start_date_actual'
                      when 'weekly' then 'week_start_date_actual'
                      when 'daily' then 'date_actual'
                      end

      idv_calendar_field = case report_cadence
                          when 'monthly' then 'month_start_calendar_id'
                          when 'weekly' then 'week_start_calendar_id'
                          when 'daily' then 'calendar_id'
                          end

      # Determine period end calculation for IAA active check
      period_end_calc = case report_cadence
                        when 'monthly' then "DATE_TRUNC('month', p.report_date) +"\
                                            " INTERVAL '1 month' - INTERVAL '1 day'"
                        when 'weekly' then "DATE_TRUNC('week', p.report_date) + "\
                                           "INTERVAL '1 week' - INTERVAL '1 day'"
                        when 'daily' then 'p.report_date'
                        end

      <<~SQL
        -- Purpose: Generate comprehensive partner metrics for all active service providers for a single reporting period
        
        WITH date_params AS (
            SELECT
                '#{report_date}'::date as report_date,
                #{calendar_id} as period_calendar_id
        ),
        
        -- Get all relevant active service providers for the target period
        active_service_providers AS (
            SELECT sp.service_provider_id,
                   sp.service_provider_name,
                   sp.issuer,
                   sp.is_active, 
                   sp.agency_name,
                   sp.agency_abbreviation,
                   sp.launch_date,
                   sp.launch_date_calendar_id,
                   sp.iaa_end_date,
                   p.period_calendar_id,
                   p.report_date
            FROM marts.service_providers sp
            CROSS JOIN date_params p
            WHERE sp.iaa_end_date > #{period_end_calc}  -- Active through end of period
              AND p.period_calendar_id >= sp.launch_date_calendar_id
              #{issuer_filter_clause}
        )
        
        SELECT 
            -- Service Provider Information
            sp.issuer,
            sp.service_provider_name,
            sp.agency_name,
            sp.service_provider_id,
            
            -- Report Period (let the marts table provide the actual period date)
            -- Most tables use period_date, but IdV tables use time range specific field names
            -- Zach is updating this in the marts table soon, which will simplify this
            COALESCE(
              usage_data.period_date, 
              sp_data.#{idv_date_field}, 
              auth_data.period_date, 
              acct_data.period_date
            ) as period_start_date,
            sp.period_calendar_id,
                
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

            FROM active_service_providers sp
            LEFT JOIN #{tables[:usage]} usage_data
                ON usage_data.service_provider_id = sp.service_provider_id
                AND usage_data.period_date_id = sp.period_calendar_id
            LEFT JOIN #{tables[:idv]} sp_data
                ON sp_data.start_service_provider_id = sp.service_provider_id
                AND sp_data.#{idv_calendar_field} = sp.period_calendar_id
            LEFT JOIN #{tables[:auth]} auth_data
                ON auth_data.service_provider_id = sp.service_provider_id
                AND auth_data.period_date_id = sp.period_calendar_id
            LEFT JOIN #{tables[:account]} acct_data
                ON acct_data.service_provider_id = sp.service_provider_id
                AND acct_data.period_date_id = sp.period_calendar_id
            ORDER BY sp.issuer;
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
