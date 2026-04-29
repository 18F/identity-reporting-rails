# frozen_string_literal: true

module Reporting
  # Generates partner reports for all relevant/active service providers for single reporting period
  # Uses marts.calendar table to determine reporting period start date given report_date and cadence
  # Usage:
  #   reporter = PartnerReportDefaultMonthly.new(
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

    attr_reader :report_date, :report_cadence, :included_issuers, :excluded_issuers

    def initialize(
      report_date:,
      report_cadence: 'monthly',
      included_issuers: nil,
      excluded_issuers: []
    )
      unless ['monthly', 'weekly', 'daily'].include?(report_cadence)
        raise ArgumentError, "Invalid report_cadence: #{report_cadence}. "\
                             "Must be one of: monthly, weekly, daily"
      end

      @report_date = report_date.to_s # Ensure report date is a string
      @report_cadence = report_cadence
      @included_issuers = included_issuers
      @excluded_issuers = excluded_issuers
    end

    def self.get_period_date_from_report_date(report_date:, cadence: 'monthly')
      # Given a date, retrieves the corresponding date for the start of its month/week/day
      # Returns string in format 'YYYY-MM-DD' or nil on failure
      unless ['monthly', 'weekly', 'daily'].include?(cadence)
        raise ArgumentError, "Invalid cadence: #{cadence}. Must be one of: monthly, weekly, daily"
      end

      query = <<~SQL
        SELECT 
          -- Dynamic period_date_actual based on cadence, formatted as YYYY-MM-DD
          CASE 
            WHEN '#{cadence}' = 'monthly' THEN TO_CHAR(month_start_date_actual, 'YYYY-MM-DD')
            WHEN '#{cadence}' = 'weekly' THEN TO_CHAR(week_start_date_actual, 'YYYY-MM-DD')
            WHEN '#{cadence}' = 'daily' THEN TO_CHAR(cal.date_actual, 'YYYY-MM-DD')
          END AS period_date_actual
        FROM marts.calendar cal
        WHERE cal.calendar_id = TO_CHAR('#{report_date}'::date, 'YYYYMMDD')::int;
      SQL

      result = DataWarehouseApplicationRecord.connection.execute(query).first
      if result.nil?
        Rails.logger.error "No calendar entry found for report_date: #{report_date}"
        return nil
      end

      result['period_date_actual'] # Now guaranteed to be in YYYY-MM-DD format

      if result.nil?
        Rails.logger.error "No calendar entry found for report_date: #{report_date}"
        return nil
      end

      result['period_date_actual']
    rescue StandardError => e
      Rails.logger.error "Failed to get period_date for #{report_date}, #{cadence}: #{e.message}"
      nil
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
                          "report with report_date: #{report_date}"
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
      required_fields = %w[issuer service_provider_name period_date_actual]
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
          period_start_date: row['period_date_actual'],
          period_calendar_id: row['period_date_id'],
          report_cadence: report_cadence,
        },
        data: build_data_section(row),
      }
    end

    def build_data_section(row)
      integer_fields = %w[
        count_active_users
        count_newly_created_accounts
        count_existing_accounts
        count_newly_proofed_users
        count_preverified_users
        count_authentications
        count_pass_sum
        count_newly_verified_sum
        count_deadend_sum
        count_friction_sum
        count_abandon_sum
        count_fraud_sum
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
        count_auth_successful
        count_auth_failure
        count_desktop_successful
        count_mobile_successful
        count_webauthn_platform_successful
        count_totp_successful
        count_piv_cac_successful
        count_sms_successful
        count_voice_successful
        count_backup_code_successful
        count_webauthn_successful
        count_personal_key_successful
        count_creation_successful
        count_creation_failed
        count_registered_blocked_fraud
      ]

      integer_fields.each_with_object({}) do |field, hash|
        value = row[field]

        if value.nil? || value.to_s.strip.empty?
          hash[field.to_sym] = nil
        else
          begin
            hash[field.to_sym] = Integer(value)
          rescue ArgumentError, TypeError => e
            # This shouldn't happen with marts tables, but log and handle gracefully
            Rails.logger.error "Failed to convert '#{value}' "\
                               "to integer for field #{field}: #{e.message}"
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

      # Map cadence to report period columns in marts.calendar table
      period_calendar_id_col = case report_cadence
                      when 'monthly' then 'month_start_calendar_id'
                      when 'weekly' then 'week_start_calendar_id'
                      when 'daily' then 'calendar_id'
                      end

      period_calendar_date_col = case report_cadence
                          when 'monthly' then 'month_start_date_actual'
                          when 'weekly' then 'week_start_date_actual'
                          when 'daily' then 'date_actual'
                          end

      <<~SQL
        WITH date_param AS (
            SELECT
                 '#{report_date}'::date as report_date
        ),
        date_period_id AS (
            SELECT 
                p.report_date,
                cal.calendar_id AS report_date_id, 
                cal.date_actual AS report_date_actual,
                '#{report_cadence}' AS cadence,
                cal.#{period_calendar_id_col} AS period_date_id, -- Int ID for start of report window
                cal.#{period_calendar_date_col} AS period_date_actual -- Date for start of report window        
            FROM marts.calendar cal
            JOIN date_param p 
            ON cal.calendar_id = TO_CHAR(p.report_date, 'YYYYMMDD')::int
        ),
        -- Get all active service providers within reporting range
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
                   date_p.report_date_id,
                   date_p.report_date,
                   date_p.period_date_id,
                   date_p.period_date_actual
            FROM marts.service_providers sp
            CROSS JOIN date_period_id date_p
            WHERE sp.iaa_end_date > date_p.report_date  
              AND date_p.report_date >= sp.launch_date   
              #{issuer_filter_clause}
        )
        
        
            SELECT 
            -- Service Provider Information
            sp.issuer,
            sp.service_provider_name,
            sp.agency_name,
            sp.service_provider_id,
            
            -- Report Period  
            sp.report_date,   -- Date passed in as a parameter / report date
            sp.period_date_id, -- start of the month for monthly, start of week for weekly, equal to report_date for daily
            sp.period_date_actual, -- same as period_date_id but in timestamp format
            
            -- ==============================================
            -- USAGE METRICS
            -- ==============================================
        
            -- Users That Accessed Services Via Login.gov
            usage_data.count_active_users,
        
            -- Active Users Breakdown
            usage_data.count_newly_created_accounts,
            usage_data.count_existing_accounts,
        
            -- Identity Verified Users
            usage_data.count_newly_proofed_users,
            usage_data.count_preverified_users,
        
            -- Authentications
            usage_data.count_authentications,
        
            -- ==============================================
            -- IDENTITY VERIFICATION OUTCOMES
            -- ==============================================
        
            -- Total Sum Counts
            sp_data.count_pass_sum,
            sp_data.count_newly_verified_sum,
            sp_data.count_deadend_sum,
            sp_data.count_friction_sum,
            sp_data.count_abandon_sum,
            sp_data.count_fraud_sum,
        
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
            auth_data.count_auth_successful,
            auth_data.count_auth_failure,
        
            -- Device Type Counts
            auth_data.count_desktop_successful,
            auth_data.count_mobile_successful,
        
            -- MFA Type Counts
            auth_data.count_webauthn_platform_successful,  -- Face / Touch
            auth_data.count_totp_successful,               -- Authenticator App
            auth_data.count_piv_cac_successful,            -- PIV / CAC
            auth_data.count_sms_successful,                -- SMS
            auth_data.count_voice_successful,              -- Voice
            auth_data.count_backup_code_successful,        -- Backup Code
            auth_data.count_webauthn_successful,           -- Security Key
            auth_data.count_personal_key_successful,        -- Personal Key
        
            -- ==============================================
            -- ACCOUNT CREATION METRICS
            -- ==============================================
        
            -- Account Creation Success Rate Components
            acct_data.count_creation_successful,
            acct_data.count_creation_failed,
        
            -- Account Creation Fraud Prevention
            acct_data.count_registered_blocked_fraud
        
            -- ==============================================
            -- MARTS TABLE JOINS
            -- ==============================================
            FROM active_service_providers sp
            LEFT JOIN #{tables[:usage]} usage_data
                ON usage_data.service_provider_id = sp.service_provider_id
                AND usage_data.period_date_id = sp.period_date_id
            LEFT JOIN #{tables[:idv]} sp_data
                ON sp_data.start_service_provider_id = sp.service_provider_id
                AND sp_data.period_date_id = sp.period_date_id
            LEFT JOIN #{tables[:auth]} auth_data
                ON auth_data.service_provider_id = sp.service_provider_id
                AND auth_data.period_date_id = sp.period_date_id
            LEFT JOIN #{tables[:account]} acct_data
                ON acct_data.service_provider_id = sp.service_provider_id
                AND acct_data.period_date_id = sp.period_date_id
            ORDER BY sp.issuer;
      SQL
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
