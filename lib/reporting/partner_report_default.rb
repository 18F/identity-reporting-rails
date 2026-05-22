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
      'monthly' => 'marts.sp_partner_report_metrics_monthly',
      'weekly' => 'marts.sp_partner_report_metrics_weekly',
      'daily' => 'marts.sp_partner_report_metrics_daily',
    }.freeze

    attr_reader :report_date, :report_cadence, :included_issuers, :excluded_issuers

    def initialize(
      report_date:,
      report_cadence: 'monthly',
      included_issuers: nil,
      excluded_issuers: []
    )
      @report_date = report_date.to_s # Ensure report date is a string
      @report_cadence = report_cadence
      @included_issuers = included_issuers
      @excluded_issuers = excluded_issuers

      unless ['monthly', 'weekly', 'daily'].include?(@report_cadence)
        raise ArgumentError, "Invalid report_cadence: #{@report_cadence}. "\
                             "Must be one of: monthly, weekly, daily"
      end
      if @included_issuers&.any? && @excluded_issuers&.any?
        raise ArgumentError, 'Cannot specify both included_issuers and excluded_issuers'
      end
    end

    def self.get_period_date_from_report_date(report_date:, cadence: 'monthly')
      # Given a date, retrieves the corresponding date for the start of its month/week/day
      # Returns string in format 'YYYY-MM-DD', raises on failure
      unless ['monthly', 'weekly', 'daily'].include?(cadence)
        raise ArgumentError, "Invalid cadence: #{cadence}"
      end

      begin
        Date.parse(report_date.to_s)
      rescue ArgumentError => e
        raise ArgumentError, "Invalid date format for report_date: #{report_date} - #{e.message}"
      end

      query = <<~SQL
        SELECT 
          CASE 
            WHEN $1 = 'monthly' THEN TO_CHAR(month_start_date_actual, 'YYYY-MM-DD')
            WHEN $1 = 'weekly' THEN TO_CHAR(week_start_date_actual, 'YYYY-MM-DD')
            WHEN $1 = 'daily' THEN TO_CHAR(cal.date_actual, 'YYYY-MM-DD')
          END AS period_date_actual
        FROM marts.calendar cal
        WHERE cal.calendar_id = TO_CHAR($2::date, 'YYYYMMDD')::int;
      SQL

      result = DataWarehouseApplicationRecord.connection.exec_query(
        query,
        'get_period_date',
        [cadence, report_date],
      ).first

      if result.nil?
        raise StandardError, "No calendar entry found for report_date: #{report_date}"
      end

      period_date = result['period_date_actual']
      if period_date.nil?
        raise StandardError, "No period_date_actual found for report_date: #{report_date}"
      end

      period_date
    end

    def generate_issuer_mapping
      # This maps SP ID to issuer string
      raw_data = fetch_issuer_mapping_data
      format_issuer_mapping(raw_data)
    end

    # Returns data hash: { issuer => json_data }
    def generate_reports
      raw_data = fetch_bulk_data
      format_by_issuer(raw_data)
    end

    private

    def fetch_issuer_mapping_data
      DataWarehouseApplicationRecord.connection.execute(issuer_mapping_query).to_a
    end

    def issuer_mapping_query
      <<~SQL
        SELECT issuer, id
        FROM idp.service_providers
        WHERE issuer IS NOT NULL
          AND TRIM(issuer) <> ''
          AND id IS NOT NULL
        ORDER BY issuer;
      SQL
    end

    def format_issuer_mapping(raw_data)
      if raw_data.empty?
        Rails.logger.warn 'No service providers found in idp.service_providers'
        return {}
      end

      result = {}
      raw_data.each do |row|
        issuer = row['issuer']
        id = row['id']

        if result.key?(issuer)
          Rails.logger.error "Duplicate issuer found in idp.service_providers: #{issuer}."\
                             " Keeping first id."
          next
        end

        begin
          result[issuer] = { id: Integer(id) }
        rescue ArgumentError, TypeError
          Rails.logger.error "Invalid id value for issuer #{issuer}: #{id.inspect}. Skipping row."
        end
      end

      result
    end

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
      required_fields = %w[issuer service_provider_name period_date]
      missing_fields = required_fields.select { |field| row[field].nil? }

      if missing_fields.any?
        raise StandardError, "Missing required fields: #{missing_fields.join(', ')}"
      end

      {
        issuer: row['issuer'],
        provider_information: {
          service_provider_name: row['service_provider_name'],
          agency_name: row['agency_name'],
          service_provider_id: row['service_provider_id'],
        },
        report_information: {
          period_start_date: row['period_date'],
          period_calendar_id: row['period_date_id'],
          report_cadence: report_cadence,
          report_date: @report_date,
        },
        data: build_data_section(row),
      }
    end

    INTEGER_DATA_FIELDS = %w[
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
    ].freeze

    def build_data_section(row)
      INTEGER_DATA_FIELDS.each_with_object({}) do |field, hash|
        value = row[field]
        if value.nil? || value.to_s.strip.empty?
          hash[field.to_sym] = nil
        else
          begin
            hash[field.to_sym] = Integer(value)
          rescue ArgumentError, TypeError => e
            Rails.logger.error "Failed to convert '#{value}' " \
                              "to integer for field #{field}: #{e.message}"
            hash[field.to_sym] = nil
          end
        end
      end
    end

    def fetch_bulk_data
      DataWarehouseApplicationRecord.connection.execute(bulk_query).to_a
    end

    def period_date
      self.class.get_period_date_from_report_date(
        report_date: @report_date,
        cadence: @report_cadence,
      )
    end

    def bulk_query
      table = CADENCE_TABLES[report_cadence]
      <<~SQL
        SELECT *
        FROM #{table}
        WHERE period_date = '#{period_date}'
          AND issuer IN (
            SELECT issuer
            FROM marts.service_providers
            WHERE iaa_end_date > '#{@report_date}'::date
              AND '#{@report_date}'::date >= launch_date
          )
          #{issuer_filter_clause}
        ORDER BY issuer;
      SQL
    end

    def issuer_filter_clause
      if @included_issuers&.any?
        sanitized = @included_issuers.map { |i| ActiveRecord::Base.connection.quote(i) }
        "AND issuer IN (#{sanitized.join(', ')})"
      elsif @excluded_issuers.any?
        sanitized = @excluded_issuers.map { |i| ActiveRecord::Base.connection.quote(i) }
        "AND issuer NOT IN (#{sanitized.join(', ')})"
      else
        ''
      end
    end
  end
end
