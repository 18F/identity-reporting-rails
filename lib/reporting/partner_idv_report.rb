# frozen_string_literal: true

module Reporting
  class PartnerIdvReport
    REDSHIFT_QUERY = <<~SQL
      WITH params AS (
          SELECT
              :month_start_calendar_id as month_start_calendar_id,
              :service_provider_id as service_provider_id
      )
      SELECT
          sp_ref.issuer,
          sp_ref.service_provider_name,
          sp_ref.agency_name,
          sp_data.start_service_provider_id,
          sp_data.month_start_date_actual,
          sp_data.month_start_calendar_id,
          sp_data.count_inauthentic_doc,
          sp_data.count_facial_mismatch,
          sp_data.count_invalid_attributes_dl_dos,
          sp_data.count_ssn_dob_deceased,
          sp_data.count_address_other_not_found,
          sp_data.count_pending_lg99_likely_fraud,
          sp_data.count_stayed_blocked,
          sp_data.count_fraud_alert,
          sp_data.count_suspicious_phone,
          sp_data.count_lack_phone_ownership,
          sp_data.count_wrong_phone_type,
          sp_data.count_blocked_by_ipp_fraud,
          sp_data.count_pass_via_lg99,
          sp_data.count_pass_online_finalization,
          sp_data.count_pass_ipp_online_portion,
          sp_data.count_pass_via_letter,
          sp_data.count_doc_auth_ux,
          sp_data.count_selfie_ux,
          sp_data.count_dob_incorrect,
          sp_data.count_ssn_incorrect,
          sp_data.count_identity_not_found,
          sp_data.count_friction_during_otp,
          sp_data.count_doc_auth_technical_issue,
          sp_data.count_resolution_technical_issues,
          sp_data.count_doc_auth_processing_issue
      FROM marts.sp_idv_outcomes_monthly sp_data
      INNER JOIN marts.service_providers sp_ref
          ON sp_data.start_service_provider_id = sp_ref.service_provider_id
      CROSS JOIN params p
      WHERE sp_data.start_service_provider_id = p.service_provider_id
          AND sp_data.month_start_calendar_id = p.month_start_calendar_id
    SQL

    attr_reader :service_provider_id, :month_start_calendar_id, :connection

    # @param [Integer] service_provider_id
    # @param [Integer] month_start_calendar_id
    # @param [#execute,#quote] connection DB connection (ActiveRecord connection, etc)
    def initialize(service_provider_id:, month_start_calendar_id:, connection:)
      @service_provider_id = service_provider_id
      @month_start_calendar_id = month_start_calendar_id
      @connection = connection
    end

    # @return [String] JSON string of query results
    def results_json
      JSON.generate(fetch_results)
    end

    # @return [Array<Hash>] array of hashes with column names as keys
    def fetch_results
      connection.execute(sql).to_a
    end

    private

    def sql
      # Substitute safely using the adapter's quoting
      REDSHIFT_QUERY.
        gsub(':service_provider_id', connection.quote(service_provider_id.to_i)).
        gsub(':month_start_calendar_id', connection.quote(month_start_calendar_id.to_i))
    end
  end
end
