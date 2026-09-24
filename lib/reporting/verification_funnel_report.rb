# frozen_string_literal: true

require 'csv'
require 'reporting/json_path_helper'

module Reporting
  class VerificationFunnelReport
    include Reporting::JsonPathHelper

    attr_reader :issuer_string, :time_range

    module Events
      # Stage 1 - Verification Demand (the funnel start / percentage denominator)
      VERIFICATION_DEMAND = 'IdV: doc auth welcome submitted'

      # Stage 2 - Document Authentication Success
      DOCUMENT_AUTHENTICATION_SUCCESS = 'IdV: doc auth ssn visited'

      # Stage 3 - Information Validation Success (phone page visit).
      # IRS is IAL2-only with no IPP/GPO, and phone pre-check is 0% in prod as 9/24/26, so
      # everyone hits this page. If phone pre-check goes above 0%, or this report
      # is reused for non-IRS SPs (IPP/GPO users skip the phone page), switch this
      # event to 'IdV: doc auth verify proofing results' AND re-add the
      # `AND #{bool_true('success_flag')}` gate on this stage in metrics_query
      # (plus the success_flag extraction in the CTE), since that event fires for
      # all users regardless of phone routing and carries a success property.
      INFORMATION_VALIDATION_SUCCESS = 'IdV: phone of record visited'

      # Stage 4 - Phone Verification Success.
      PHONE_VERIFICATION_SUCCESS = 'idv_enter_password_visited'

      # Stage 5 - Total Verified (reached the agency handoff).
      TOTAL_VERIFIED = 'User registration: agency handoff visited'

      def self.all_events
        constants.map { |constant| const_get(constant) }
      end
    end

    # @param [String] issuer_string
    # @param [Range<Time>] time_range
    def initialize(issuer_string:, time_range:)
      @issuer_string = issuer_string
      @time_range = time_range
    end

    def as_reports
      [
        {
          title: 'Definitions',
          table: definitions_table,
          filename: 'definitions',
        },
        {
          title: 'Overview',
          table: overview_table,
          filename: 'overview',
        },
        {
          title: 'Verification Funnel Metrics',
          table: funnel_table,
          filename: 'verification_funnel_metrics',
        },
      ]
    end

    def definitions_table
      [
        ['Metric', 'Definition'],
        ['Verification Demand',
         'The count of users who started the identity verification process'],
        ['Document Authentication Success',
         'Users who successfully completed document authentication'],
        ['Information Validation Success', 'Users who successfully validated their information'],
        ['Phone Verification Success', 'Users who successfully verified using their phone'],
        ['Verification Successes', 'Users who completed the entire process'],
        ['Verification Failures',
         'The count of users that did not complete the identity verification process'],
      ]
    end

    def overview_table
      [
        ['Report Timeframe', "#{time_range.begin} to #{time_range.end}"],
        ['Report Generated', Date.current.to_s],
        ['Issuer', issuer_string.to_s],
      ]
    end

    def funnel_table
      demand = metrics.fetch('verification_demand')
      doc_auth = metrics.fetch('document_authentication_success')
      info_validation = metrics.fetch('information_validation_success')
      phone = metrics.fetch('phone_verification_success')
      verified = metrics.fetch('total_verified')
      failures = demand - verified

      [
        ['Metric', 'Count', 'Rate'],
        ['Verification Demand', demand, safely_divide(demand, demand)],
        ['Document Authentication Success', doc_auth, safely_divide(doc_auth, demand)],
        ['Information Validation Success', info_validation, safely_divide(info_validation, demand)],
        ['Phone Verification Success', phone, safely_divide(phone, demand)],
        ['Verification Successes', verified, safely_divide(verified, demand)],
        ['Verification Failures', failures, safely_divide(failures, demand)],
      ]
    end

    private

    def metrics
      @metrics ||= begin
        row = connection.select_all(metrics_query).to_a.first || {}
        row.transform_values(&:to_i)
      end
    end

    def metrics_query
      facial_match = extract_json_path(
        'message', 'properties.sp_request.facial_match', type: 'BOOLEAN'
      )

      <<~SQL
        WITH base_events AS (
          SELECT
            user_id,
            name
          FROM logs.events
          WHERE service_provider = #{connection.quote(issuer_string)}
            AND cloudwatch_timestamp >= #{connection.quote(formatted_start_time)}
            AND cloudwatch_timestamp <= #{connection.quote(formatted_end_time)}
            AND #{facial_match} = TRUE
            AND user_id IS NOT NULL
            AND user_id <> ''
        )
        SELECT
          COUNT(DISTINCT CASE WHEN name = #{connection.quote(Events::VERIFICATION_DEMAND)}
                THEN user_id END) AS verification_demand,

          COUNT(DISTINCT CASE WHEN name = #{connection.quote(Events::DOCUMENT_AUTHENTICATION_SUCCESS)}
                THEN user_id END) AS document_authentication_success,

          COUNT(DISTINCT CASE WHEN name = #{connection.quote(Events::INFORMATION_VALIDATION_SUCCESS)}
                THEN user_id END) AS information_validation_success,

          COUNT(DISTINCT CASE WHEN name = #{connection.quote(Events::PHONE_VERIFICATION_SUCCESS)}
                THEN user_id END) AS phone_verification_success,

          COUNT(DISTINCT CASE WHEN name = #{connection.quote(Events::TOTAL_VERIFIED)}
                THEN user_id END) AS total_verified
        FROM base_events
      SQL
    end

    def formatted_start_time
      time_range.begin.strftime('%Y-%m-%dT%H:%M:%SZ')
    end

    def formatted_end_time
      time_range.end.strftime('%Y-%m-%dT%H:%M:%SZ')
    end

    def safely_divide(numerator, denominator)
      return 0.0 if denominator.to_f.zero?

      numerator.to_f / denominator.to_f
    end
  end
end
