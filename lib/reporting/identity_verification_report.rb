# frozen_string_literal: true

require 'reporting/json_path_helper'

module Reporting
  class IdentityVerificationReport
    include Reporting::JsonPathHelper

    attr_reader :time_range

    module Events
      IDV_DOC_AUTH_WELCOME = 'IdV: doc auth welcome visited'
      IDV_DOC_AUTH_WELCOME_SUBMITTED = 'IdV: doc auth welcome submitted'
      IDV_DOC_AUTH_GETTING_STARTED = 'IdV: doc auth getting_started visited'
      IDV_DOC_AUTH_IMAGE_UPLOAD = 'IdV: doc auth image upload vendor submitted'
      IDV_DOC_AUTH_SOCURE_VERIFICATION_DATA = 'idv_socure_verification_data_requested'
      IDV_DOC_AUTH_VERIFY_RESULTS = 'IdV: doc auth verify proofing results'
      IDV_PHONE_FINDER_RESULTS = 'IdV: phone confirmation vendor'
      IDV_FINAL_RESOLUTION = 'IdV: final resolution'
      GPO_VERIFICATION_SUBMITTED = 'IdV: enter verify by mail code submitted'
      GPO_VERIFICATION_SUBMITTED_OLD = 'IdV: GPO verification submitted'
      USPS_ENROLLMENT_STATUS_UPDATED = 'GetUspsProofingResultsJob: Enrollment status updated'
      FRAUD_REVIEW_PASSED = 'Fraud: Profile review passed'
      FRAUD_REVIEW_REJECT_AUTOMATIC = 'Fraud: Automatic Fraud Rejection'
      FRAUD_REVIEW_REJECT_MANUAL = 'Fraud: Profile review rejected'

      def self.all_events
        constants.map { |constant| const_get(constant) }
      end
    end

    def initialize(time_range:)
      @time_range = time_range
    end

    # USED (both tables)
    def idv_started
      metrics.fetch('idv_started')
    end

    # USED (proofing_rate_table)
    def idv_doc_auth_welcome_submitted
      metrics.fetch('welcome_submitted')
    end

    # USED (proofing_rate_table)
    def idv_doc_auth_image_vendor_submitted
      metrics.fetch('image_submitted')
    end

    # USED (proofing_rate_table)
    def idv_doc_auth_socure_verification_data_requested
      metrics.fetch('socure')
    end

    # USED (condensed_idv_table)
    def idv_final_resolution
      metrics.fetch('idv_final_resolution')
    end

    # USED (both tables)
    def successfully_verified_users
      metrics.fetch('successfully_verified_users')
    end

    # USED (proofing_rate_table)
    def idv_doc_auth_rejected
      metrics.fetch('idv_doc_auth_rejected')
    end

    # USED (proofing_rate_table)
    def idv_fraud_rejected
      metrics.fetch('fraud_rejected')
    end

    # --- final-resolution sub-buckets (UNUSED) ---
    def idv_final_resolution_verified
      metrics.fetch('fr_verified_users')
    end

    def idv_final_resolution_in_person
      metrics.fetch('fr_in_person_users')
    end

    def idv_final_resolution_fraud_review
      metrics.fetch('fr_fraud_review_users')
    end

    def idv_final_resolution_gpo
      metrics.fetch('fr_gpo_users')
    end

    def idv_final_resolution_gpo_fraud_review
      metrics.fetch('fr_gpo_fraud_review_users')
    end

    def idv_final_resolution_in_person_fraud_review
      metrics.fetch('fr_in_person_fraud_review_users')
    end

    def idv_final_resolution_gpo_in_person
      metrics.fetch('fr_gpo_in_person_users')
    end

    def idv_final_resolution_gpo_in_person_fraud_review
      metrics.fetch('fr_gpo_in_person_fraud_review_users')
    end

    # UNUSED individual reject buckets (UNUSED - debugging/visibility)
    def reject_doc_auth
      metrics.fetch('reject_doc_auth')
    end

    def reject_verify
      metrics.fetch('reject_verify')
    end

    def reject_phone
      metrics.fetch('reject_phone')
    end

    # --- rates ---

    # USED (both tables)
    def blanket_proofing_rate
      safely_divide(successfully_verified_users, idv_started)
    end

    # USED (proofing_rate_table)
    def intent_proofing_rate
      safely_divide(successfully_verified_users, idv_doc_auth_welcome_submitted)
    end

    # USED (proofing_rate_table)
    def actual_proofing_rate
      denom =
        idv_doc_auth_image_vendor_submitted +
        idv_doc_auth_socure_verification_data_requested
      safely_divide(successfully_verified_users, denom)
    end

    # USED (proofing_rate_table)
    def industry_proofing_rate
      safely_divide(
        successfully_verified_users,
        successfully_verified_users + idv_doc_auth_rejected,
      )
    end

    # USED (condensed_idv_table)
    def idv_final_resolution_rate
      safely_divide(idv_final_resolution, idv_started)
    end

    # USED (condensed_idv_table)
    # A prod table direct query, not derived from logs
    def verified_user_count
      @verified_user_count ||= connection.select_value(verified_user_count_query).to_i
    end

    private

    def metrics
      @metrics ||= begin
        row = connection.select_all(metrics_query).to_a.first || {}
        row.transform_values(&:to_i)
      end
    end

    def metrics_query
      <<~SQL
        WITH flagged AS (
          SELECT
            e.name,
            e.user_id,

            #{bool_true(extract_json_path('message', 'properties.event_properties.success', type: 'BOOLEAN'))} AS success,

            COALESCE(
              #{bool_true(extract_json_path('message', 'properties.event_properties.fraud_review_pending', type: 'BOOLEAN'))}
              OR #{extract_json_path('message', 'properties.event_properties.fraud_pending_reason')} IS NOT NULL
              OR #{bool_true(extract_json_path('message', 'properties.event_properties.fraud_check_failed', type: 'BOOLEAN'))}
              OR COALESCE(#{extract_json_path('message', 'properties.event_properties.tmx_status')}, '')
                   IN ('threatmetrix_review', 'threatmetrix_reject'),
              FALSE
            ) AS fraud_review_pending,

            #{bool_true(extract_json_path('message', 'properties.event_properties.gpo_verification_pending', type: 'BOOLEAN'))} AS gpo_pending,
            #{bool_true(extract_json_path('message', 'properties.event_properties.in_person_verification_pending', type: 'BOOLEAN'))} AS in_person_pending,
            (#{extract_json_path('message', 'properties.event_properties.deactivation_reason')} IS NOT NULL) AS has_deactivation_reason,

            (
              #{bool_not_true(extract_json_path('message', 'properties.event_properties.success', type: 'BOOLEAN'))}
              AND COALESCE(#{extract_json_path('message', 'properties.event_properties.doc_auth_result')}, '')
                    NOT IN ('Failed', 'Attention')
            ) AS doc_auth_failed_non_fraud

          FROM logs.events e
          WHERE e.cloudwatch_timestamp >= #{connection.quote(time_range.begin)}
            AND e.cloudwatch_timestamp <= #{connection.quote(time_range.end)}
            AND e.user_id IS NOT NULL
            AND e.user_id <> ''
            AND e.name IN (#{quoted(Events.all_events)})
            AND (
              e.name != #{connection.quote(Events::USPS_ENROLLMENT_STATUS_UPDATED)}
              OR #{bool_true(extract_json_path('message', 'properties.event_properties.passed', type: 'BOOLEAN'))}
            )
            AND (
              e.name NOT IN (#{quoted(gpo_submission_events)})
              OR (
                #{bool_true(extract_json_path('message', 'properties.event_properties.success', type: 'BOOLEAN'))}
                AND #{bool_not_true(extract_json_path('message', 'properties.event_properties.pending_in_person_enrollment', type: 'BOOLEAN'))}
              )
            )
            AND (
              e.name != #{connection.quote(Events::FRAUD_REVIEW_PASSED)}
              OR #{bool_true(extract_json_path('message', 'properties.event_properties.success', type: 'BOOLEAN'))}
            )
        ),

        counted AS (
          SELECT
            f.*,
            CASE
              WHEN f.fraud_review_pending
                AND f.name IN (
                  #{connection.quote(Events::GPO_VERIFICATION_SUBMITTED)},
                  #{connection.quote(Events::GPO_VERIFICATION_SUBMITTED_OLD)},
                  #{connection.quote(Events::USPS_ENROLLMENT_STATUS_UPDATED)}
                )
              THEN FALSE ELSE TRUE
            END AS keep_for_event_bucket,
            (
              f.name = #{connection.quote(Events::IDV_FINAL_RESOLUTION)}
              AND NOT f.fraud_review_pending
              AND NOT f.gpo_pending
              AND NOT f.in_person_pending
              AND NOT f.has_deactivation_reason
            ) AS fr_verified,
            (
              f.name = #{connection.quote(Events::IDV_FINAL_RESOLUTION)}
              AND NOT f.gpo_pending
              AND f.in_person_pending
              AND NOT f.fraud_review_pending
            ) AS fr_in_person
          FROM flagged f
        ),

        verified_or_in_person AS (
          SELECT DISTINCT user_id
          FROM counted
          WHERE fr_verified OR fr_in_person
        ),

        reject_union AS (
          SELECT DISTINCT user_id
          FROM counted
          WHERE (name = #{connection.quote(Events::IDV_DOC_AUTH_IMAGE_UPLOAD)} AND doc_auth_failed_non_fraud)
             OR (name = #{connection.quote(Events::IDV_DOC_AUTH_VERIFY_RESULTS)} AND NOT success)
             OR (name = #{connection.quote(Events::IDV_PHONE_FINDER_RESULTS)} AND NOT success)
        )

        SELECT
          COUNT(DISTINCT CASE WHEN keep_for_event_bucket
                AND name IN (#{connection.quote(Events::IDV_DOC_AUTH_WELCOME)},
                             #{connection.quote(Events::IDV_DOC_AUTH_GETTING_STARTED)})
                THEN user_id END) AS idv_started,

          COUNT(DISTINCT CASE WHEN keep_for_event_bucket
                AND name = #{connection.quote(Events::IDV_DOC_AUTH_WELCOME_SUBMITTED)}
                THEN user_id END) AS welcome_submitted,

          COUNT(DISTINCT CASE WHEN keep_for_event_bucket
                AND name = #{connection.quote(Events::IDV_DOC_AUTH_IMAGE_UPLOAD)}
                THEN user_id END) AS image_submitted,

          COUNT(DISTINCT CASE WHEN keep_for_event_bucket
                AND name = #{connection.quote(Events::IDV_DOC_AUTH_SOCURE_VERIFICATION_DATA)}
                THEN user_id END) AS socure,

          COUNT(DISTINCT CASE WHEN keep_for_event_bucket
                AND name = #{connection.quote(Events::IDV_FINAL_RESOLUTION)}
                THEN user_id END) AS idv_final_resolution,

          COUNT(DISTINCT CASE WHEN fr_verified  THEN user_id END) AS fr_verified_users,
          COUNT(DISTINCT CASE WHEN fr_in_person THEN user_id END) AS fr_in_person_users,

          COUNT(DISTINCT CASE WHEN name = #{connection.quote(Events::IDV_FINAL_RESOLUTION)}
                AND NOT gpo_pending AND NOT in_person_pending AND fraud_review_pending
                THEN user_id END) AS fr_fraud_review_users,

          COUNT(DISTINCT CASE WHEN name = #{connection.quote(Events::IDV_FINAL_RESOLUTION)}
                AND gpo_pending AND NOT in_person_pending AND NOT fraud_review_pending
                THEN user_id END) AS fr_gpo_users,

          COUNT(DISTINCT CASE WHEN name = #{connection.quote(Events::IDV_FINAL_RESOLUTION)}
                AND gpo_pending AND NOT in_person_pending AND fraud_review_pending
                THEN user_id END) AS fr_gpo_fraud_review_users,

          COUNT(DISTINCT CASE WHEN name = #{connection.quote(Events::IDV_FINAL_RESOLUTION)}
                AND NOT gpo_pending AND in_person_pending AND fraud_review_pending
                THEN user_id END) AS fr_in_person_fraud_review_users,

          COUNT(DISTINCT CASE WHEN name = #{connection.quote(Events::IDV_FINAL_RESOLUTION)}
                AND gpo_pending AND in_person_pending AND NOT fraud_review_pending
                THEN user_id END) AS fr_gpo_in_person_users,

          COUNT(DISTINCT CASE WHEN name = #{connection.quote(Events::IDV_FINAL_RESOLUTION)}
                AND gpo_pending AND in_person_pending AND fraud_review_pending
                THEN user_id END) AS fr_gpo_in_person_fraud_review_users,

          COUNT(DISTINCT CASE WHEN name = #{connection.quote(Events::IDV_DOC_AUTH_IMAGE_UPLOAD)}
                AND doc_auth_failed_non_fraud THEN user_id END) AS reject_doc_auth,
          COUNT(DISTINCT CASE WHEN name = #{connection.quote(Events::IDV_DOC_AUTH_VERIFY_RESULTS)}
                AND NOT success THEN user_id END) AS reject_verify,
          COUNT(DISTINCT CASE WHEN name = #{connection.quote(Events::IDV_PHONE_FINDER_RESULTS)}
                AND NOT success THEN user_id END) AS reject_phone,

          COUNT(DISTINCT CASE
                WHEN fr_verified
                  OR (name = #{connection.quote(Events::USPS_ENROLLMENT_STATUS_UPDATED)} AND keep_for_event_bucket)
                  OR name = #{connection.quote(Events::FRAUD_REVIEW_PASSED)}
                  OR (name IN (#{connection.quote(Events::GPO_VERIFICATION_SUBMITTED)},
                              #{connection.quote(Events::GPO_VERIFICATION_SUBMITTED_OLD)}) AND keep_for_event_bucket)
                THEN user_id END) AS successfully_verified_users,

          COUNT(DISTINCT CASE
                WHEN name IN (#{connection.quote(Events::FRAUD_REVIEW_REJECT_AUTOMATIC)},
                              #{connection.quote(Events::FRAUD_REVIEW_REJECT_MANUAL)})
                THEN user_id END) AS fraud_rejected,

          (
            SELECT COUNT(*)
            FROM reject_union r
            WHERE NOT EXISTS (
              SELECT 1 FROM verified_or_in_person v WHERE v.user_id = r.user_id
            )
          ) AS idv_doc_auth_rejected

        FROM counted
      SQL
    end

    def verified_user_count_query
      <<~SQL
        SELECT COUNT(*)
        FROM idp.profiles
        WHERE active = TRUE
          AND verified_at <= #{connection.quote(time_range.end.end_of_day)}
      SQL
    end

    def gpo_submission_events
      [
        Events::GPO_VERIFICATION_SUBMITTED,
        Events::GPO_VERIFICATION_SUBMITTED_OLD,
      ]
    end

    def quoted(values)
      values.map { |value| connection.quote(value) }.join(', ')
    end

    def connection
      DataWarehouseApplicationRecord.connection
    end

    def safely_divide(numerator, denominator)
      return 0.0 if denominator.to_f.zero?

      numerator.to_f / denominator.to_f
    end

    def bool_true(path)
      "COALESCE(#{path} = TRUE, FALSE)"
    end

    def bool_not_true(path)
      "NOT COALESCE(#{path} = TRUE, FALSE)"
    end
  end
end
