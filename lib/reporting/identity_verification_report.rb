# frozen_string_literal: true

require 'csv'

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

    module Results
      IDV_FINAL_RESOLUTION_VERIFIED = 'IdV: final resolution - Verified'
      IDV_FINAL_RESOLUTION_FRAUD_REVIEW = 'IdV: final resolution - Fraud Review Pending'
      IDV_FINAL_RESOLUTION_GPO = 'IdV: final resolution - GPO Pending'
      # Note the lowercase v - this was there in IdP repo as well, so just copying
      IDV_FINAL_RESOLUTION_GPO_FRAUD_REVIEW =
        'Idv: final resolution - GPO Pending + Fraud Review Pending'
      IDV_FINAL_RESOLUTION_IN_PERSON = 'IdV: final resolution - In Person Proofing'
      IDV_FINAL_RESOLUTION_IN_PERSON_FRAUD_REVIEW =
        'IdV: final resolution - In Person Proofing + Fraud Review Pending'
      IDV_FINAL_RESOLUTION_GPO_IN_PERSON =
        'IdV: final resolution - GPO Pending + In Person Pending'
      IDV_FINAL_RESOLUTION_GPO_IN_PERSON_FRAUD_REVIEW =
        'IdV: final resolution - GPO Pending + In Person Pending + Fraud Review'

      IDV_REJECT_DOC_AUTH = 'IdV Reject: Doc Auth'
      IDV_REJECT_VERIFY = 'IdV Reject: Verify'
      IDV_REJECT_PHONE_FINDER = 'IdV Reject: Phone Finder'
    end

    # Because historically fraud-related events were not tagged with SP data,
    # we need to pull these out-of-band events *even if* they are marked as
    # pending fraud review. This allows us to attribute untagged fraud-related
    # events (by matching on user_id). We filter these events for counting
    # purposes, though.
    EVENTS_TO_IGNORE_IF_FRAUD_REVIEW_PENDING = [
      Events::GPO_VERIFICATION_SUBMITTED,
      Events::GPO_VERIFICATION_SUBMITTED_OLD,
      Events::USPS_ENROLLMENT_STATUS_UPDATED,
    ].to_set.freeze

    # For batch streaming redshift results
    BATCH_SIZE = 50_000

    def initialize(time_range:, data: nil)
      @time_range = time_range
      @data = data
    end

    def blanket_proofing_rate
      safely_divide(successfully_verified_users, idv_started)
    end

    def intent_proofing_rate
      safely_divide(successfully_verified_users, idv_doc_auth_welcome_submitted)
    end

    def actual_proofing_rate
      denom =
        idv_doc_auth_image_vendor_submitted +
        idv_doc_auth_socure_verification_data_requested

      safely_divide(successfully_verified_users, denom)
    end

    def industry_proofing_rate
      safely_divide(
        successfully_verified_users,
        successfully_verified_users + idv_doc_auth_rejected,
      )
    end

    def idv_final_resolution_rate
      safely_divide(idv_final_resolution, idv_started)
    end

    def idv_started
      @idv_started ||= (
        data[Events::IDV_DOC_AUTH_WELCOME] +
        data[Events::IDV_DOC_AUTH_GETTING_STARTED]
      ).count
    end

    def idv_doc_auth_welcome_submitted
      data[Events::IDV_DOC_AUTH_WELCOME_SUBMITTED].count
    end

    def idv_doc_auth_image_vendor_submitted
      data[Events::IDV_DOC_AUTH_IMAGE_UPLOAD].count
    end

    def idv_doc_auth_socure_verification_data_requested
      data[Events::IDV_DOC_AUTH_SOCURE_VERIFICATION_DATA].count
    end

    def idv_final_resolution
      data[Events::IDV_FINAL_RESOLUTION].count
    end

    def idv_final_resolution_verified
      data[Results::IDV_FINAL_RESOLUTION_VERIFIED].count
    end

    def idv_final_resolution_fraud_review
      data[Results::IDV_FINAL_RESOLUTION_FRAUD_REVIEW].count
    end

    def idv_final_resolution_gpo
      data[Results::IDV_FINAL_RESOLUTION_GPO].count
    end

    def idv_final_resolution_gpo_fraud_review
      data[Results::IDV_FINAL_RESOLUTION_GPO_FRAUD_REVIEW].count
    end

    def idv_final_resolution_in_person
      data[Results::IDV_FINAL_RESOLUTION_IN_PERSON].count
    end

    def idv_final_resolution_in_person_fraud_review
      data[Results::IDV_FINAL_RESOLUTION_IN_PERSON_FRAUD_REVIEW].count
    end

    def idv_final_resolution_gpo_in_person
      data[Results::IDV_FINAL_RESOLUTION_GPO_IN_PERSON].count
    end

    def idv_final_resolution_gpo_in_person_fraud_review
      data[Results::IDV_FINAL_RESOLUTION_GPO_IN_PERSON_FRAUD_REVIEW].count
    end

    def gpo_verification_submitted
      @gpo_verification_submitted ||= (
        data[Events::GPO_VERIFICATION_SUBMITTED] +
        data[Events::GPO_VERIFICATION_SUBMITTED_OLD]
      ).count
    end

    def usps_enrollment_status_updated
      data[Events::USPS_ENROLLMENT_STATUS_UPDATED].count
    end

    def fraud_review_passed
      passed_fraud_review_users.count
    end

    def idv_fraud_rejected
      did_not_pass_fraud_review_users.count
    end

    def idv_doc_auth_rejected
      @idv_doc_auth_rejected ||= (
        data[Results::IDV_REJECT_DOC_AUTH] +
        data[Results::IDV_REJECT_VERIFY] +
        data[Results::IDV_REJECT_PHONE_FINDER] -
        data[Results::IDV_FINAL_RESOLUTION_VERIFIED] -
        data[Results::IDV_FINAL_RESOLUTION_IN_PERSON]
      ).count
    end

    def successfully_verified_users
      @successfully_verified_users ||= (
        data[Results::IDV_FINAL_RESOLUTION_VERIFIED] +
        data[Events::USPS_ENROLLMENT_STATUS_UPDATED] +
        passed_fraud_review_users +
        data[Events::GPO_VERIFICATION_SUBMITTED] +
        data[Events::GPO_VERIFICATION_SUBMITTED_OLD]
      ).count
    end

    def passed_fraud_review_users
      data[Events::FRAUD_REVIEW_PASSED]
    end

    def did_not_pass_fraud_review_users
      data[Events::FRAUD_REVIEW_REJECT_AUTOMATIC] +
        data[Events::FRAUD_REVIEW_REJECT_MANUAL]
    end

    def verified_user_count
      @verified_user_count ||= connection.select_value(verified_user_count_query).to_i
    end

    # rubocop:disable Layout/LineLength
    # rubocop:disable Metrics/BlockLength
    # Turns query results into a hash keyed by event name; values are Sets of unique
    # user_ids for that event. Flag columns ('1'/'0') are computed in SQL (see #select_columns)
    # to avoid parsing the SUPER `message` blob in Ruby. This mirrors the prior CloudWatch
    # implementation, which precomputed the same flags in the Logs Insights query.
    # @return [Hash<String, Set<String>>]
    def data
      @data ||= begin
        users = Hash.new { |hash, event_name| hash[event_name] = Set.new }

        fetch_results.each do |row|
          event = row['name']
          user_id = row['user_id']
          next if user_id.blank?

          success = row['success'] == '1'
          fraud_review_pending = row['fraud_review_pending'] == '1'
          gpo_verification_pending = row['gpo_verification_pending'] == '1'
          in_person_verification_pending = row['in_person_verification_pending'] == '1'
          profile_not_pending = row['profile_not_pending'] == '1'
          doc_auth_failed_non_fraud = row['doc_auth_failed_non_fraud'] == '1'

          # NOTE: source-level filters (USPS passed=1, GPO success=1 and
          # !pending_in_person_enrollment, fraud-passed success=1) are applied in SQL
          # (see #where_clause), mirroring the old CloudWatch `| filter` clauses.
          # The per-user fraud-attribution filter below stays in Ruby because it
          # depends on the user's fraud state, not just the row.
          ignore_event_for_user =
            fraud_review_pending &&
            EVENTS_TO_IGNORE_IF_FRAUD_REVIEW_PENDING.include?(event)

          users[event] << user_id unless ignore_event_for_user

          case event
          when Events::IDV_FINAL_RESOLUTION
            # We count users for each final resolution outcome, considering the
            # combinations of pending states and fraud review status:
            #
            # | fraud_review_pending | gpo_verification_pending | in_person_verification_pending | IDV_FINAL_RESOLUTION_      |
            # |----------------------|--------------------------|--------------------------------|----------------------------|
            # | false                | false                    | false                          | VERIFIED                   |
            # | true                 | false                    | false                          | FRAUD_REVIEW               |
            # | false                | true                     | false                          | GPO                        |
            # | true                 | true                     | false                          | GPO_FRAUD_REVIEW           |
            # | false                | false                    | true                           | IN_PERSON                  |
            # | true                 | false                    | true                           | IN_PERSON_FRAUD_REVIEW     |
            # | false                | true                     | true                           | GPO_IN_PERSON              |
            # | true                 | true                     | true                           | GPO_IN_PERSON_FRAUD_REVIEW |
            #
            # `profile_not_pending` means all three pending flags are false AND there
            # is no deactivation_reason recorded.
            categorize_final_resolution(
              users: users,
              user_id: user_id,
              profile_not_pending: profile_not_pending,
              fraud_review_pending: fraud_review_pending,
              gpo_verification_pending: gpo_verification_pending,
              in_person_verification_pending: in_person_verification_pending,
            )
          when Events::IDV_DOC_AUTH_IMAGE_UPLOAD
            users[Results::IDV_REJECT_DOC_AUTH] << user_id if doc_auth_failed_non_fraud
          when Events::IDV_DOC_AUTH_VERIFY_RESULTS
            users[Results::IDV_REJECT_VERIFY] << user_id unless success
          when Events::IDV_PHONE_FINDER_RESULTS
            users[Results::IDV_REJECT_PHONE_FINDER] << user_id unless success
          end
        end

        users
      end
    end
    # rubocop:enable Metrics/BlockLength
    # rubocop:enable Layout/LineLength

    private

    # Streams results from Redshift in batches to avoid loading the full result set
    # (potentially tens of millions of rows) into memory at once
    def fetch_results
      return enum_for(:fetch_results) unless block_given?

      last_ts = nil
      last_id = nil

      loop do
        rows = connection.execute(page_query(last_ts, last_id)).to_a
        break if rows.empty?

        rows.each { |row| yield row }

        new_last = rows.last
        new_ts = new_last['cloudwatch_timestamp']
        new_id = new_last['id']

        # Safety guard: if the cursor did not advance, stop to avoid an infinite loop
        break if new_ts == last_ts && new_id == last_id

        last_ts = new_ts
        last_id = new_id
      end
    end

    # Builds one page of the keyset-paginated query
    # When last_ts/last_id are nil we're on the first page (no lower keyset bound)
    def page_query(last_ts, last_id)
      keyset_clause =
        if last_ts.nil?
          ''
        else
          <<~SQL
            AND (
              cloudwatch_timestamp > #{connection.quote(last_ts)}
              OR (
                cloudwatch_timestamp = #{connection.quote(last_ts)}
                AND id > #{connection.quote(last_id)}
              )
            )
          SQL
        end

      <<~SQL
        SELECT
          #{select_columns}
        FROM logs.events
        WHERE #{where_clause}
          #{keyset_clause}
        ORDER BY cloudwatch_timestamp ASC, id ASC
        LIMIT #{BATCH_SIZE}
      SQL
    end

    # All flag derivation happens here in SQL so Ruby never touches the SUPER `message`
    # blob. Each flag is emitted as '1'/'0' text to preserve the contract the Ruby `data`
    # method relies on (row['flag'] == '1'). Mirrors the old CloudWatch query's `fields`.
    def select_columns
      <<~SQL
        id,
        name,
        #{extract_json_path('message', 'properties.user_id')} AS user_id,
        cloudwatch_timestamp,

        CASE WHEN #{bool_true(success_path)} THEN '1' ELSE '0' END AS success,
        CASE WHEN #{fraud_review_pending_sql} THEN '1' ELSE '0' END AS fraud_review_pending,
        CASE WHEN #{bool_true(gpo_pending_path)} THEN '1' ELSE '0' END AS gpo_verification_pending,
        CASE WHEN #{bool_true(in_person_pending_path)} THEN '1' ELSE '0' END AS in_person_verification_pending,
        CASE WHEN #{deactivation_reason_present_sql} THEN '1' ELSE '0' END AS has_other_deactivation_reason,
        CASE WHEN #{doc_auth_failed_non_fraud_sql} THEN '1' ELSE '0' END AS doc_auth_failed_non_fraud,

        CASE
          WHEN NOT (#{fraud_review_pending_sql})
            AND #{bool_not_true(gpo_pending_path)}
            AND #{bool_not_true(in_person_pending_path)}
            AND NOT (#{deactivation_reason_present_sql})
          THEN '1' ELSE '0'
        END AS profile_not_pending
      SQL
    end

    # Source-level row filters, mirroring the old CloudWatch filter clauses
    def where_clause
      <<~SQL
        name IN (#{quoted(Events.all_events)})

        AND (
          name != #{connection.quote(Events::USPS_ENROLLMENT_STATUS_UPDATED)}
          OR #{bool_true(usps_passed_path)}
        )

        AND (
          name NOT IN (#{quoted(gpo_submission_events)})
          OR (
            #{bool_true(success_path)}
            AND #{bool_not_true(gpo_pending_in_person_path)}
          )
        )

        AND (
          name != #{connection.quote(Events::FRAUD_REVIEW_PASSED)}
          OR #{bool_true(success_path)}
        )

        AND cloudwatch_timestamp >= #{connection.quote(time_range.begin)}
        AND cloudwatch_timestamp <= #{connection.quote(time_range.end)}
      SQL
    end

    # The normalized fraud-review-pending expression, ported from the old CloudWatch
    # `normalized_fraud_review_pending`. NOTE: fraud_pending_reason is present on
    # 'IdV: final resolution' events; for GPO / IPP it will be set but the
    # fraud_review_pending flag is 0, so we must consider it independently.
    def fraud_review_pending_sql
      <<~SQL.strip
        COALESCE(
          (
            #{bool_true(fraud_review_pending_path)}
            OR #{fraud_pending_reason_path}::varchar IS NOT NULL
            OR #{bool_true(fraud_check_failed_path)}
            OR COALESCE(#{tmx_status_path}::varchar, '') IN ('threatmetrix_review', 'threatmetrix_reject')
          ),
          FALSE
        )
      SQL
    end

    # success = '0' AND doc_auth_result NOT IN ('Failed', 'Attention'),
    # mirroring the old CloudWatch `doc_auth_failed_non_fraud` field.
    def doc_auth_failed_non_fraud_sql
      <<~SQL.strip
        (
          #{bool_not_true(success_path)}
          AND COALESCE(#{doc_auth_result_path}::varchar, '') NOT IN ('Failed', 'Attention')
        )
      SQL
    end

    def deactivation_reason_present_sql
      "#{deactivation_reason_path}::varchar IS NOT NULL"
    end

    def success_path
      extract_json_path('message', 'properties.event_properties.success')
    end

    def fraud_review_pending_path
      extract_json_path(
        'message', 'properties.event_properties.fraud_review_pending'
      )
    end

    def fraud_pending_reason_path
      extract_json_path('message', 'properties.event_properties.fraud_pending_reason')
    end

    def fraud_check_failed_path
      extract_json_path(
        'message', 'properties.event_properties.fraud_check_failed'
      )
    end

    def tmx_status_path
      extract_json_path('message', 'properties.event_properties.tmx_status')
    end

    def gpo_pending_path
      extract_json_path(
        'message', 'properties.event_properties.gpo_verification_pending'
      )
    end

    def in_person_pending_path
      extract_json_path(
        'message', 'properties.event_properties.in_person_verification_pending'
      )
    end

    def deactivation_reason_path
      extract_json_path('message', 'properties.event_properties.deactivation_reason')
    end

    def doc_auth_result_path
      extract_json_path('message', 'properties.event_properties.doc_auth_result')
    end

    def usps_passed_path
      extract_json_path('message', 'properties.event_properties.passed')
    end

    def gpo_pending_in_person_path
      extract_json_path(
        'message', 'properties.event_properties.pending_in_person_enrollment'
      )
    end

    def gpo_submission_events
      [
        Events::GPO_VERIFICATION_SUBMITTED,
        Events::GPO_VERIFICATION_SUBMITTED_OLD,
      ]
    end

    def categorize_final_resolution(
      users:,
      user_id:,
      profile_not_pending:,
      fraud_review_pending:,
      gpo_verification_pending:,
      in_person_verification_pending:
    )
      users[Results::IDV_FINAL_RESOLUTION_VERIFIED] << user_id if profile_not_pending

      if !gpo_verification_pending && !in_person_verification_pending
        users[Results::IDV_FINAL_RESOLUTION_FRAUD_REVIEW] << user_id if fraud_review_pending
      elsif gpo_verification_pending && !in_person_verification_pending
        users[Results::IDV_FINAL_RESOLUTION_GPO] << user_id unless fraud_review_pending
        users[Results::IDV_FINAL_RESOLUTION_GPO_FRAUD_REVIEW] << user_id if fraud_review_pending
      elsif !gpo_verification_pending && in_person_verification_pending
        users[Results::IDV_FINAL_RESOLUTION_IN_PERSON] << user_id unless fraud_review_pending
        if fraud_review_pending
          users[Results::IDV_FINAL_RESOLUTION_IN_PERSON_FRAUD_REVIEW] << user_id
        end
      elsif gpo_verification_pending && in_person_verification_pending
        users[Results::IDV_FINAL_RESOLUTION_GPO_IN_PERSON] << user_id unless fraud_review_pending
        if fraud_review_pending
          users[Results::IDV_FINAL_RESOLUTION_GPO_IN_PERSON_FRAUD_REVIEW] << user_id
        end
      end
    end

    def verified_user_count_query
      <<~SQL
        SELECT COUNT(*)
        FROM idp.profiles
        WHERE active = TRUE
          AND verified_at <= #{connection.quote(time_range.end.end_of_day)}
      SQL
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

    # SUPER boolean fields: Redshift coerces them in an `= TRUE` equality
    # comparison, but rejects IS TRUE and turns ::varchar casts into NULL.
    # COALESCE(..., FALSE) makes absent/NULL keys read as not-true.
    def bool_true(path)
      "COALESCE(#{path} = TRUE, FALSE)"
    end

    def bool_not_true(path)
      "NOT COALESCE(#{path} = TRUE, FALSE)"
    end
  end
end
