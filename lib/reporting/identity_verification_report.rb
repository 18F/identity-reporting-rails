# frozen_string_literal: true

require 'csv'
require 'json'

module Reporting
  class IdentityVerificationReport
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

    EVENTS_TO_IGNORE_IF_FRAUD_REVIEW_PENDING = [
      Events::GPO_VERIFICATION_SUBMITTED,
      Events::GPO_VERIFICATION_SUBMITTED_OLD,
      Events::USPS_ENROLLMENT_STATUS_UPDATED,
    ].to_set.freeze

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

    def idv_final_resolution_in_person
      data[Results::IDV_FINAL_RESOLUTION_IN_PERSON].count
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

    # rubocop:disable Metrics/BlockLength
    def data
      @data ||= begin
        users = Hash.new { |hash, key| hash[key] = Set.new }

        fetch_results.each do |row|
          event = row['name']
          user_id = row['user_id']
          next if user_id.blank?

          event_properties = extract_event_properties(row['message'])
          success = event_success(row, event_properties)

          fraud_review_pending = fraud_review_pending?(event_properties)
          gpo_verification_pending =
            true_value?(event_properties['gpo_verification_pending'])
          in_person_verification_pending =
            true_value?(event_properties['in_person_verification_pending'])
          has_other_deactivation_reason =
            event_properties['deactivation_reason'].present?

          profile_not_pending =
            !fraud_review_pending &&
            !gpo_verification_pending &&
            !in_person_verification_pending &&
            !has_other_deactivation_reason

          next if skip_event?(event, event_properties, success)

          ignore_event_for_user =
            fraud_review_pending &&
            EVENTS_TO_IGNORE_IF_FRAUD_REVIEW_PENDING.include?(event)

          users[event] << user_id unless ignore_event_for_user

          case event
          when Events::IDV_FINAL_RESOLUTION
            categorize_final_resolution(
              users: users,
              user_id: user_id,
              profile_not_pending: profile_not_pending,
              fraud_review_pending: fraud_review_pending,
              gpo_verification_pending: gpo_verification_pending,
              in_person_verification_pending: in_person_verification_pending,
            )
          when Events::IDV_DOC_AUTH_IMAGE_UPLOAD
            if doc_auth_failed_non_fraud?(event_properties, success)
              users[Results::IDV_REJECT_DOC_AUTH] << user_id
            end
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

    private

    def fetch_results
      connection.execute(query).to_a
    end

    def query
      <<~SQL
        SELECT
          name,
          user_id,
          success,
          message
        FROM logs.events
        WHERE name IN (#{quoted(Events.all_events)})
          AND cloudwatch_timestamp BETWEEN #{connection.quote(time_range.begin)}
          AND #{connection.quote(time_range.end)}
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

    def skip_event?(event, event_properties, success)
      if event == Events::USPS_ENROLLMENT_STATUS_UPDATED
        return true unless true_value?(event_properties['passed'])
      end

      if [
        Events::GPO_VERIFICATION_SUBMITTED,
        Events::GPO_VERIFICATION_SUBMITTED_OLD,
      ].include?(event)
        return true unless success
        return true if true_value?(event_properties['pending_in_person_enrollment'])
      end

      event == Events::FRAUD_REVIEW_PASSED && !success
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

    def quoted(values)
      values.map { |value| connection.quote(value) }.join(', ')
    end

    def connection
      DataWarehouseApplicationRecord.connection
    end

    def extract_event_properties(message)
      payload =
        case message
        when Hash
          message
        when String
          JSON.parse(message)
        else
          {}
        end

      payload.fetch('properties', {}).fetch('event_properties', {}) || {}
    rescue JSON::ParserError
      {}
    end

    def event_success(row, event_properties)
      if event_properties.key?('success')
        true_value?(event_properties['success'])
      else
        true_value?(row['success'])
      end
    end

    def true_value?(value)
      value == true ||
        value.to_s == '1' ||
        value.to_s.casecmp('true').zero?
    end

    def fraud_review_pending?(event_properties)
      true_value?(event_properties['fraud_review_pending']) ||
        event_properties['fraud_pending_reason'].present? ||
        true_value?(event_properties['fraud_check_failed']) ||
        %w[threatmetrix_review threatmetrix_reject].include?(
          event_properties['tmx_status'],
        )
    end

    def doc_auth_failed_non_fraud?(event_properties, success)
      !success && !%w[Failed Attention].include?(event_properties['doc_auth_result'].to_s)
    end

    def safely_divide(numerator, denominator)
      return 0.0 if denominator.to_f.zero?

      numerator.to_f / denominator.to_f
    end
  end
end
