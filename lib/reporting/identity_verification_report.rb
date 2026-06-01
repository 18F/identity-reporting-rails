# frozen_string_literal: true

require 'csv'
require 'json'
require 'set'

module Reporting
  class IdentityVerificationReport
    attr_reader :issuers, :time_range

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
        constants.map { |c| const_get(c) }
      end
    end

    module Results
      IDV_FINAL_RESOLUTION_VERIFIED = 'IdV: final resolution - Verified'
      IDV_FINAL_RESOLUTION_FRAUD_REVIEW = 'IdV: final resolution - Fraud Review Pending'
      IDV_FINAL_RESOLUTION_GPO = 'IdV: final resolution - GPO Pending'
      IDV_FINAL_RESOLUTION_GPO_FRAUD_REVIEW = 'Idv: final resolution - GPO Pending + Fraud Review Pending'
      IDV_FINAL_RESOLUTION_IN_PERSON = 'IdV: final resolution - In Person Proofing'
      IDV_FINAL_RESOLUTION_IN_PERSON_FRAUD_REVIEW = 'IdV: final resolution - In Person Proofing + Fraud Review Pending'
      IDV_FINAL_RESOLUTION_GPO_IN_PERSON = 'IdV: final resolution - GPO Pending + In Person Pending'
      IDV_FINAL_RESOLUTION_GPO_IN_PERSON_FRAUD_REVIEW = 'IdV: final resolution - GPO Pending + In Person Pending + Fraud Review'

      IDV_REJECT_DOC_AUTH = 'IdV Reject: Doc Auth'
      IDV_REJECT_VERIFY = 'IdV Reject: Verify'
      IDV_REJECT_PHONE_FINDER = 'IdV Reject: Phone Finder'
    end

    EVENTS_TO_IGNORE_IF_FRAUD_REVIEW_PENDING = [
      Events::GPO_VERIFICATION_SUBMITTED,
      Events::GPO_VERIFICATION_SUBMITTED_OLD,
      Events::USPS_ENROLLMENT_STATUS_UPDATED,
    ].to_set.freeze

    FRAUD_EVENT_NAMES = [
      Events::FRAUD_REVIEW_PASSED,
      Events::FRAUD_REVIEW_REJECT_AUTOMATIC,
      Events::FRAUD_REVIEW_REJECT_MANUAL,
    ].freeze

    def initialize(issuers:, time_range:, data: nil)
      @issuers = issuers
      @time_range = time_range
      @data = data
    end

    def as_csv
      csv = []

      csv << ['Report Timeframe', "#{time_range.begin} to #{time_range.end}"]
      csv << ['Report Generated', Date.today.to_s] # rubocop:disable Rails/Date
      csv << ['Issuer', issuers.join(', ')] if issuers.present?
      csv << []
      csv << ['Metric', '# of Users']
      csv << []
      csv << ['IDV started', idv_started]
      csv << ['Welcome Submitted', idv_doc_auth_welcome_submitted]
      csv << ['Image Submitted', idv_doc_auth_image_vendor_submitted]
      csv << ['Socure Verification Data Requested', idv_doc_auth_socure_verification_data_requested]
      csv << []
      csv << ['Workflow completed', idv_final_resolution]
      csv << ['Workflow completed - With Phone Number', idv_final_resolution_verified]
      csv << ['Workflow completed - With Phone Number - Fraud Review', idv_final_resolution_fraud_review]
      csv << ['Workflow completed - GPO Pending', idv_final_resolution_gpo]
      csv << ['Workflow completed - GPO Pending - Fraud Review', idv_final_resolution_gpo_fraud_review]
      csv << ['Workflow completed - In-Person Pending', idv_final_resolution_in_person]
      csv << ['Workflow completed - In-Person Pending - Fraud Review', idv_final_resolution_in_person_fraud_review]
      csv << ['Workflow completed - GPO + In-Person Pending', idv_final_resolution_gpo_in_person]
      csv << ['Workflow completed - GPO + In-Person Pending - Fraud Review', idv_final_resolution_gpo_in_person_fraud_review]
      csv << []
      csv << ['Fraud review rejected', idv_fraud_rejected]
      csv << ['Successfully Verified', successfully_verified_users]
      csv << ['Successfully Verified - With phone number', idv_final_resolution_verified]
      csv << ['Successfully Verified - With mailed code', gpo_verification_submitted]
      csv << ['Successfully Verified - In Person', usps_enrollment_status_updated]
      csv << ['Successfully Verified - Passed fraud review', fraud_review_passed]
      csv << ['Blanket Proofing Rate (IDV Started to Successfully Verified)', blanket_proofing_rate]
      csv << ['Intent Proofing Rate (Welcome Submitted to Successfully Verified)', intent_proofing_rate]
      csv << ['Actual Proofing Rate (Image Submitted to Successfully Verified)', actual_proofing_rate]
      csv << ['Industry Proofing Rate (Verified minus IDV Rejected)', industry_proofing_rate]
    end

    def to_csv
      CSV.generate do |csv|
        as_csv.each { |row| csv << row }
      end
    end

    def merge(other)
      self.class.new(
        issuers: (Array(issuers) + Array(other.issuers)).uniq,
        time_range: Range.new(
          [time_range.begin, other.time_range.begin].min,
          [time_range.end, other.time_range.end].max,
        ),
        data: data.merge(other.data) { |_event, old_user_ids, new_user_ids| old_user_ids + new_user_ids },
      )
    end

    def blanket_proofing_rate
      safely_divide(successfully_verified_users, idv_started)
    end

    def intent_proofing_rate
      safely_divide(successfully_verified_users, idv_doc_auth_welcome_submitted)
    end

    def actual_proofing_rate
      denom = idv_doc_auth_image_vendor_submitted + idv_doc_auth_socure_verification_data_requested
      safely_divide(successfully_verified_users, denom)
    end

    def industry_proofing_rate
      safely_divide(successfully_verified_users, successfully_verified_users + idv_doc_auth_rejected)
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

    def idv_final_resolution_rate
      safely_divide(idv_final_resolution, idv_started)
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

    def passed_fraud_review_users
      users = data[Events::FRAUD_REVIEW_PASSED]
      return users if issuers.blank?

      users_with_events_for_any_issuer =
        issuers.each_with_object(Set.new) { |issuer, accumulated| accumulated.merge(data[sp_key(issuer)]) }

      users & users_with_events_for_any_issuer
    end

    def did_not_pass_fraud_review_users
      result = data[Events::FRAUD_REVIEW_REJECT_AUTOMATIC] + data[Events::FRAUD_REVIEW_REJECT_MANUAL]

      issuers&.each do |issuer|
        result &= data[sp_key(issuer)]
      end

      result
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

    def idv_started
      @idv_started ||= (data[Events::IDV_DOC_AUTH_WELCOME] + data[Events::IDV_DOC_AUTH_GETTING_STARTED]).count
    end

    def idv_doc_auth_image_vendor_submitted
      data[Events::IDV_DOC_AUTH_IMAGE_UPLOAD].count
    end

    def idv_doc_auth_socure_verification_data_requested
      data[Events::IDV_DOC_AUTH_SOCURE_VERIFICATION_DATA].count
    end

    def idv_doc_auth_welcome_submitted
      data[Events::IDV_DOC_AUTH_WELCOME_SUBMITTED].count
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

    def idv_fraud_rejected
      did_not_pass_fraud_review_users.count
    end

    def fraud_review_passed
      passed_fraud_review_users.count
    end

    def verified_user_count
      @verified_user_count ||= Reports::BaseReport.transaction_with_timeout do
        Profile.active.where('verified_at <= ?', time_range.end.end_of_day).count
      end
    end

    def data
      @data ||= begin
        users = Hash.new { |h, event_name| h[event_name] = Set.new }

        fetch_results.each do |row|
          event = row['name']
          user_id = row['user_id']
          next if user_id.blank?

          event_properties = extract_event_properties(row['message'])
          success = event_success(row, event_properties)
          gpo_verification_pending = true_value?(event_properties['gpo_verification_pending'])
          in_person_verification_pending = true_value?(event_properties['in_person_verification_pending'])
          fraud_review_pending = fraud_review_pending?(event_properties)
          has_other_deactivation_reason = event_properties['deactivation_reason'].present?
          profile_not_pending =
            !fraud_review_pending &&
            !gpo_verification_pending &&
            !in_person_verification_pending &&
            !has_other_deactivation_reason
          service_provider = row['service_provider'].presence || event_properties['issuer']

          if event == Events::USPS_ENROLLMENT_STATUS_UPDATED && !true_value?(event_properties['passed'])
            next
          end

          if [Events::GPO_VERIFICATION_SUBMITTED, Events::GPO_VERIFICATION_SUBMITTED_OLD].include?(event)
            next if !success || true_value?(event_properties['pending_in_person_enrollment'])
          end

          next if event == Events::FRAUD_REVIEW_PASSED && !success

          if issuers.present?
            tagged_event_for_issuer = service_provider.present? && issuers.include?(service_provider)
            next if !tagged_event_for_issuer && !FRAUD_EVENT_NAMES.include?(event)
          end

          ignore_event_for_user =
            fraud_review_pending && EVENTS_TO_IGNORE_IF_FRAUD_REVIEW_PENDING.include?(event)

          users[event] << user_id unless ignore_event_for_user
          users[sp_key(service_provider)] << user_id if service_provider.present?

          case event
          when Events::IDV_FINAL_RESOLUTION
            users[Results::IDV_FINAL_RESOLUTION_VERIFIED] << user_id if profile_not_pending

            if !gpo_verification_pending && !in_person_verification_pending
              users[Results::IDV_FINAL_RESOLUTION_FRAUD_REVIEW] << user_id if fraud_review_pending
            elsif gpo_verification_pending && !in_person_verification_pending
              users[Results::IDV_FINAL_RESOLUTION_GPO] << user_id unless fraud_review_pending
              users[Results::IDV_FINAL_RESOLUTION_GPO_FRAUD_REVIEW] << user_id if fraud_review_pending
            elsif !gpo_verification_pending && in_person_verification_pending
              users[Results::IDV_FINAL_RESOLUTION_IN_PERSON] << user_id unless fraud_review_pending
              users[Results::IDV_FINAL_RESOLUTION_IN_PERSON_FRAUD_REVIEW] << user_id if fraud_review_pending
            elsif gpo_verification_pending && in_person_verification_pending
              users[Results::IDV_FINAL_RESOLUTION_GPO_IN_PERSON] << user_id unless fraud_review_pending
              users[Results::IDV_FINAL_RESOLUTION_GPO_IN_PERSON_FRAUD_REVIEW] << user_id if fraud_review_pending
            end
          when Events::IDV_DOC_AUTH_IMAGE_UPLOAD
            users[Results::IDV_REJECT_DOC_AUTH] << user_id if doc_auth_failed_non_fraud?(event_properties, success)
          when Events::IDV_DOC_AUTH_VERIFY_RESULTS
            users[Results::IDV_REJECT_VERIFY] << user_id unless success
          when Events::IDV_PHONE_FINDER_RESULTS
            users[Results::IDV_REJECT_PHONE_FINDER] << user_id unless success
          end
        end

        users
      end
    end

    def fetch_results
      connection.execute(query).to_a
    end

    private

    def query
      conditions = []
      conditions << "name in (#{quoted(Events.all_events)})"
      conditions << "cloudwatch_timestamp between #{connection.quote(time_range.begin)} and #{connection.quote(time_range.end)}"

      if issuers.present?
        conditions << "(service_provider in (#{quoted(issuers)}) OR name in (#{quoted(FRAUD_EVENT_NAMES)}))"
      end

      <<~SQL
        SELECT
          name,
          user_id,
          success,
          service_provider,
          message
        FROM logs.events
        WHERE #{conditions.join(' AND ')}
      SQL
    end

    def quoted(values)
      values.map { |value| connection.quote(value) }.join(', ')
    end

    def connection
      DataWarehouseApplicationRecord.connection
    end

    def true_value?(value)
      value == true || value.to_s == '1' || value.to_s.casecmp('true').zero?
    end

    def extract_event_properties(message)
      payload = case message
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

    def fraud_review_pending?(event_properties)
      true_value?(event_properties['fraud_review_pending']) ||
        event_properties['fraud_pending_reason'].present? ||
        true_value?(event_properties['fraud_check_failed']) ||
        %w[threatmetrix_review threatmetrix_reject].include?(event_properties['tmx_status'])
    end

    def doc_auth_failed_non_fraud?(event_properties, success)
      !success && !%w[Failed Attention].include?(event_properties['doc_auth_result'].to_s)
    end

    def safely_divide(numerator, denominator)
      return 0.0 if denominator.to_f.zero?

      numerator.to_f / denominator.to_f
    end

    def sp_key(issuer)
      "sp:#{issuer}"
    end
  end
end
