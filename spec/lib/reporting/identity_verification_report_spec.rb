# frozen_string_literal: true

require 'rails_helper'
require 'reporting/identity_verification_report'
require 'reporting/json_path_helper'

# Note - this spec logic is largely copied from
# identity-idp/spec/lib/reporting/identity_verification_report_spec.rb
# but has been changed to reflect the SQL processing approach (instead of Ruby
# processing in the IdP repo). The reporting-rails report also has less
# functionality (i.e. no issuer filtering, no #merge, no CloudWatch client).

RSpec.describe Reporting::IdentityVerificationReport do
  let(:time_range) { Date.new(2026, 7, 1).in_time_zone('UTC').all_day }

  subject(:report) { described_class.new(time_range: time_range) }

  before(:each) { @event_seq = 0 }

  def create_event(user_id:, name:, success: nil, event_properties: {}, **overrides)
    props = event_properties.dup
    props[:success] = success unless success.nil?

    @event_seq += 1

    FactoryBot.create(
      :event,
      id: "event_#{@event_seq}",
      user_id: user_id,
      name: name,
      cloudwatch_timestamp: time_range.begin + 1.hour,
      success: success,
      message: { properties: { event_properties: props } }.to_json,
      new_event: true,
      **overrides,
    )
  end

  before do
    # Divergence from old spec: the old CloudWatch spec pre-condensed each user's
    # events into one row per (user, event) via Ruby. Since
    # the reporting-rails version has SQL doing the dedupe/aggregation
    # on the raw events, the spec needs all the raw events to test the SQL logic (additional events)

    # user1: online verification, failed each vendor once then succeeded -> Verified
    create_event(user_id: 'user1', name: 'IdV: doc auth welcome visited')
    create_event(user_id: 'user1', name: 'IdV: doc auth welcome submitted')
    create_event(
      user_id: 'user1',
      name: 'IdV: doc auth image upload vendor submitted',
      success: false,
      event_properties: { doc_auth_result: 'Passed' },
    )
    create_event(
      user_id: 'user1', name: 'IdV: doc auth image upload vendor submitted', success: true,
    )
    create_event(user_id: 'user1', name: 'IdV: doc auth verify proofing results', success: false)
    create_event(user_id: 'user1', name: 'IdV: doc auth verify proofing results', success: true)
    create_event(user_id: 'user1', name: 'IdV: phone confirmation vendor', success: false)
    create_event(user_id: 'user1', name: 'IdV: phone confirmation vendor', success: true)
    create_event(user_id: 'user1', name: 'IdV: final resolution', success: true) # Verified

    # user2: GPO (mailed code) pending, incomplete
    create_event(user_id: 'user2', name: 'IdV: doc auth welcome visited')
    create_event(user_id: 'user2', name: 'IdV: doc auth welcome submitted')
    create_event(user_id: 'user2', name: 'idv_socure_verification_data_requested', success: true)
    create_event(
      user_id: 'user2',
      name: 'IdV: final resolution',
      success: true,
      event_properties: { gpo_verification_pending: true },
    )

    # user3: fraud review pending at final resolution, then passed
    create_event(user_id: 'user3', name: 'IdV: doc auth welcome visited')
    create_event(user_id: 'user3', name: 'IdV: doc auth welcome submitted')
    create_event(
      user_id: 'user3', name: 'IdV: doc auth image upload vendor submitted',
      success: true
    )
    create_event(
      user_id: 'user3',
      name: 'IdV: final resolution',
      success: true,
      event_properties: { fraud_review_pending: true },
    )
    create_event(user_id: 'user3', name: 'Fraud: Profile review passed', success: true)

    # user4: GPO submission then passed fraud review
    create_event(user_id: 'user4', name: 'IdV: GPO verification submitted', success: true)
    create_event(user_id: 'user4', name: 'Fraud: Profile review passed', success: true)

    # user5: in-person pending, doc auth failed (non-fraud), USPS passed
    create_event(user_id: 'user5', name: 'IdV: doc auth welcome visited')
    create_event(user_id: 'user5', name: 'IdV: doc auth welcome submitted')
    create_event(
      user_id: 'user5',
      name: 'IdV: doc auth image upload vendor submitted',
      success: false,
      event_properties: { doc_auth_result: 'Passed' },
    )
    create_event(
      user_id: 'user5',
      name: 'IdV: final resolution',
      success: true,
      event_properties: { in_person_verification_pending: true },
    )
    create_event(
      user_id: 'user5',
      name: 'GetUspsProofingResultsJob: Enrollment status updated',
      event_properties: { passed: true },
    )

    # user6: incomplete (welcome + failed doc auth, never resolved)
    create_event(user_id: 'user6', name: 'IdV: doc auth welcome visited')
    create_event(user_id: 'user6', name: 'IdV: doc auth welcome submitted')
    create_event(
      user_id: 'user6',
      name: 'IdV: doc auth image upload vendor submitted',
      success: false,
      event_properties: { doc_auth_result: 'Passed' },
    )

    # user7: fraud review pending at final resolution, then rejected
    create_event(user_id: 'user7', name: 'IdV: doc auth welcome visited')
    create_event(user_id: 'user7', name: 'IdV: doc auth welcome submitted')
    create_event(
      user_id: 'user7', name: 'IdV: doc auth image upload vendor submitted',
      success: true
    )
    create_event(
      user_id: 'user7',
      name: 'IdV: final resolution',
      success: true,
      event_properties: { fraud_review_pending: true },
    )
    create_event(user_id: 'user7', name: 'Fraud: Profile review rejected', success: true)

    # user8: fraud rejection only
    create_event(user_id: 'user8', name: 'Fraud: Profile review rejected', success: true)

    # user9: GPO submission with fraud review pending, then rejected
    create_event(
      user_id: 'user9',
      name: 'IdV: GPO verification submitted',
      success: true,
      event_properties: { fraud_review_pending: true },
    )
    create_event(user_id: 'user9', name: 'Fraud: Profile review rejected', success: true)

    # user10: USPS update while in fraud review, then passed
    create_event(
      user_id: 'user10',
      name: 'GetUspsProofingResultsJob: Enrollment status updated',
      event_properties: { passed: true, fraud_review_pending: true },
    )
    create_event(user_id: 'user10', name: 'Fraud: Profile review passed', success: true)

    # user11: bounced on welcome screen
    create_event(user_id: 'user11', name: 'IdV: doc auth welcome visited')
  end

  describe 'event-level counts' do
    it '#idv_started counts unique users across welcome and getting started events' do
      expect(report.idv_started).to eq(7)
    end

    it '#idv_doc_auth_welcome_submitted counts users who submitted the welcome screen' do
      expect(report.idv_doc_auth_welcome_submitted).to eq(6)
    end

    it '#idv_doc_auth_image_vendor_submitted counts users who submitted images' do
      expect(report.idv_doc_auth_image_vendor_submitted).to eq(5)
    end

    it '#idv_doc_auth_socure_verification_data_requested counts socure data requests' do
      expect(report.idv_doc_auth_socure_verification_data_requested).to eq(1)
    end

    it '#idv_final_resolution counts users who reached final resolution' do
      expect(report.idv_final_resolution).to eq(5)
    end
  end

  describe 'final-resolution bucket counts' do
    it 'counts the Verified bucket' do
      expect(report.idv_final_resolution_verified).to eq(1) # user1
    end

    it 'counts the GPO Pending bucket' do
      expect(report.idv_final_resolution_gpo).to eq(1) # user2
    end

    it 'counts the In Person Proofing bucket' do
      expect(report.idv_final_resolution_in_person).to eq(1) # user5
    end

    it 'counts the Fraud Review Pending bucket' do
      expect(report.idv_final_resolution_fraud_review).to eq(2) # user3, user7
    end
  end

  describe 'reject / fraud counts' do
    it '#reject_doc_auth counts non-fraud doc auth failures' do
      expect(report.reject_doc_auth).to eq(3) # user1, user5, user6
    end

    it '#reject_verify counts verify failures' do
      expect(report.reject_verify).to eq(1) # user1
    end

    it '#reject_phone counts phone failures' do
      expect(report.reject_phone).to eq(1) # user1
    end

    it '#idv_fraud_rejected counts users who failed fraud review (automatic + manual)' do
      expect(report.idv_fraud_rejected).to eq(3) # user7, user8, user9
    end

    it '#idv_doc_auth_rejected is rejects MINUS verified/in-person (set subtraction)' do
      # reject union = {user1, user5, user6}; minus verified {user1} and in-person {user5} = {user6}
      expect(report.idv_doc_auth_rejected).to eq(1)
    end
  end

  describe '#successfully_verified_users' do
    it 'counts users who verified, completed GPO/USPS, or passed fraud review' do
      # user1 (verified), user4 (gpo submit), user5 (usps), user3 + user10 (fraud passed) = 5
      expect(report.successfully_verified_users).to eq(5)
    end
  end

  # New tests (for robustness): more thoroughly test #idv_doc_auth_rejected (which is cross-row)
  describe '#idv_doc_auth_rejected set subtraction (isolated)' do
    before { Event.delete_all }

    it 'excludes a user who was rejected but later verified' do
      create_event(
        user_id: 'retry',
        name: 'IdV: doc auth image upload vendor submitted',
        success: false,
        event_properties: { doc_auth_result: 'Passed' },
      )
      create_event(user_id: 'retry', name: 'IdV: final resolution', success: true) # Verified

      expect(report.idv_doc_auth_rejected).to eq(0)
    end

    it 'counts a user who was rejected and never verified' do
      create_event(
        user_id: 'stuck',
        name: 'IdV: doc auth image upload vendor submitted',
        success: false,
        event_properties: { doc_auth_result: 'Passed' },
      )

      expect(report.idv_doc_auth_rejected).to eq(1)
    end

    it 'excludes a user who was rejected but is in-person pending' do
      create_event(
        user_id: 'ipp',
        name: 'IdV: doc auth image upload vendor submitted',
        success: false,
        event_properties: { doc_auth_result: 'Passed' },
      )
      create_event(
        user_id: 'ipp',
        name: 'IdV: final resolution',
        success: true,
        event_properties: { in_person_verification_pending: true },
      )

      expect(report.idv_doc_auth_rejected).to eq(0)
    end
  end

  describe 'source-level filters (applied in SQL)' do
    # new tests (for robustness): individually test the exclusion rules
    context 'GPO submission with fraud review pending' do
      before { Event.delete_all }

      it 'does not count a fraud-pending GPO submission as successfully verified' do
        create_event(
          user_id: 'clean_gpo',
          name: 'IdV: GPO verification submitted',
          success: true,
        )
        create_event(
          user_id: 'fraud_gpo',
          name: 'IdV: GPO verification submitted',
          success: true,
          event_properties: { fraud_review_pending: true },
        )

        # only the clean GPO submission counts
        expect(report.successfully_verified_users).to eq(1)
      end
    end

    context 'USPS enrollment update that did not pass' do
      before { Event.delete_all }

      it 'excludes the user from successfully verified' do
        create_event(
          user_id: 'usps_failed',
          name: 'GetUspsProofingResultsJob: Enrollment status updated',
          event_properties: { passed: false },
        )

        expect(report.successfully_verified_users).to eq(0)
      end
    end

    context 'fraud review passed event that was not successful' do
      before { Event.delete_all }

      it 'excludes the user from successfully verified' do
        create_event(
          user_id: 'fraud_not_success',
          name: 'Fraud: Profile review passed',
          success: false,
        )

        expect(report.successfully_verified_users).to eq(0)
      end
    end

    it 'skips rows with a blank user_id' do
      create_event(user_id: nil, name: 'IdV: doc auth welcome visited')

      expect(report.idv_started).to eq(7)
    end

    it 'excludes events outside the time range' do
      create_event(
        user_id: 'user_out_of_range',
        name: 'IdV: doc auth welcome visited',
        cloudwatch_timestamp: time_range.end + 5.days,
      )

      expect(report.idv_started).to eq(7)
    end
  end

  # New test - couldwatch version handled malformed data whereas we read SQL JSON super blob
  describe 'malformed / partial message handling' do
    before { Event.delete_all }

    it 'does not raise and still counts the event when event_properties is empty' do
      create_event(user_id: 'sparse', name: 'IdV: doc auth welcome visited', event_properties: {})

      expect { report.idv_started }.not_to raise_error
      expect(report.idv_started).to eq(1)
    end
  end

  describe 'proofing rates' do
    describe '#blanket_proofing_rate' do
      it 'is successfully verified over idv started' do
        expect(report.blanket_proofing_rate).to eq(5.0 / 7.0)
      end
    end

    describe '#intent_proofing_rate' do
      it 'is successfully verified over welcome submitted' do
        expect(report.intent_proofing_rate).to eq(5.0 / 6.0)
      end
    end

    describe '#actual_proofing_rate' do
      it 'is successfully verified over image + socure submissions' do
        # image submitted = 5, socure = 1 -> denominator 6
        expect(report.actual_proofing_rate).to eq(5.0 / 6.0)
      end
    end

    describe '#industry_proofing_rate' do
      it 'is successfully verified over verified plus non-fraud rejected' do
        # successfully verified = 5, doc auth rejected = 1 -> 5 / 6
        expect(report.industry_proofing_rate).to eq(5.0 / 6.0)
      end
    end

    describe '#idv_final_resolution_rate' do
      it 'is final resolution over idv started' do
        expect(report.idv_final_resolution_rate).to eq(5.0 / 7.0)
      end
    end

    context 'when there is no data in the time range' do
      subject(:report) do
        described_class.new(time_range: Date.new(1999, 1, 1).in_time_zone('UTC').all_day)
      end

      it 'safely returns 0.0 for all rates instead of raising' do
        aggregate_failures do
          expect(report.blanket_proofing_rate).to eq(0.0)
          expect(report.intent_proofing_rate).to eq(0.0)
          expect(report.actual_proofing_rate).to eq(0.0)
          expect(report.industry_proofing_rate).to eq(0.0)
          expect(report.idv_final_resolution_rate).to eq(0.0)
        end
      end
    end
  end

  describe '#verified_user_count' do
    let(:connection) do
      instance_double(ActiveRecord::ConnectionAdapters::AbstractAdapter)
    end

    before do
      allow(report).to receive(:connection).and_return(connection)
      allow(connection).to receive(:quote) { |value| "'#{value}'" }
    end

    it 'returns the count from the profiles query' do
      allow(connection).to receive(:select_value).and_return(42)

      expect(report.verified_user_count).to eq(42)
    end

    it 'queries idp.profiles filtering on active and verified_at' do
      expect(connection).to receive(:select_value) do |sql|
        expect(sql).to include('FROM idp.profiles')
        expect(sql).to include('active = TRUE')
        expect(sql).to include('verified_at <=')
        1
      end

      report.verified_user_count
    end
  end

  describe '#metrics_query' do
    it 'filters by the configured event names' do
      query = report.send(:metrics_query)

      aggregate_failures do
        Reporting::IdentityVerificationReport::Events.all_events.each do |event|
          expect(query).to include(event)
        end
      end
    end

    it 'scopes to logs.events and the time range bounds' do
      query = report.send(:metrics_query)

      aggregate_failures do
        expect(query).to include('logs.events')
        expect(query).to include('cloudwatch_timestamp >=')
        expect(query).to include('cloudwatch_timestamp <=')
      end
    end
  end

  describe 'final-resolution categorization matrix' do
    before { Event.delete_all }

    def final_resolution(user_id:, **event_properties)
      create_event(
        user_id: user_id,
        name: 'IdV: final resolution',
        success: true,
        event_properties: event_properties,
      )
    end

    it 'categorizes a fully resolved profile as Verified when nothing is pending' do
      final_resolution(user_id: 'verified_user')

      expect(report.idv_final_resolution_verified).to eq(1)
    end

    it 'categorizes GPO pending (no fraud) as GPO Pending only' do
      final_resolution(user_id: 'gpo', gpo_verification_pending: true)

      aggregate_failures do
        expect(report.idv_final_resolution_gpo).to eq(1)
        expect(report.idv_final_resolution_gpo_fraud_review).to eq(0)
        expect(report.idv_final_resolution_verified).to eq(0)
      end
    end

    it 'categorizes GPO pending + fraud review' do
      final_resolution(
        user_id: 'gpo_fraud',
        gpo_verification_pending: true,
        fraud_review_pending: true,
      )

      aggregate_failures do
        expect(report.idv_final_resolution_gpo_fraud_review).to eq(1)
        expect(report.idv_final_resolution_gpo).to eq(0)
        expect(report.idv_final_resolution_verified).to eq(0)
      end
    end

    it 'categorizes in-person pending + fraud review' do
      final_resolution(
        user_id: 'ipp_fraud',
        in_person_verification_pending: true,
        fraud_review_pending: true,
      )

      aggregate_failures do
        expect(report.idv_final_resolution_in_person_fraud_review).to eq(1)
        expect(report.idv_final_resolution_in_person).to eq(0)
        expect(report.idv_final_resolution_verified).to eq(0)
      end
    end

    it 'categorizes GPO pending + in-person pending (no fraud)' do
      final_resolution(
        user_id: 'gpo_ipp',
        gpo_verification_pending: true,
        in_person_verification_pending: true,
      )

      aggregate_failures do
        expect(report.idv_final_resolution_gpo_in_person).to eq(1)
        expect(report.idv_final_resolution_verified).to eq(0)
      end
    end

    it 'categorizes GPO pending + in-person pending + fraud review' do
      final_resolution(
        user_id: 'gpo_ipp_fraud',
        gpo_verification_pending: true,
        in_person_verification_pending: true,
        fraud_review_pending: true,
      )

      aggregate_failures do
        expect(report.idv_final_resolution_gpo_in_person_fraud_review).to eq(1)
        expect(report.idv_final_resolution_gpo_in_person).to eq(0)
        expect(report.idv_final_resolution_verified).to eq(0)
      end
    end

    it 'does not mark a profile Verified when it has a deactivation reason' do
      final_resolution(user_id: 'deactivated', deactivation_reason: 'password_reset')

      aggregate_failures do
        # has_deactivation_reason -> profile_not_pending is false -> not Verified
        expect(report.idv_final_resolution_verified).to eq(0)
        # no pending flags and fraud_review_pending is false -> lands in no pending bucket
        expect(report.idv_final_resolution_fraud_review).to eq(0)
        # but it IS still a final-resolution event
        expect(report.idv_final_resolution).to eq(1)
      end
    end
  end
end
