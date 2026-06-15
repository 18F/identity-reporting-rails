# frozen_string_literal: true

require 'rails_helper'
require 'reporting/identity_verification_report'

RSpec.describe Reporting::IdentityVerificationReport do
  let(:time_range) { Date.new(2022, 1, 1).in_time_zone('UTC').all_day }

  subject(:report) { described_class.new(time_range: time_range) }

  before(:each) { @event_seq = 0 }

  def create_event(user_id:, name:, success: nil, event_properties: {})
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
    )
  end

  before do
    # --- user1: online verification, failed each vendor once then succeeded ---
    create_event(user_id: 'user1', name: 'IdV: doc auth welcome visited')
    create_event(user_id: 'user1', name: 'IdV: doc auth welcome submitted')
    create_event(
      user_id: 'user1',
      name: 'IdV: doc auth image upload vendor submitted',
      success: false,
      event_properties: { doc_auth_result: 'Passed' }, # non-fraud failure -> doc auth reject
    )
    create_event(
      user_id: 'user1',
      name: 'IdV: doc auth image upload vendor submitted',
      success: true,
    )
    create_event(
      user_id: 'user1', name: 'IdV: doc auth verify proofing results', success: false,
    )
    create_event(
      user_id: 'user1', name: 'IdV: doc auth verify proofing results', success: true,
    )
    create_event(user_id: 'user1', name: 'IdV: phone confirmation vendor', success: false)
    create_event(user_id: 'user1', name: 'IdV: phone confirmation vendor', success: true)
    create_event(
      user_id: 'user1', name: 'IdV: final resolution', success: true,
    ) # no pending flags -> profile_not_pending -> Verified

    # --- user2: GPO (mailed code) pending, incomplete ---
    create_event(user_id: 'user2', name: 'IdV: doc auth welcome visited')
    create_event(user_id: 'user2', name: 'IdV: doc auth welcome submitted')
    create_event(
      user_id: 'user2', name: 'idv_socure_verification_data_requested', success: true,
    )
    create_event(
      user_id: 'user2',
      name: 'IdV: final resolution',
      success: true,
      event_properties: { gpo_verification_pending: true },
    )

    # --- user3: fraud review pending at final resolution, then passed ---
    create_event(user_id: 'user3', name: 'IdV: doc auth welcome visited')
    create_event(user_id: 'user3', name: 'IdV: doc auth welcome submitted')
    create_event(
      user_id: 'user3', name: 'IdV: doc auth image upload vendor submitted', success: true,
    )
    create_event(
      user_id: 'user3',
      name: 'IdV: final resolution',
      success: true,
      event_properties: { fraud_review_pending: true },
    )
    create_event(user_id: 'user3', name: 'Fraud: Profile review passed', success: true)

    # --- user4: GPO submission (old event name) then passed fraud review ---
    create_event(user_id: 'user4', name: 'IdV: GPO verification submitted', success: true)
    create_event(user_id: 'user4', name: 'Fraud: Profile review passed', success: true)

    # --- user5: in-person pending at final resolution, doc auth failed (non-fraud), USPS passed ---
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

    # --- user6: incomplete (welcome + failed doc auth, never resolved) ---
    create_event(user_id: 'user6', name: 'IdV: doc auth welcome visited')
    create_event(user_id: 'user6', name: 'IdV: doc auth welcome submitted')
    create_event(
      user_id: 'user6',
      name: 'IdV: doc auth image upload vendor submitted',
      success: false,
      event_properties: { doc_auth_result: 'Passed' },
    )

    # --- user7: fraud review pending at final resolution, then rejected ---
    create_event(user_id: 'user7', name: 'IdV: doc auth welcome visited')
    create_event(user_id: 'user7', name: 'IdV: doc auth welcome submitted')
    create_event(
      user_id: 'user7', name: 'IdV: doc auth image upload vendor submitted', success: true,
    )
    create_event(
      user_id: 'user7',
      name: 'IdV: final resolution',
      success: true,
      event_properties: { fraud_review_pending: true },
    )
    create_event(user_id: 'user7', name: 'Fraud: Profile review rejected', success: true)

    # --- user8: fraud rejection only ---
    create_event(user_id: 'user8', name: 'Fraud: Profile review rejected', success: true)

    # --- user9: GPO submission with fraud review pending, then rejected ---
    create_event(
      user_id: 'user9',
      name: 'IdV: GPO verification submitted',
      success: true,
      event_properties: { fraud_review_pending: true },
    )
    create_event(user_id: 'user9', name: 'Fraud: Profile review rejected', success: true)

    # --- user10: USPS update while in fraud review, then passed ---
    create_event(
      user_id: 'user10',
      name: 'GetUspsProofingResultsJob: Enrollment status updated',
      event_properties: { passed: true, fraud_review_pending: true },
    )
    create_event(user_id: 'user10', name: 'Fraud: Profile review passed', success: true)

    # --- user11: bounced on welcome screen ---
    create_event(user_id: 'user11', name: 'IdV: doc auth welcome visited')
  end

  describe '#data' do
    it 'counts unique users per event and derived result as a hash' do
      expect(report.data.transform_values(&:count)).to eq(
        # events
        'IdV: doc auth welcome visited' => 7,
        'IdV: doc auth welcome submitted' => 6,
        'IdV: doc auth image upload vendor submitted' => 5,
        'idv_socure_verification_data_requested' => 1,
        'IdV: doc auth verify proofing results' => 1,
        'IdV: phone confirmation vendor' => 1,
        'IdV: final resolution' => 5,
        'IdV: GPO verification submitted' => 1,
        'GetUspsProofingResultsJob: Enrollment status updated' => 1,
        'Fraud: Profile review passed' => 3,
        'Fraud: Profile review rejected' => 3,

        # derived results
        'IdV: final resolution - Verified' => 1,
        'IdV: final resolution - Fraud Review Pending' => 2,
        'IdV: final resolution - GPO Pending' => 1,
        'IdV: final resolution - In Person Proofing' => 1,
        'IdV Reject: Doc Auth' => 3,
        'IdV Reject: Verify' => 1,
        'IdV Reject: Phone Finder' => 1,
      )
    end

    it 'ignores GPO/USPS events for a user whose final resolution is fraud-review pending' do
      # user9's GPO submission carries fraud_review_pending, so it is not counted
      # as a GPO verification submitted event.
      gpo_users = report.data['IdV: GPO verification submitted']
      expect(gpo_users).to include('user4')
      expect(gpo_users).not_to include('user9')
    end

    it 'skips USPS enrollment updates that did not pass' do
      create_event(
        user_id: 'user12',
        name: 'GetUspsProofingResultsJob: Enrollment status updated',
        event_properties: { passed: false },
      )

      expect(report.data['GetUspsProofingResultsJob: Enrollment status updated']).
        not_to include('user12')
    end

    it 'skips fraud review passed events that were not successful' do
      FactoryBot.create(
        :event,
        id: 'event_fraud_failed',
        user_id: 'user13',
        name: 'Fraud: Profile review passed',
        cloudwatch_timestamp: time_range.begin + 1.hour,
        success: false,
        message: { properties: { event_properties: { success: false } } }.to_json,
        new_event: true,
      )

      expect(report.data['Fraud: Profile review passed']).not_to include('user13')
    end
  end

  describe '#idv_started' do
    it 'counts unique users across welcome and getting started events' do
      expect(report.idv_started).to eq(7)
    end
  end

  describe '#idv_doc_auth_welcome_submitted' do
    it 'counts users who submitted the welcome screen' do
      expect(report.idv_doc_auth_welcome_submitted).to eq(6)
    end
  end

  describe '#idv_doc_auth_image_vendor_submitted' do
    it 'counts users who submitted images' do
      expect(report.idv_doc_auth_image_vendor_submitted).to eq(5)
    end
  end

  describe '#idv_doc_auth_socure_verification_data_requested' do
    it 'counts socure verification data requests' do
      expect(report.idv_doc_auth_socure_verification_data_requested).to eq(1)
    end
  end

  describe '#idv_final_resolution' do
    it 'counts users who reached final resolution' do
      expect(report.idv_final_resolution).to eq(5)
    end
  end

  describe '#idv_doc_auth_rejected' do
    it 'is the count of users who failed proofing and never verified' do
      # Set union of doc-auth/verify/phone rejects = {user1, user5, user6},
      # minus verified {user1} and in-person {user5} = {user6}
      expect(report.idv_doc_auth_rejected).to eq(1)
    end
  end

  describe '#idv_fraud_rejected' do
    it 'counts users who failed fraud review (automatic + manual)' do
      expect(report.idv_fraud_rejected).to eq(3)
    end
  end

  describe '#fraud_review_passed' do
    it 'counts users who passed fraud review' do
      expect(report.fraud_review_passed).to eq(3)
    end
  end

  describe '#successfully_verified_users' do
    it 'is the count of users who verified, completed GPO/USPS, or passed fraud review' do
      expect(report.successfully_verified_users).to eq(5)
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

    context 'when there is no data' do
      let(:empty_data) { Hash.new { |hash, key| hash[key] = Set.new } }

      subject(:report) { described_class.new(time_range: time_range, data: empty_data) }

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

  describe 'event property parsing' do
    it 'falls back to the top-level success column when event_properties lacks success' do
      # Build an event whose message has no success in event_properties,
      # but the column is set.
      FactoryBot.create(
        :event,
        id: 'event_top_level',
        user_id: 'user_top_level',
        name: 'IdV: doc auth verify proofing results',
        cloudwatch_timestamp: time_range.begin + 1.hour,
        success: false,
        message: { properties: { event_properties: {} } }.to_json,
        new_event: true,
      )

      expect(report.data['IdV Reject: Verify']).to include('user_top_level')
    end

    it 'gracefully handles malformed JSON messages' do
      FactoryBot.create(
        :event,
        id: 'event_bad_json',
        user_id: 'user_bad_json',
        name: 'IdV: doc auth welcome visited',
        cloudwatch_timestamp: time_range.begin + 1.hour,
        message: 'not-valid-json',
        new_event: true,
      )

      expect { report.data }.not_to raise_error
      expect(report.data['IdV: doc auth welcome visited']).to include('user_bad_json')
    end

    it 'skips rows with a blank user_id' do
      FactoryBot.create(
        :event,
        id: 'event_blank_user',
        user_id: nil,
        name: 'IdV: doc auth welcome visited',
        cloudwatch_timestamp: time_range.begin + 1.hour,
        message: { properties: { event_properties: {} } }.to_json,
        new_event: true,
      )

      expect(report.idv_started).to eq(7)
    end
  end

  describe '#query' do
    it 'filters by the configured event names' do
      query = report.send(:query)

      aggregate_failures do
        Reporting::IdentityVerificationReport::Events.all_events.each do |event|
          expect(query).to include(event)
        end
      end
    end

    it 'filters on the time range bounds' do
      query = report.send(:query)

      aggregate_failures do
        expect(query).to include('cloudwatch_timestamp BETWEEN')
        expect(query).to include('logs.events')
      end
    end
  end

  describe 'time range scoping' do
    it 'excludes events outside the time range' do
      FactoryBot.create(
        :event,
        id: 'event_out_of_range',
        user_id: 'user_out_of_range',
        name: 'IdV: doc auth welcome visited',
        cloudwatch_timestamp: time_range.end + 5.days,
        message: { properties: { event_properties: {} } }.to_json,
        new_event: true,
      )

      expect(report.data['IdV: doc auth welcome visited']).not_to include('user_out_of_range')
    end
  end

  describe '#categorize_final_resolution branches' do
    # Isolate these cases so they don't affect the main fixture's counts.
    before { Event.delete_all }

    def final_resolution(user_id:, **event_properties)
      create_event(
        user_id: user_id,
        name: 'IdV: final resolution',
        success: true,
        event_properties: event_properties,
      )
    end

    it 'categorizes GPO pending + fraud review' do
      final_resolution(
        user_id: 'gpo_fraud',
        gpo_verification_pending: true,
        fraud_review_pending: true,
      )

      data = report.data
      expect(data['Idv: final resolution - GPO Pending + Fraud Review Pending']).
        to include('gpo_fraud')
      # not the plain GPO Pending bucket
      expect(data['IdV: final resolution - GPO Pending']).not_to include('gpo_fraud')
      # fraud review pending means profile is pending -> not Verified
      expect(data['IdV: final resolution - Verified']).not_to include('gpo_fraud')
    end

    it 'categorizes in-person pending + fraud review' do
      final_resolution(
        user_id: 'ipp_fraud',
        in_person_verification_pending: true,
        fraud_review_pending: true,
      )

      data = report.data
      expect(data['IdV: final resolution - In Person Proofing + Fraud Review Pending']).
        to include('ipp_fraud')
      expect(data['IdV: final resolution - In Person Proofing']).not_to include('ipp_fraud')
      expect(data['IdV: final resolution - Verified']).not_to include('ipp_fraud')
    end

    it 'categorizes GPO pending + in-person pending (no fraud)' do
      final_resolution(
        user_id: 'gpo_ipp',
        gpo_verification_pending: true,
        in_person_verification_pending: true,
      )

      data = report.data
      expect(data['IdV: final resolution - GPO Pending + In Person Pending']).
        to include('gpo_ipp')
      # pending profile -> not Verified
      expect(data['IdV: final resolution - Verified']).not_to include('gpo_ipp')
    end

    it 'categorizes GPO pending + in-person pending + fraud review' do
      final_resolution(
        user_id: 'gpo_ipp_fraud',
        gpo_verification_pending: true,
        in_person_verification_pending: true,
        fraud_review_pending: true,
      )

      data = report.data
      expect(data['IdV: final resolution - GPO Pending + In Person Pending + Fraud Review']).
        to include('gpo_ipp_fraud')
      expect(data['IdV: final resolution - GPO Pending + In Person Pending']).
        not_to include('gpo_ipp_fraud')
      expect(data['IdV: final resolution - Verified']).not_to include('gpo_ipp_fraud')
    end

    it 'categorizes a fully resolved profile as Verified when nothing is pending' do
      final_resolution(user_id: 'verified_user')

      data = report.data
      expect(data['IdV: final resolution - Verified']).to include('verified_user')
    end

    it 'does not mark a profile Verified when it has a deactivation reason' do
      final_resolution(
        user_id: 'deactivated',
        deactivation_reason: 'password_reset',
      )

      data = report.data
      # has_other_deactivation_reason -> profile_not_pending is false
      expect(data['IdV: final resolution - Verified']).not_to include('deactivated')
      # no pending flags, so it falls into the fraud-review branch guard but
      # fraud_review_pending is false -> lands in no pending bucket
      expect(data['IdV: final resolution - Fraud Review Pending']).not_to include('deactivated')
    end
  end
end
