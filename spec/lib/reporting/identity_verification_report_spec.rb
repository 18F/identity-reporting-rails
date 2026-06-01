require 'rails_helper'
require 'reporting/identity_verification_report'

RSpec.describe Reporting::IdentityVerificationReport do
  let(:time_range) do
    Time.zone.parse('2026-05-01 00:00:00 UTC')..
      Time.zone.parse('2026-05-31 23:59:59 UTC')
  end

  subject(:report) { described_class.new(issuers: nil, time_range: time_range) }

  def create_event(
    user_id:,
    name:,
    service_provider: nil,
    event_properties: {},
    cloudwatch_timestamp: time_range.begin + 1.day
  )
    message_payload = {
      'properties' => {
        'user_id' => user_id,
        'service_provider' => service_provider,
        'event_properties' => event_properties,
      },
    }

    Event.create!(
      id: SecureRandom.uuid,
      message: JSON.parse(JSON.generate(message_payload)),
      cloudwatch_timestamp: cloudwatch_timestamp,
      name: name,
      user_id: user_id,
      service_provider: service_provider,
      success: event_properties.fetch('success', true),
      new_event: true,
      time: cloudwatch_timestamp,
    )
  end

  before do
    Event.delete_all

    create(
      :profile,
      :active,
      user: create(:user, uuid: SecureRandom.uuid),
      verified_at: time_range.begin + 2.days,
    )
    create(
      :profile,
      :active,
      user: create(:user, uuid: SecureRandom.uuid),
      verified_at: time_range.begin + 3.days,
    )

    create_event(user_id: 'u1', name: described_class::Events::IDV_DOC_AUTH_WELCOME)
    create_event(user_id: 'u2', name: described_class::Events::IDV_DOC_AUTH_GETTING_STARTED)
    create_event(user_id: 'u1', name: described_class::Events::IDV_DOC_AUTH_WELCOME_SUBMITTED)
    create_event(
      user_id: 'u1',
      name: described_class::Events::IDV_DOC_AUTH_IMAGE_UPLOAD,
      event_properties: {
        'success' => false,
        'doc_auth_result' => 'SomeOtherFailure',
      },
    )
    create_event(
      user_id: 'u2',
      name: described_class::Events::IDV_DOC_AUTH_SOCURE_VERIFICATION_DATA,
    )
    create_event(
      user_id: 'u2',
      name: described_class::Events::IDV_FINAL_RESOLUTION,
      service_provider: 'issuer-a',
      event_properties: {
        'fraud_review_pending' => false,
        'gpo_verification_pending' => false,
        'in_person_verification_pending' => false,
      },
    )
    create_event(
      user_id: 'u2',
      name: described_class::Events::FRAUD_REVIEW_REJECT_MANUAL,
      service_provider: 'issuer-a',
      event_properties: {
        'success' => true,
      },
    )
    create_event(
      user_id: 'u4',
      name: described_class::Events::FRAUD_REVIEW_PASSED,
      event_properties: {
        'success' => true,
      },
    )
    create_event(
      user_id: 'u4',
      name: described_class::Events::IDV_DOC_AUTH_WELCOME,
      service_provider: 'issuer-a',
    )
    create_event(
      user_id: 'u4',
      name: described_class::Events::GPO_VERIFICATION_SUBMITTED,
      event_properties: {
        'success' => true,
        'pending_in_person_enrollment' => false,
      },
    )
    create_event(
      user_id: 'u5',
      name: described_class::Events::USPS_ENROLLMENT_STATUS_UPDATED,
      event_properties: {
        'passed' => true,
      },
    )
    create_event(
      user_id: 'u6',
      name: described_class::Events::IDV_DOC_AUTH_VERIFY_RESULTS,
      event_properties: {
        'success' => false,
      },
    )
    create_event(
      user_id: 'u7',
      name: described_class::Events::IDV_PHONE_FINDER_RESULTS,
      event_properties: {
        'success' => false,
      },
    )
  end

  describe 'core metrics' do
    it 'calculates key IDV metrics from Redshift logs.events rows' do
      aggregate_failures do
        expect(report.idv_started).to eq(3)
        expect(report.idv_doc_auth_welcome_submitted).to eq(1)
        expect(report.idv_doc_auth_image_vendor_submitted).to eq(1)
        expect(report.idv_doc_auth_socure_verification_data_requested).to eq(1)
        expect(report.idv_final_resolution).to eq(1)
        expect(report.successfully_verified_users).to be >= 1
        expect(report.idv_doc_auth_rejected).to eq(3)
        expect(report.idv_fraud_rejected).to eq(1)
        expect(report.verified_user_count).to eq(2)
      end
    end
  end

  describe 'issuer filtering for fraud pass-through events' do
    it 'counts passed fraud review users only when the user has issuer-tagged events' do
      issuer_report = described_class.new(issuers: ['issuer-a'], time_range: time_range)
      other_issuer_report = described_class.new(issuers: ['issuer-b'], time_range: time_range)

      aggregate_failures do
        expect(issuer_report.fraud_review_passed).to eq(1)
        expect(other_issuer_report.fraud_review_passed).to eq(0)
      end
    end
  end
end
