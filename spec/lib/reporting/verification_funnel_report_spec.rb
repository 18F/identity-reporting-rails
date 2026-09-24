# frozen_string_literal: true

require 'rails_helper'
require 'reporting/verification_funnel_report'
require 'reporting/json_path_helper'

# This spec exercises the real SQL against the test data warehouse (PostgreSQL)
# using FactoryBot :event records, mirroring identity_verification_report_spec.
RSpec.describe Reporting::VerificationFunnelReport do
  let(:issuer) { 'urn:gov:gsa:openidconnect.profiles:sp:sso:irs:test' }
  let(:other_issuer) { 'urn:gov:gsa:openidconnect.profiles:sp:sso:other:test' }
  let(:time_range) { Date.new(2026, 7, 1).in_time_zone('UTC').all_month }

  subject(:report) { described_class.new(issuer_string: issuer, time_range: time_range) }

  before(:each) { @event_seq = 0 }

  # facial_match defaults to true because this report is scoped to facial-match
  # flows; pass facial_match: nil to simulate a non-facial-match event.
  def create_event(user_id:, name:, success: nil, facial_match: true, service_provider: nil,
                   timestamp: time_range.begin + 1.hour)
    @event_seq += 1

    event_properties = {}
    event_properties[:success] = success unless success.nil?

    message = {
      properties: {
        sp_request: { facial_match: facial_match },
        event_properties: event_properties,
      },
    }

    FactoryBot.create(
      :event,
      id: "event_#{@event_seq}",
      user_id: user_id,
      name: name,
      service_provider: service_provider || issuer,
      cloudwatch_timestamp: timestamp,
      message: message.to_json,
      new_event: true,
    )
  end

  demand = 'IdV: doc auth welcome submitted'
  doc_auth = 'IdV: doc auth ssn visited'
  info_val = 'IdV: doc auth verify proofing results'
  phone = 'idv_enter_password_visited'
  verified = 'User registration: agency handoff visited'

  before do
    # user1: full funnel, verified
    create_event(user_id: 'user1', name: demand)
    create_event(user_id: 'user1', name: doc_auth)
    create_event(user_id: 'user1', name: info_val, success: true)
    create_event(user_id: 'user1', name: phone)
    create_event(user_id: 'user1', name: verified)

    # user2: reached info validation, then dropped off (no phone/verified)
    create_event(user_id: 'user2', name: demand)
    create_event(user_id: 'user2', name: doc_auth)
    create_event(user_id: 'user2', name: info_val, success: true)

    # user3: never reached the SSN page (no doc_auth event) - stage 1 only.
    # (An AAMVA/DMV state-ID failure blocks the user before the SSN page, so the
    # doc-auth page-visit event never fires for them - a legitimate doc-auth drop.)
    create_event(user_id: 'user3', name: demand)

    # user4: NON-facial-match user - must be excluded entirely by the filter
    create_event(user_id: 'user4', name: demand, facial_match: nil)
    create_event(user_id: 'user4', name: doc_auth, facial_match: nil)

    # user5: different service provider - must be excluded by the SP filter
    create_event(user_id: 'user5', name: demand, service_provider: other_issuer)

    # user6: outside the time range - must be excluded
    create_event(user_id: 'user6', name: demand, timestamp: time_range.end + 2.days)
  end

  describe '#funnel_table' do
    subject(:rows) { report.funnel_table }

    it 'counts unique users at each stage (excluding wrong SP, non-facial-match, out of range)' do
      # Only user1, user2, user3 qualify at the demand stage.
      expect(rows[1]).to eq(['Verification Demand', 3, 1.0])
      # Stage 2 (SSN page visited): user1, user2 (user3 never reached it).
      expect(rows[2]).to eq(['Document Authentication Success', 2, 2.0 / 3])
      # Stage 3 success: user1, user2.
      expect(rows[3]).to eq(['Information Validation Success', 2, 2.0 / 3])
      # Stage 4: user1.
      expect(rows[4]).to eq(['Phone Verification Success', 1, 1.0 / 3])
      # Stage 5: user1.
      expect(rows[5]).to eq(['Verification Successes', 1, 1.0 / 3])
      # Failures = demand - verified = 3 - 1 = 2.
      expect(rows[6]).to eq(['Verification Failures', 2, 2.0 / 3])
    end
  end

  describe '#as_reports' do
    it 'returns definitions, overview, and funnel reports with filenames' do
      reports = report.as_reports
      expect(reports.map { |r| r[:filename] }).to eq(
        %w[definitions overview verification_funnel_metrics],
      )
      expect(reports.map { |r| r[:table] }).to all(be_an(Array))
    end

    it 'includes the issuer and generated date in the overview table' do
      expect(report.overview_table).to include(
        ['Issuer', issuer],
        ['Report Generated', Date.current.to_s],
      )
    end
  end
end
