require 'rails_helper'

RSpec.describe FraudOps::EventFieldExtractor do
  let(:event_type_url) do
    'https://schemas.login.gov/secevent/attempts-api/event-type/idv-phone-submitted'
  end
  let(:event_object) do
    {
      occurred_at: 1_774_297_419.1,
      success: true,
      device_id: 'dev-xyz',
      user_ip_address: '104.56.43.23',
      agency_uuid: 'agency-bbb',
      unique_session_id: 'sess-ccc',
      user_uuid: 'uuid-aaa',
      user_id: 21,
    }
  end
  let(:enveloped) { { jti: 'j1', iat: 1, events: { event_type_url.to_sym => event_object } } }
  let(:no_envelope) { { event_type_url.to_sym => event_object } }

  describe '.call' do
    it 'flattens enveloped payloads and derives the event_type slug' do
      expect(described_class.call(enveloped)).to eq(
        event_type: 'idv-phone-submitted',
        success: true,
        device_id: 'dev-xyz',
        user_ip_address: '104.56.43.23',
        agency_uuid: 'agency-bbb',
        unique_session_id: 'sess-ccc',
      )
    end

    it 'flattens the no-envelope shape (top-level event-type key)' do
      expect(described_class.call(no_envelope)).to eq(
        event_type: 'idv-phone-submitted',
        success: true,
        device_id: 'dev-xyz',
        user_ip_address: '104.56.43.23',
        agency_uuid: 'agency-bbb',
        unique_session_id: 'sess-ccc',
      )
    end

    it 'maps absent fields to nil without raising' do
      sparse = { events: { event_type_url.to_sym => { unique_session_id: 'only' } } }
      expect(described_class.call(sparse)).to eq(
        event_type: 'idv-phone-submitted',
        success: nil,
        device_id: nil,
        user_ip_address: nil,
        agency_uuid: nil,
        unique_session_id: 'only',
      )
    end

    it 'maps JSON-null values to nil (not the string "null")' do
      nulled = { events: { event_type_url.to_sym => { device_id: nil, success: false } } }
      result = described_class.call(nulled)
      expect(result[:device_id]).to be_nil
      expect(result[:success]).to eq(false)
    end

    it 'returns all-nil (including event_type) when no event object is present' do
      expect(described_class.call({ jti: 'x', iat: 1 })).to eq(
        event_type: nil, success: nil, device_id: nil,
        user_ip_address: nil, agency_uuid: nil, unique_session_id: nil
      )
    end

    it 'returns all-nil when the enveloped event value is not a Hash' do
      malformed = { events: { event_type_url.to_sym => 'oops' } }
      expect(described_class.call(malformed)).to eq(
        event_type: nil, success: nil, device_id: nil,
        user_ip_address: nil, agency_uuid: nil, unique_session_id: nil
      )
    end
  end

  describe '.event_object' do
    it 'returns the single event value for the enveloped shape' do
      expect(described_class.event_object(enveloped)).to eq(event_object)
    end

    it 'returns the single event value for the no-envelope shape' do
      expect(described_class.event_object(no_envelope)).to eq(event_object)
    end

    it 'returns nil when there is no event object' do
      expect(described_class.event_object({ jti: 'x' })).to be_nil
    end

    it 'returns nil when the enveloped event value is not a Hash' do
      expect(described_class.event_object({ events: { event_type_url.to_sym => 'oops' } })).
        to be_nil
    end
  end
end
