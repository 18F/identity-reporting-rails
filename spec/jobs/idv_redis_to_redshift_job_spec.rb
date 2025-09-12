require 'rails_helper'
require 'factory_bot'

RSpec.describe IdvRedisToRedshiftJob, type: :job do
  let(:fcms_job) { IdvRedisToRedshiftJob.new }
  let(:redis_client) { AttemptsApi::RedisClient.new }
  let(:event_size) { 50 }

  describe '#perform' do
    context 'when idv records are present in redis' do
      before do
        allow(IdentityConfig.store).to receive(:data_warehouse_fcms_enabled).and_return(true)
        event_size.times do |i|
          redis_client.write_event(
            event_key: "event-1234-#{i}",
            jwe: "eyJhbGciOiJSU0EtT0FFUCIsImVuYyI6IkEyNTZHQ00iLCJ0eXAiOiJzZWNldmVudCtqd2UiLCJ6aXAiOiJERUYiLCJraWQiOiIxYmZiODYxMmE2NWU3NTkzOThlNGM0NGZmOWRkMWQ4OTZlNzBk",
            timestamp: Time.current,
          )
        end
      end

      it 'imports the events into fcms.encrypted_events' do
        allow(Rails.logger).to receive(:info).and_call_original
        msg = { job: 'IdvRedisToRedshiftJob',
                success: true,
                message: "IdvRedisToRedshiftJob: Read #{event_size} event(s) from Redis for processing." }
        expect(Rails.logger).to receive(:info).with(msg.to_json)
        fcms_job.perform

        result = DataWarehouseApplicationRecord.connection.execute(
          "SELECT count(*) FROM fcms.encrypted_events",
        ).to_a
        expect(result.length).to eq(1)
        expect(result.first['count']).to eq(event_size)
      end
    end
  end
end
