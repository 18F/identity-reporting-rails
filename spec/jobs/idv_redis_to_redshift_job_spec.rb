require 'rails_helper'
require 'factory_bot'

RSpec.describe IdvRedisToRedshiftJob, type: :job do
  let(:fcms_job) { IdvRedisToRedshiftJob.new }
  let(:redis_client) { AttemptsApi::RedisClient.new }
  let(:test_timestamp) { Time.current }

  def write_events_to_redis(event_size, start_index = 0)
    event_size.times do |i|
      redis_client.write_event(
        event_key: "event-1234-#{start_index + i}",
        jwe: 'eyJhbGciOiJSU0EtT0FFUCIsImVuYy',
        timestamp: test_timestamp,
      )
    end
  end

  def perform_job_with_logging(exp_event_count)
    write_events_to_redis(exp_event_count)
    allow(Rails.logger).to receive(:info).and_call_original
    msg = {
      job: 'IdvRedisToRedshiftJob',
      success: true,
      message: "IdvRedisToRedshiftJob: Read #{exp_event_count} event(s) from Redis for processing.",
    }
    expect(Rails.logger).to receive(:info).with(msg.to_json)
    fcms_job.perform
  end

  describe '#perform' do
    context 'when idv records are present in redis' do
      before do
        allow(IdentityConfig.store).to receive(:data_warehouse_fcms_enabled).and_return(true)
      end

      it 'imports the events into fcms.encrypted_events' do
        event_size = 50
        perform_job_with_logging(event_size)

        result = DataWarehouseApplicationRecord.connection.execute(
          'SELECT count(*) FROM fcms.encrypted_events',
        ).to_a
        expect(result.length).to eq(1)
        expect(result.first['count']).to eq(event_size)
      end

      it 'Reruns the full load without duplicating records' do
        # First, load 50 records and run the job
        initial_event_size = 50
        perform_job_with_logging(initial_event_size)

        # Get event_keys after first run
        first_run_events = DataWarehouseApplicationRecord.connection.execute(
          'SELECT event_key FROM fcms.encrypted_events',
        ).to_a.map { |row| row['event_key'] }

        # Verify the first run loaded 50 records
        expect(first_run_events.length).to eq(initial_event_size)

        # Now add 5 more records to Redis (total 55)
        incremental_event_size = 5
        additional_event_size = initial_event_size + incremental_event_size
        # Run the job again - should only process the 5 new records
        perform_job_with_logging(additional_event_size)

        # Get event_keys after second run
        total_events_in_db = DataWarehouseApplicationRecord.connection.execute(
          'SELECT event_key FROM fcms.encrypted_events',
        ).to_a.map { |row| row['event_key'] }

        # Calculate the difference (newly added event_keys)
        newly_added_events = total_events_in_db - first_run_events

        # Verify total is now 55 records (50 original + 5 new)
        expect(total_events_in_db.length).to eq(additional_event_size)
        expect(newly_added_events.length).to eq(incremental_event_size)
      end
    end
  end
end
