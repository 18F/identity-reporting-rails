require 'rails_helper'
require 'factory_bot'

RSpec.describe IdvRedisToRedshiftJob, type: :job do
  let(:fraudops_job) { IdvRedisToRedshiftJob.new }
  let(:redis_client) { FraudOps::RedisClient.new }

  def write_event(event_key:, jwe:, timestamp:)
    formatted_time = timestamp.
      in_time_zone('UTC').
      change(min: (timestamp.min / 5) * 5).
      iso8601
    key = "fraud-ops-events:#{formatted_time}"
    redis_client.redis_pool.with do |client|
      client.hset(key, event_key, jwe)
      client.expire(key, 604800)
    end
  end

  def write_events_to_redis(event_size, timestamp)
    event_size.times do |i|
      write_event(
        event_key: "event-1234-#{i}",
        jwe: 'eyJhbGciOiJSU0EtT0FFUCIsImVuYy',
        timestamp: timestamp,
      )
    end
  end

  def perform_job_with_logging(expected_count, actual_count, event_timestamp)
    write_events_to_redis(expected_count, event_timestamp)
    allow(Rails.logger).to receive(:info).and_call_original
    msg = {
      job: 'IdvRedisToRedshiftJob',
      success: true,
      message: "Read #{actual_count} event(s) from Redis for processing.",
    }
    expect(Rails.logger).to receive(:info).with(msg.to_json)
    fraudops_job.perform
  end

  describe '#perform' do
    context 'when idv records are present in redis' do
      before do
        allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(true)
      end

      it 'imports the events into fraudops.encrypted_events' do
        event_size = 50
        perform_job_with_logging(event_size, event_size, Time.current - 1.hour)

        result = DataWarehouseApplicationRecord.connection.execute(
          'SELECT count(*) FROM fraudops.encrypted_events',
        ).to_a
        expect(result.length).to eq(1)
        expect(result.first['count']).to eq(event_size)
      end

      it 'Reruns the full load without duplicating records' do
        # First, load 50 records and run the job
        current_timestamp_minus_hour = Time.current - 1.hour
        initial_event_size = 50
        perform_job_with_logging(
          initial_event_size,
          initial_event_size,
          current_timestamp_minus_hour,
        )

        # Get event_keys after first run
        first_run_events = DataWarehouseApplicationRecord.connection.execute(
          'SELECT event_key FROM fraudops.encrypted_events',
        ).to_a.map { |row| row['event_key'] }

        # Verify the first run loaded 50 records
        expect(first_run_events.length).to eq(initial_event_size)

        # Now add 5 more records to Redis (total 55)
        incremental_event_size = 5
        additional_event_size = initial_event_size + incremental_event_size
        # Run the job again - should only process the 5 new records
        perform_job_with_logging(
          additional_event_size,
          additional_event_size,
          current_timestamp_minus_hour,
        )

        # Get event_keys after second run
        total_events_in_db = DataWarehouseApplicationRecord.connection.execute(
          'SELECT event_key FROM fraudops.encrypted_events',
        ).to_a.map { |row| row['event_key'] }

        # Calculate the difference (newly added event_keys)
        newly_added_events = total_events_in_db - first_run_events

        # Verify total is now 55 records (50 original + 5 new)
        expect(total_events_in_db.length).to eq(additional_event_size)
        expect(newly_added_events.length).to eq(incremental_event_size)
      end

      it 'do not process data from current 5 minute bucket' do
        perform_job_with_logging(50, 0, Time.current)

        result = DataWarehouseApplicationRecord.connection.execute(
          'SELECT count(*) FROM fraudops.encrypted_events',
        ).to_a
        expect(result.length).to eq(1)
        expect(result.first['count']).to eq(0)
      end
    end
    context 'when fraud_ops_tracker_enabled flag is false' do
      before do
        allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(false)
      end

      it 'does not run the job' do
        write_events_to_redis(50, Time.current - 1.hour)
        allow(Rails.logger).to receive(:info).and_call_original
        msg = {
          job: 'IdvRedisToRedshiftJob',
          success: false,
          message: 'fraud_ops_tracker_enabled is false, skipping job.',
        }
        expect(Rails.logger).to receive(:info).with(msg.to_json)
        fraudops_job.perform

        result = DataWarehouseApplicationRecord.connection.execute(
          'SELECT count(*) FROM fraudops.encrypted_events',
        ).to_a
        expect(result.length).to eq(1)
        expect(result.first['count']).to eq(0)
      end
    end
  end
end
