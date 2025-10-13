require 'rails_helper'

RSpec.describe FraudOpsPiiDecryptJob, type: :job do
  let(:job) { described_class.new }
  let(:private_key) { OpenSSL::PKey::RSA.generate(2048) }
  let(:private_key_pem) { private_key.to_pem }
  let(:mock_connection) { instance_double(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter) }
  let(:sample_event_data) { { user_id: 123, action: 'login' } }
  let(:encrypted_message) { 'encrypted_data_string' }
  let(:batch_size) { 1000 }

  let(:encrypted_events) do
    [
      {
        'event_key' => 'event_1',
        'message' => encrypted_message,
      },
      {
        'event_key' => 'event_2',
        'message' => encrypted_message,
      },
    ]
  end

  before do
    allow(job).to receive(:connection).and_return(mock_connection)
    allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(true)
    allow(IdentityConfig.store).to receive(:fraud_ops_private_key).and_return(private_key_pem)
    allow(DataWarehouseApplicationRecord).to receive(:transaction).and_yield
    allow(DataWarehouseApplicationRecord).to receive(:connection).and_return(mock_connection)
  end

  describe '#perform' do
    context 'when job is disabled' do
      before do
        allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(false)
      end

      it 'skips job execution and logs info' do
        expect(Rails.logger).to receive(:info).with(a_string_matching('Skipped'))
        job.perform
      end
    end

    context 'when no encrypted events exist' do
      before do
        allow(job).to receive(:fetch_encrypted_events).and_return([])
        allow(Rails.logger).to receive(:info)
      end

      it 'completes successfully without processing any events' do
        expect { job.perform }.not_to raise_error
        expect(job).not_to receive(:process_encrypted_events_bulk)
      end
    end

    context 'when encrypted events exist' do
      before do
        allow(job).to receive(:process_encrypted_events_bulk).and_return(2)
        allow(Rails.logger).to receive(:info)
      end

      it 'processes events successfully' do
        allow(job).to receive(:fetch_encrypted_events).and_return(encrypted_events, [])

        expect { job.perform }.not_to raise_error
        expect(job).to have_received(:process_encrypted_events_bulk).once
      end

      it 'processes in batches until no events remain' do
        # The loop breaks after first batch because encrypted_events.size (2) < batch_size (1000)
        expect(job).to receive(:fetch_encrypted_events).with(limit: batch_size).and_return(
          encrypted_events,
        )
        expect(job).to receive(:process_encrypted_events_bulk).once

        job.perform
      end

      it 'accepts custom batch_size parameter' do
        custom_batch_size = 500
        expect(job).to receive(:fetch_encrypted_events).
          with(limit: custom_batch_size).and_return([])

        job.perform(batch_size: custom_batch_size)
      end
    end

    context 'when processing multiple batches' do
      let(:full_batch) { Array.new(batch_size) { encrypted_events.first } }
      let(:partial_batch) { [encrypted_events.first] }

      before do
        allow(job).to receive(:process_encrypted_events_bulk).
          and_return(batch_size, 1)
        allow(Rails.logger).to receive(:info)
      end

      it 'continues processing until batch is incomplete' do
        # The loop breaks after partial_batch because partial_batch.size (1) < batch_size (1000)
        expect(job).to receive(:fetch_encrypted_events).with(limit: batch_size).
          and_return(full_batch, partial_batch)
        expect(job).to receive(:process_encrypted_events_bulk).twice

        job.perform
      end
    end

    context 'when an error occurs' do
      let(:error_message) { 'Database connection failed' }

      before do
        allow(job).to receive(:fetch_encrypted_events).and_raise(StandardError, error_message)
      end

      it 'logs error and re-raises exception' do
        expect(Rails.logger).to receive(:error)
        expect { job.perform }.to raise_error(StandardError, error_message)
      end
    end
  end

  describe '#fetch_encrypted_events' do
    let(:limit) { 1000 }
    let(:expected_query_pattern) do
      %r{SELECT\ event_key,\ message\ FROM\ fraudops\.encrypted_events
         \s+WHERE\ processed_timestamp\ IS\ NULL
         \s+ORDER\ BY\ event_key\ LIMIT}x
    end
    let(:query_result) { instance_double(ActiveRecord::Result) }

    before do
      allow(query_result).to receive(:to_a).and_return(encrypted_events)
      allow(ActiveRecord::Base).to receive(:send).
        with(:sanitize_sql_array, anything).and_call_original
    end

    it 'executes the correct SQL query with limit' do
      expect(mock_connection).to receive(:execute).
        with(a_string_matching(expected_query_pattern)).
        and_return(query_result)

      result = job.send(:fetch_encrypted_events, limit: limit)
      expect(result).to eq(encrypted_events)
    end

    it 'sanitizes SQL with proper limit parameter' do
      expect(ActiveRecord::Base).to receive(:send).
        with(:sanitize_sql_array, array_including(limit))

      allow(mock_connection).to receive(:execute).and_return(query_result)
      job.send(:fetch_encrypted_events, limit: limit)
    end
  end

  describe '#process_encrypted_events_bulk' do
    let(:decrypted_events) do
      [
        { event_key: 'event_1', message: sample_event_data },
        { event_key: 'event_2', message: sample_event_data },
      ]
    end
    let(:successful_ids) { ['event_1', 'event_2'] }

    before do
      allow(job).to receive(:decrypt_events).and_return([decrypted_events, successful_ids])
      allow(job).to receive(:bulk_insert_decrypted_events)
      allow(job).to receive(:bulk_update_processed_timestamp)
    end

    context 'when events are empty' do
      it 'returns 0 without processing' do
        result = job.send(:process_encrypted_events_bulk, [])
        expect(result).to eq(0)
      end
    end

    context 'when no events decrypt successfully' do
      before do
        allow(job).to receive(:decrypt_events).and_return([[], []])
      end

      it 'logs info and returns 0' do
        expect(Rails.logger).to receive(:info)
        result = job.send(:process_encrypted_events_bulk, encrypted_events)
        expect(result).to eq(0)
      end
    end

    context 'when decryption is successful' do
      it 'performs bulk operations in a transaction' do
        expect(DataWarehouseApplicationRecord).to receive(:transaction).and_yield
        expect(job).to receive(:bulk_insert_decrypted_events).with(decrypted_events)
        expect(job).to receive(:bulk_update_processed_timestamp).with(successful_ids)

        job.send(:process_encrypted_events_bulk, encrypted_events)
      end

      it 'instruments the batch persistence' do
        expect(ActiveSupport::Notifications).to receive(:instrument).
          with('fraud_ops_pii_decrypt_job.persist_batch')

        job.send(:process_encrypted_events_bulk, encrypted_events)
      end

      it 'logs completion with counts' do
        expect(Rails.logger).to receive(:info).
          with(a_string_matching(/inserted_count.*2.*updated_count.*2/))

        job.send(:process_encrypted_events_bulk, encrypted_events)
      end

      it 'returns the count of successfully processed events' do
        result = job.send(:process_encrypted_events_bulk, encrypted_events)
        expect(result).to eq(2)
      end
    end

    context 'when bulk processing fails' do
      let(:db_error) { ActiveRecord::StatementInvalid.new('Constraint violation') }

      before do
        allow(job).to receive(:bulk_insert_decrypted_events).and_raise(db_error)
      end

      it 're-raises exception' do
        expect { job.send(:process_encrypted_events_bulk, encrypted_events) }.
          to raise_error(ActiveRecord::StatementInvalid)
      end
    end
  end

  describe '#decrypt_events' do
    before do
      allow(job).to receive(:private_key).and_return(private_key)
    end

    context 'when all events decrypt successfully' do
      before do
        allow(job).to receive(:decrypt_data).and_return(sample_event_data)
      end

      it 'returns decrypted events and successful IDs' do
        decrypted, ids = job.send(:decrypt_events, encrypted_events)

        expect(decrypted).to eq(
          [
            { event_key: 'event_1', message: sample_event_data },
            { event_key: 'event_2', message: sample_event_data },
          ],
        )
        expect(ids).to eq(['event_1', 'event_2'])
      end
    end

    context 'when some events fail to decrypt' do
      before do
        allow(job).to receive(:decrypt_data).and_return(sample_event_data, nil)
      end

      it 'only includes successfully decrypted events' do
        decrypted, ids = job.send(:decrypt_events, encrypted_events)

        expect(decrypted).to eq(
          [
            { event_key: 'event_1', message: sample_event_data },
          ],
        )
        expect(ids).to eq(['event_1'])
      end
    end
  end

  describe '#bulk_insert_decrypted_events' do
    let(:decrypted_events) do
      [
        { event_key: 'event_1', message: sample_event_data },
        { event_key: 'event_2', message: sample_event_data },
      ]
    end

    before do
      allow(ActiveRecord::Base).to receive(:send).
        with(:sanitize_sql_array, anything).and_call_original
      allow(JSON).to receive(:generate).and_call_original
    end

    context 'when using PostgreSQL adapter' do
      before do
        allow(job).to receive(:using_redshift_adapter?).and_return(false)
        allow(mock_connection).to receive(:execute)
      end

      it 'uses jsonb cast in insert statement' do
        expected_pattern = %r{INSERT\ INTO\ fraudops\.decrypted_events
                      \s*\(event_key,\ message\)
                      \s*VALUES.*::jsonb}x

        expect(mock_connection).to receive(:execute).
          with(a_string_matching(expected_pattern))

        job.send(:bulk_insert_decrypted_events, decrypted_events)
      end

      it 'logs successful insertion' do
        expect(Rails.logger).to receive(:info).with(a_string_matching(/row_count.*2/))
        job.send(:bulk_insert_decrypted_events, decrypted_events)
      end
    end

    context 'when using Redshift adapter' do
      before do
        allow(job).to receive(:using_redshift_adapter?).and_return(true)
        allow(mock_connection).to receive(:execute)
      end

      it 'uses JSON_PARSE in insert statement' do
        expected_pattern = %r{INSERT\ INTO\ fraudops\.decrypted_events
                      \s*\(event_key,\ message\)
                      \s*VALUES.*JSON_PARSE}x

        expect(mock_connection).to receive(:execute).
          with(a_string_matching(expected_pattern))

        job.send(:bulk_insert_decrypted_events, decrypted_events)
      end
    end

    context 'when decrypted_events is empty' do
      it 'returns early without executing query' do
        expect(mock_connection).not_to receive(:execute)

        job.send(:bulk_insert_decrypted_events, [])
      end
    end
  end

  describe '#bulk_update_processed_timestamp' do
    let(:event_ids) { ['event_1', 'event_2'] }

    before do
      allow(ActiveRecord::Base).to receive(:send).
        with(:sanitize_sql_array, anything).and_call_original
    end

    context 'when update is successful' do
      it 'executes update query with all event IDs' do
        expected_pattern = %r{UPDATE\ fraudops\.encrypted_events
                      \s+SET\ processed_timestamp\ =\ CURRENT_TIMESTAMP
                      \s+WHERE\ event_key\ IN}x

        expect(mock_connection).to receive(:execute).
          with(a_string_matching(expected_pattern))

        job.send(:bulk_update_processed_timestamp, event_ids)
      end

      it 'logs successful update' do
        allow(mock_connection).to receive(:execute)
        expect(Rails.logger).to receive(:info).with(a_string_matching(/updated_count.*2/))

        job.send(:bulk_update_processed_timestamp, event_ids)
      end
    end

    context 'when event_ids is empty' do
      it 'returns early without executing query' do
        expect(mock_connection).not_to receive(:execute)

        job.send(:bulk_update_processed_timestamp, [])
      end
    end
  end

  describe '#decrypt_data' do
    let(:encrypted_data) { 'encrypted_jwe_token' }
    let(:decrypted_json) { sample_event_data.to_json }
    let(:event_key) { 'event_123' }

    context 'when decryption is successful' do
      before do
        allow(JWE).to receive(:decrypt).with(encrypted_data, private_key).and_return(decrypted_json)
      end

      it 'returns parsed JSON data with symbolized keys' do
        result = job.send(:decrypt_data, encrypted_data, private_key, event_key)
        expect(result).to eq(sample_event_data)
      end
    end

    context 'when decryption fails' do
      let(:jwe_error) { StandardError.new('Invalid JWE token') }

      before do
        allow(JWE).to receive(:decrypt).and_raise(jwe_error)
      end

      it 'logs error with event_key and returns nil' do
        expect(Rails.logger).to receive(:error).with(
          a_string_matching(/event_key.*#{event_key}/),
        )

        result = job.send(:decrypt_data, encrypted_data, private_key, event_key)
        expect(result).to be_nil
      end
    end

    context 'when JSON parsing fails' do
      before do
        allow(JWE).to receive(:decrypt).and_return('invalid json')
      end

      it 'logs error with event_key and returns nil' do
        expect(Rails.logger).to receive(:error).with(
          a_string_matching(/event_key.*#{event_key}/),
        )

        result = job.send(:decrypt_data, encrypted_data, private_key, event_key)
        expect(result).to be_nil
      end
    end
  end
end
