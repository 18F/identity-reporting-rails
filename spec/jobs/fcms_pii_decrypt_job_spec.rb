require 'rails_helper'

RSpec.describe FcmsPiiDecryptJob, type: :job do
  let(:job) { described_class.new }
  let(:private_key) { OpenSSL::PKey::RSA.generate(2048) }
  let(:private_key_pem) { private_key.to_pem }
  let(:mock_connection) { instance_double(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter) }
  let(:sample_event_data) { { user_id: 123, action: 'login' } }
  let(:encrypted_message) { 'encrypted_data_string' }

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
  end

  describe '#perform' do
    context 'when job is disabled' do
      before do
        allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(false)
      end

      it 'skips job execution and logs info' do
        expect(JobHelpers::LogHelper).to receive(:log_info).
          with('Skipped because fraud_ops_tracker_enabled is false')

        job.perform
      end
    end

    context 'when no encrypted events exist' do
      before do
        allow(job).to receive(:fetch_encrypted_events).and_return([])
      end

      it 'logs info and returns early' do
        expect(JobHelpers::LogHelper).to receive(:log_info).
          with('No encrypted events to process')

        job.perform
      end
    end

    context 'when encrypted events exist' do
      before do
        allow(job).to receive(:fetch_encrypted_events).and_return(encrypted_events)
        allow(job).to receive(:decrypt_data).and_return(sample_event_data)
        allow(job).to receive(:insert_decrypted_events)
        allow(job).to receive(:mark_events_as_processed)
      end

      it 'processes events successfully' do
        expect(JobHelpers::LogHelper).to receive(:log_success).
          with('Job completed', total_events: 2, successfully_processed: 2)

        job.perform
      end

      it 'uses the configured private key' do
        expect(IdentityConfig.store).
          to receive(:fraud_ops_private_key).and_return(private_key_pem)
        expect(OpenSSL::PKey::RSA).to receive(:new).with(private_key_pem)

        job.perform
      end
    end

    context 'when an error occurs' do
      let(:error_message) { 'Database connection failed' }

      before do
        allow(job).to receive(:fetch_encrypted_events).and_raise(StandardError, error_message)
      end

      it 'logs error and re-raises exception' do
        expect(JobHelpers::LogHelper).to receive(:log_error).
          with('Job failed', error: error_message)

        expect { job.perform }.to raise_error(StandardError, error_message)
      end
    end
  end

  describe '#fetch_encrypted_events' do
    let(:expected_query) do
      'SELECT event_key, message ' \
        'FROM fcms.encrypted_events ' \
        'WHERE processed_timestamp IS NULL'
    end
    let(:query_result) { instance_double(ActiveRecord::Result) }

    before do
      allow(query_result).to receive(:to_a).and_return(encrypted_events)
    end

    it 'executes the correct SQL query' do
      expect(mock_connection).to receive(:execute).with(expected_query).and_return(query_result)

      result = job.send(:fetch_encrypted_events)
      expect(result).to eq(encrypted_events)
    end
  end

  describe '#process_encrypted_events' do
    context 'when decryption is successful' do
      before do
        allow(job).to receive(:decrypt_data).and_return(sample_event_data)
        allow(job).to receive(:insert_decrypted_events)
      end

      it 'returns successfully processed event IDs' do
        result = job.send(:process_encrypted_events, encrypted_events, private_key)
        expect(result).to eq(['event_1', 'event_2'])
      end

      it 'calls insert_decrypted_events with correct data' do
        expected_decrypted_events = [
          {
            event_key: 'event_1',
            message: sample_event_data,
          },
          {
            event_key: 'event_2',
            message: sample_event_data,
          },
        ]

        expect(job).to receive(:insert_decrypted_events).with(expected_decrypted_events)

        job.send(:process_encrypted_events, encrypted_events, private_key)
      end
    end

    context 'when decryption fails for some events' do
      before do
        allow(job).to receive(:decrypt_data).and_return(sample_event_data, nil)
        allow(job).to receive(:insert_decrypted_events)
      end

      it 'logs failure and skips failed events' do
        expect(JobHelpers::LogHelper).to receive(:log_info).
          with('Failed to decrypt event', event_key: 'event_2')

        result = job.send(:process_encrypted_events, encrypted_events, private_key)
        expect(result).to eq(['event_1'])
      end

      it 'only inserts successfully decrypted events' do
        expected_decrypted_events = [
          {
            event_key: 'event_1',
            message: sample_event_data,
          },
        ]

        expect(job).to receive(:insert_decrypted_events).with(expected_decrypted_events)

        job.send(:process_encrypted_events, encrypted_events, private_key)
      end
    end
  end

  describe '#insert_decrypted_events' do
    let(:decrypted_events) do
      [
        {
          event_key: 'event_1',
          message: sample_event_data,
        },
      ]
    end

    before do
      allow(mock_connection).to receive(:quote).with('event_1').and_return("'event_1'")
      allow(mock_connection).
        to receive(:quote).
        with(sample_event_data.to_json).
        and_return("'#{sample_event_data.to_json}'")
    end

    context 'when insertion is successful' do
      it 'executes insert query and logs success' do
        expected_sanitized_sql = "INSERT INTO fcms.events (event_key, message) VALUES " \
                                  "('event_1', JSON_PARSE('{\"user_id\":123," \
                                  "\"action\":\"login\"}'));"

        expect(mock_connection).to receive(:execute).with(expected_sanitized_sql)
        expect(JobHelpers::LogHelper).to receive(:log_success).
          with('Data inserted to events table', row_count: 1)

        job.send(:insert_decrypted_events, decrypted_events)
      end
    end

    context 'when insertion fails' do
      let(:db_error) { ActiveRecord::StatementInvalid.new('Unique constraint violation') }

      before do
        allow(mock_connection).to receive(:execute).and_raise(db_error)
      end

      it 'logs error and re-raises exception' do
        expect(JobHelpers::LogHelper).to receive(:log_error).
          with('Failed to insert data to events table', error: db_error.message)

        expect { job.send(:insert_decrypted_events, decrypted_events) }.
          to raise_error(ActiveRecord::StatementInvalid)
      end
    end

    context 'when decrypted_events is empty' do
      it 'returns early without executing query' do
        expect(mock_connection).not_to receive(:execute)

        job.send(:insert_decrypted_events, [])
      end
    end
  end

  describe '#decrypt_data' do
    let(:encrypted_data) { 'encrypted_jwe_token' }
    let(:decrypted_json) { sample_event_data.to_json }

    context 'when decryption is successful' do
      before do
        allow(JWE).to receive(:decrypt).with(encrypted_data, private_key).and_return(decrypted_json)
      end

      it 'returns parsed JSON data' do
        result = job.send(:decrypt_data, encrypted_data, private_key)
        expect(result).to eq(sample_event_data)
      end
    end

    context 'when decryption fails' do
      let(:jwe_error) { StandardError.new('Invalid JWE token') }

      before do
        allow(JWE).to receive(:decrypt).and_raise(jwe_error)
      end

      it 'logs error and returns nil' do
        expect(JobHelpers::LogHelper).to receive(:log_error).
          with('Failed to decrypt data', error: jwe_error.message)

        result = job.send(:decrypt_data, encrypted_data, private_key)
        expect(result).to be_nil
      end
    end

    context 'when JSON parsing fails' do
      before do
        allow(JWE).to receive(:decrypt).with(encrypted_data, private_key).and_return('invalid json')
      end

      it 'logs error and returns nil' do
        expect(JobHelpers::LogHelper).to receive(:log_error).
          with('Failed to decrypt data', error: anything)

        result = job.send(:decrypt_data, encrypted_data, private_key)
        expect(result).to be_nil
      end
    end
  end

  describe '#mark_events_as_processed' do
    let(:event_ids) { ['event_1', 'event_2'] }

    before do
      allow(mock_connection).to receive(:quote).with('event_1').and_return("'event_1'")
      allow(mock_connection).to receive(:quote).with('event_2').and_return("'event_2'")
    end

    context 'when update is successful' do
      it 'executes update query and logs success' do
        expected_query = %r{
          UPDATE\ fcms\.encrypted_events\s+
          SET\ processed_timestamp\ =\ CURRENT_TIMESTAMP\s+
          WHERE\ event_key\ IN\ \('event_1',\ 'event_2'\)
        }x

        expect(mock_connection).to receive(:execute).with(a_string_matching(expected_query))
        expect(JobHelpers::LogHelper).to receive(:log_success).
          with('Updated processed_timestamp in encrypted_events', updated_count: 2)

        job.send(:mark_events_as_processed, event_ids)
      end
    end

    context 'when update fails' do
      let(:db_error) { ActiveRecord::StatementInvalid.new('Table not found') }

      before do
        allow(mock_connection).to receive(:execute).and_raise(db_error)
      end

      it 'logs error and re-raises exception' do
        expect(JobHelpers::LogHelper).to receive(:log_error).
          with('Failed to update processed_timestamp', error: db_error.message)

        expect { job.send(:mark_events_as_processed, event_ids) }.
          to raise_error(ActiveRecord::StatementInvalid)
      end
    end

    context 'when event_ids is empty' do
      it 'returns early without executing query' do
        expect(mock_connection).not_to receive(:execute)

        job.send(:mark_events_as_processed, [])
      end
    end
  end

  describe '#job_enabled?' do
    it 'returns the fraud_ops_tracker_enabled config value' do
      expect(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(true)

      expect(job.send(:job_enabled?)).to be true
    end
  end

  describe '#skip_job_execution' do
    it 'logs appropriate message' do
      expect(JobHelpers::LogHelper).to receive(:log_info).
        with('Skipped because fraud_ops_tracker_enabled is false')

      job.send(:skip_job_execution)
    end
  end

  describe '#private_key' do
    it 'returns an OpenSSL::PKey::RSA instance from the config' do
      expect(IdentityConfig.store).to receive(:fraud_ops_private_key).and_return(private_key_pem)
      expect(OpenSSL::PKey::RSA).to receive(:new).with(private_key_pem).and_return(private_key)

      result = job.send(:private_key)
      expect(result).to eq(private_key)
    end
  end

  describe '#connection' do
    before do
      # Reset the memoized connection
      job.instance_variable_set(:@connection, nil)
      allow(job).to receive(:connection).and_call_original
    end

    it 'returns DataWarehouseApplicationRecord connection' do
      expect(DataWarehouseApplicationRecord).to receive(:connection).and_return(mock_connection)

      result = job.send(:connection)
      expect(result).to eq(mock_connection)
    end

    it 'memoizes the connection' do
      expect(DataWarehouseApplicationRecord).
        to receive(:connection).once.and_return(mock_connection)

      # Call twice to test memoization
      job.send(:connection)
      job.send(:connection)
    end
  end
end
