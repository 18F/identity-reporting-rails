require 'rails_helper'

RSpec.describe FcmsPiiDecryptJob, type: :job do
  let(:private_key_pem) { OpenSSL::PKey::RSA.new(2048).to_pem }
  let(:private_key) { OpenSSL::PKey::RSA.new(private_key_pem) }
  let(:job) { described_class.new }
  let(:mock_connection) { double('connection') }

  before do
    allow(DataWarehouseApplicationRecord).to receive(:connection).and_return(mock_connection)
    allow(Rails.logger).to receive(:info)
  end

  describe '#perform' do
    context 'when fraud_ops_tracker_enabled is false' do
      before do
        allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(false)
      end

      it 'skips processing and logs the reason' do
        expect(Rails.logger).to receive(:info).with(
          'FcmsPiiDecryptJob: Skipped because fraud_ops_tracker_enabled is false',
        )
        expect(job).not_to receive(:fetch_insert_delete_data_from_redshift)
        expect(job).not_to receive(:insert_data_to_redshift_events)

        job.perform(private_key_pem)
      end
    end

    context 'when fraud_ops_tracker_enabled is true' do
      before do
        allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(true)
        allow(job).to receive(:fetch_insert_delete_data_from_redshift)
        allow(job).to receive(:insert_data_to_redshift_events)
      end

      it 'processes the job successfully' do
        expect(job).to receive(:fetch_insert_delete_data_from_redshift)
        expect(job).to receive(:insert_data_to_redshift_events).with(kind_of(OpenSSL::PKey::RSA))
        expect(job).to receive(:log_info).with('FcmsPiiDecryptJob: Job completed', true)

        job.perform(private_key_pem)
      end

      it 'creates RSA private key from PEM string' do
        expect(OpenSSL::PKey::RSA).to receive(:new).with(private_key_pem).and_call_original

        job.perform(private_key_pem)
      end

      context 'when an error occurs' do
        let(:error_message) { 'Database connection failed' }
        let(:error) { StandardError.new(error_message) }

        before do
          allow(job).to receive(:fetch_insert_delete_data_from_redshift).and_raise(error)
        end

        it 'logs the error and re-raises it' do
          expect(job).to receive(:log_info).with(
            'FcmsPiiDecryptJob: Job failed',
            false,
            { error: error_message },
          )

          expect { job.perform(private_key_pem) }.to raise_error(StandardError, error_message)
        end
      end
    end
    # end

    # describe '#decrypt_jwt' do
    #   let(:encrypted_jwt) { 'encrypted.jwt.token' }
    #   let(:decrypted_data) { { 'jti' => '123', 'data' => 'test' } }
    #   let(:decrypted_jwt_string) { decrypted_data.to_json }

    #   it 'decrypts JWT and returns parsed JSON' do
    #     expect(JWE).to receive(:decrypt).with(
    #       encrypted_jwt,
    #       private_key,
    #     ).and_return(decrypted_jwt_string)
    #     expect(JSON).to receive(:parse).with(decrypted_jwt_string).and_return(decrypted_data)

    #     result = job.send(:decrypt_jwt, encrypted_jwt, private_key)
    #     expect(result).to eq(decrypted_data)
    #   end
    # end

    # describe '#fetch_insert_delete_data_from_redshift' do
    #   let(:expected_query) do
    #     <<-SQL
    #     WITH moved_records AS (
    #       DELETE FROM fcms.unextracted_events
    #       RETURNING message
    #     )
    #     INSERT INTO fcms.encrypted_events (message, import_timestamp)
    #     SELECT message, CURRENT_TIMESTAMP FROM moved_records;
    #     SQL
    #   end
    #   let(:query_result) { [{ 'message' => 'test_message' }] }

    #   context 'when query executes successfully' do
    #     before do
    #       allow(mock_connection).to receive(:exec_query).with(expected_query).and_return(
    #         double('result', to_a: query_result),
    #       )
    #     end

    #     it 'executes the move query and logs success' do
    #       expect(mock_connection).to receive(:exec_query).with(expected_query)
    #       expect(job).to receive(:log_info).with(
    #         'FcmsPiiDecryptJob: Data fetch from unextracted_events to encrypted_events succeeded',
    #         true,
    #       )

    #       result = job.send(:fetch_insert_delete_data_from_redshift)
    #       expect(result).to eq(query_result)
    #     end
    #   end

    #   context 'when query fails' do
    #     let(:error) { ActiveRecord::StatementInvalid.new('SQL error') }

    #     before do
    #       allow(mock_connection).to receive(:exec_query).and_raise(error)
    #     end

    #     it 'logs the error and re-raises it' do
    #       expect(job).to receive(:log_info).with(
    #         'FcmsPiiDecryptJob: Data fetch from unextracted_events to encrypted_events failed',
    #         false,
    #         { error: 'SQL error' },
    #       )

    #       expect do
    #         job.send(:fetch_insert_delete_data_from_redshift)
    #       end.to raise_error(ActiveRecord::StatementInvalid)
    #     end
    #   end
    # end

    # describe '#insert_data_to_redshift_events' do
    #   let(:encrypted_events_query) do
    #     'SELECT message FROM fcms.encrypted_events WHERE processed_timestamp IS NULL'
    #   end

    #   context 'when there are no encrypted events' do
    #     before do
    #       allow(mock_connection).to receive(:exec_query).with(encrypted_events_query).and_return(
    #         double('result', to_a: []),
    #       )
    #     end

    #     it 'returns early without processing' do
    #       expect(job).not_to receive(:decrypt_jwt)
    #       expect(mock_connection).not_to receive(:execute)

    #       job.send(:insert_data_to_redshift_events, private_key)
    #     end
    #   end

    #   context 'when there are encrypted events to process' do
    #     let(:encrypted_events) do
    #       [
    #         { 'message' => 'encrypted_jwt_1' },
    #         { 'message' => 'encrypted_jwt_2' },
    #       ]
    #     end
    #     let(:decrypted_message_1) { { 'jti' => 'jti_1', 'data' => 'data_1' } }
    #     let(:decrypted_message_2) { { 'jti' => 'jti_2', 'data' => 'data_2' } }

    #     before do
    #       allow(mock_connection).to receive(:exec_query).with(encrypted_events_query).and_return(
    #         double('result', to_a: encrypted_events),
    #       )
    #       allow(job).to receive(:decrypt_jwt).with(
    #         'encrypted_jwt_1',
    #         private_key,
    #       ).and_return(decrypted_message_1)
    #       allow(job).to receive(:decrypt_jwt).with(
    #         'encrypted_jwt_2',
    #         private_key,
    #       ).and_return(decrypted_message_2)
    #       allow(ActiveRecord::Base.connection).to receive(:quote).with('jti_1').and_return("'jti_1'")
    #       allow(ActiveRecord::Base.connection).to receive(:quote).with('jti_2').and_return("'jti_2'")
    #       allow(ActiveRecord::Base.connection).to receive(:quote).with(decrypted_message_1.to_json).and_return("'#{decrypted_message_1.to_json}'")
    #       allow(ActiveRecord::Base.connection).to receive(:quote).with(decrypted_message_2.to_json).and_return("'#{decrypted_message_2.to_json}'")
    #       allow(job).to receive(:update_encrypted_events_processed)
    #     end

    #     context 'when insert succeeds' do
    #       it 'decrypts messages and inserts them into events table' do
    #         expect(mock_connection).to receive(:execute) do |query|
    #           expect(query).to include('INSERT INTO fcms.events (jti, message, import_timestamp)')
    #           expect(query).to include('ON CONFLICT (jti) DO NOTHING')
    #           expect(query).to include("'jti_1'")
    #           expect(query).to include("'jti_2'")
    #         end

    #         expect(job).to receive(:log_info).with(
    #           'FcmsPiiDecryptJob: Data insert to Redshift events succeeded',
    #           true,
    #           { row_count: 2 },
    #         )
    #         expect(job).to receive(:update_encrypted_events_processed)

    #         job.send(:insert_data_to_redshift_events, private_key)
    #       end
    #     end

    #     context 'when insert fails' do
    #       let(:error) { ActiveRecord::StatementInvalid.new('Insert failed') }

    #       before do
    #         allow(mock_connection).to receive(:execute).and_raise(error)
    #       end

    #       it 'logs the error and re-raises it' do
    #         expect(job).to receive(:log_info).with(
    #           'FcmsPiiDecryptJob: Data insert to Redshift events failed',
    #           false,
    #           { error: 'Insert failed' },
    #         )

    #         expect do
    #           job.send(
    #             :insert_data_to_redshift_events,
    #             private_key,
    #           )
    #         end.to raise_error(ActiveRecord::StatementInvalid)
    #       end
    #     end

    #     context 'when values array is empty after processing' do
    #       before do
    #         allow(job).to receive(:decrypt_jwt).and_return({ 'jti' => nil })
    #         # This would cause the values array to be empty after filtering
    #         encrypted_events_empty = []
    #         allow_any_instance_of(Array).to receive(:map).and_return([])
    #         allow_any_instance_of(Array).to receive(:join).and_return('')
    #         allow_any_instance_of(String).to receive(:empty?).and_return(true)
    #       end

    #       it 'logs no new events message and returns' do
    #         # Override the stubbing to make values empty
    #         allow_any_instance_of(described_class).to receive(:insert_data_to_redshift_events) do |instance, key|
    #           # Simulate empty values scenario
    #           instance.send(:log_info, 'FcmsPiiDecryptJob: No new encrypted events to process', true)
    #           return
    #         end

    #         job.send(:insert_data_to_redshift_events, private_key)
    #       end
    #     end
    #   end
    # end

    # describe '#update_encrypted_events_processed' do
    #   let(:update_query) do
    #     <<~SQL
    #       UPDATE fcms.encrypted_events
    #       SET processed_timestamp = CURRENT_TIMESTAMP
    #       WHERE processed_timestamp IS NULL
    #     SQL
    #   end

    #   context 'when update succeeds' do
    #     it 'updates processed_timestamp and logs success' do
    #       expect(mock_connection).to receive(:execute).with(update_query)
    #       expect(job).to receive(:log_info).with(
    #         'FcmsPiiDecryptJob: Updated processed_timestamp in encrypted_events',
    #         true,
    #       )

    #       job.send(:update_encrypted_events_processed)
    #     end
    #   end

    #   context 'when update fails' do
    #     let(:error) { ActiveRecord::StatementInvalid.new('Update failed') }

    #     before do
    #       allow(mock_connection).to receive(:execute).and_raise(error)
    #     end

    #     it 'logs the error and re-raises it' do
    #       expect(job).to receive(:log_info).with(
    #         'FcmsPiiDecryptJob: Failed to update processed_timestamp in encrypted_events',
    #         false,
    #         { error: 'Update failed' },
    #       )

    #       expect do
    #         job.send(:update_encrypted_events_processed)
    #       end.to raise_error(ActiveRecord::StatementInvalid)
    #     end
    #   end
    # end

    # describe '#log_info' do
    #   let(:message) { 'Test message' }
    #   let(:success) { true }
    #   let(:additional_info) { { row_count: 5 } }
    #   let(:expected_log_data) do
    #     {
    #       job: 'FcmsPiiDecryptJob',
    #       success: true,
    #       message: 'Test message',
    #       row_count: 5,
    #     }
    #   end

    #   it 'logs structured JSON data' do
    #     expect(Rails.logger).to receive(:info).with(expected_log_data.to_json)

    #     job.send(:log_info, message, success, additional_info)
    #   end

    #   it 'works without additional_info' do
    #     expected_log_data_minimal = {
    #       job: 'FcmsPiiDecryptJob',
    #       success: true,
    #       message: 'Test message',
    #     }

    #     expect(Rails.logger).to receive(:info).with(expected_log_data_minimal.to_json)

    #     job.send(:log_info, message, success)
    #   end
    # end

    # # Integration-style test
    # describe 'job queue configuration' do
    #   it 'is queued on the default queue' do
    #     expect(described_class.queue_name).to eq('default')
    #   end
    # end

    # describe 'error handling integration' do
    #   before do
    #     allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(true)
    #   end

    #   it 'handles OpenSSL::PKey::RSAError when creating private key' do
    #     invalid_pem = 'invalid-pem-string'

    #     expect { job.perform(invalid_pem) }.to raise_error(OpenSSL::PKey::RSAError)
    #   end
  end
end
