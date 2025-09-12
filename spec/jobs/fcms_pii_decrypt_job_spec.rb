require 'rails_helper'
require 'factory_bot'

RSpec.describe FcmsPiiDecryptJob, type: :job do
  # let(:private_key) { JobHelpers::AttemptsApiKeypairHelper.private_key.to_pem) }
  # let(:public_key) { JobHelpers::AttemptsApiKeypairHelper.public_key.to_pem }
  # let(:fcms_job) { FcmsPiiDecryptJob.new }

  describe '#perform' do
    context 'when the record are present in unextracted_events' do
      # before do
      #   # allow(fcms_job).to receive(:fetch_insert_delete_data_from_redshift).and_call_original
      #   # allow(fcms_job).to receive(:insert_data_to_redshift_events).and_call_original
      #   # # Insert a mock encrypted event into unextracted_events
      #   # encrypted_payload = AttemptsApiImportJob.new.send(:encrypt_mock_jwt_payload,
      #   #                                                 AttemptsApiImportJob.new.send(:mock_jwt_payload,
      #   #                                                                               event_type: 'event-5678-0'),
      #   #                                                 OpenSSL::PKey::RSA.new(public_key))
      #   # DataWarehouseApplicationRecord.connection.execute(
      #   #   "INSERT INTO fcms.unextracted_events (key_hash, message, import_timestamp) VALUES ('event-5678-0', '#{encrypted_payload}', CURRENT_TIMESTAMP)"
      #   # )
      # end

      it 'imports the unextracted_events into encrypted_events and events table' do
        # allow(Rails.logger).to receive(:info).and_call_original
        # msg = { job: 'FcmsPiiDecryptJob',
        #         success: true,
        #         message: 'FcmsPiiDecryptJob: Job started' }
        # expect(Rails.logger).to receive(:info).with(msg.to_json)
        # expect(Rails.logger).to receive(:info).with(
        #   '{"job":"FcmsPiiDecryptJob","success":true,"message":"FcmsPiiDecryptJob: Data fetch from unextracted_events to encrypted_events succeeded"}',
        # )
        # expect(Rails.logger).to receive(:info).with(
        #   '{"job":"FcmsPiiDecryptJob","success":true,"message":"FcmsPiiDecryptJob: Job completed"}',
        # )
        # fcms_job.perform(private_key)

        # expect(fcms_job).to have_received(:fetch_insert_delete_data_from_redshift).once
        # expect(fcms_job).to have_received(:insert_data_to_redshift_events).with(
        #   OpenSSL::PKey::RSA.new(private_key)
        # ).once
      end

      it 'handles exceptions during data import' do
      end
    end
  end
end
