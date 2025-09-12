require 'rails_helper'
require 'factory_bot'

RSpec.describe IDVRedisToRedshiftJob, type: :job do
  let(:fcms_job) { IDVRedisToRedshiftJob.new }

  describe '#perform' do
    context 'when mock api returns an encrypted event' do
      before do
        allow(fcms_job).to receive(:fetch_api_data).and_return(
          {
            sets: [
              { 'event-1234-0' => 'eyJhbGciOiJSU0EtT0FFUCIsImVuYyI6IkEyNTZHQ00iLCJ0eXAiOiJzZWNldmVudCtqd2UiLCJ6aXAiOiJERUYiLCJraWQiOiIxYmZiODYxMmE2NWU3NTkzOThlNGM0NGZmOWRkMWQ4OTZlNzBk' },
            ],
          }.with_indifferent_access,
        )
        # allow(FcmsPiiDecryptJob).to receive(:perform_now)
      end

      it 'imports the events into fcms.unextracted_events and triggers decryption job' do
        allow(Rails.logger).to receive(:info).and_call_original
        msg = { job: 'IDVRedisToRedshiftJob',
                success: true,
                message: 'IDVRedisToRedshiftJob: Job started' }
        expect(Rails.logger).to receive(:info).with(msg.to_json)
        expect(Rails.logger).to receive(:info).with(
          '{"job":"IDVRedisToRedshiftJob","success":true,"message":"IDVRedisToRedshiftJob: Processing 1 events"}',
        )
        expect(Rails.logger).to receive(:info).with(
          '{"job":"IDVRedisToRedshiftJob","success":true,"message":"IDVRedisToRedshiftJob: Data import to Redshift succeeded","row_count":1}',
        )
        expect(Rails.logger).to receive(:info).with('{"job":"IDVRedisToRedshiftJob","success":true,"message":"IDVRedisToRedshiftJob: Job completed"}')
        fcms_job.perform

        result = DataWarehouseApplicationRecord.connection.execute(
          "SELECT * FROM fcms.unextracted_events WHERE key_hash = 'event-1234-0'",
        ).to_a
        expect(result.length).to eq(1)
        expect(result.first['key_hash']).to eq('event-1234-0')
        expect(result.first['message']).to start_with('eyJhbGciOiJSU0EtT0FFUCIsImVuYyI6IkEyNTZHQ00iLCJ0eXAiOiJzZWNldmVudCtqd2UiLCJ6aXAiOiJERUYiLCJraWQiOiIxYmZiODYxMmE2NWU3NTkzOThlNGM0NGZmOWRkMWQ4OTZlNzBk')
      end

      it 'handles exceptions during data import' do
        allow(DataWarehouseApplicationRecord.connection).to receive(:execute).
          and_raise(ActiveRecord::StatementInvalid.new('DB error'))

        allow(Rails.logger).to receive(:info)

        expect { fcms_job.perform }.to raise_error(ActiveRecord::StatementInvalid, 'DB error')

        expect(Rails.logger).to have_received(:info).with(
          a_string_including('"success":false', 'Data import to Redshift failed', 'DB error'),
        )
      end
    end
  end
end
