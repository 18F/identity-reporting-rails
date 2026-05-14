require 'rails_helper'
require 'factory_bot'

RSpec.describe PiiRowCheckerJob, type: :job do
  let(:pii_job) { PiiRowCheckerJob.new }

  describe 'concurrency key' do
    it 'locks per logs table' do
      job = PiiRowCheckerJob.new('events')

      expect(job.good_job_concurrency_key).to eq('PiiRowCheckerJob-default-logs-events')
    end
  end

  describe '#perform' do
    context 'when target table is unextracted_events' do
      let(:test_pattern) { 'fakey' }
      before do
        FactoryBot.create(
          :unextracted_event,
          cloudwatch_timestamp: '2024-05-29T20:34:17.933Z',
          message: {
            name: 'Sign in page visited',
            properties: {
              event_properties: {
                flash: nil,
              },
              new_event: true,
              path: '/',
              session_duration: 0.000758357,
              user_id: 'anonymous-uuid',
              browser_device_name: test_pattern,
              browser_mobile: false,
              browser_bot: false,
            },
            time: '2024-05-29T20:34:13.334Z',
            id: '07324b45-e043-45cd-8d17-33e8cc8c54e0',
            visitor_id: 'c41b33a5-f48b-42b9-aabd-617cec0db776',
            visit_id: '4f67f9da-2dc3-4049-86ce-49f005441091',
            log_filename: 'events.log',
          },
        )
      end
      context 'when name1 ' do
        it 'logs warning when pii pattern with name1' do
          expected_message =
            'PiiRowCheckerJob: Found name1 PII in logs.unextracted_events'
          expect(Rails.logger).to receive(:warn).with(expected_message)
          pii_job.perform('unextracted_events')
        end
      end
      context 'when name2 ' do
        it 'logs warning when pii pattern with name2' do
          expected_message =
            'PiiRowCheckerJob: Found name1 PII in logs.unextracted_events'
          expect(Rails.logger).to receive(:warn).with(expected_message)
          pii_job.perform('unextracted_events')
        end
      end
      context 'when dob dash' do
        let(:test_pattern) { '1938-10-06' }
        it 'logs warning when pii pattern with dob dash' do
          expected_message = 'PiiRowCheckerJob: Found dob_with_dash PII in logs.unextracted_events'
          expect(Rails.logger).to receive(:warn).with(expected_message)
          pii_job.perform('unextracted_events')
        end
      end
      context 'when dob slash' do
        let(:test_pattern) { '10/06/1938' }
        it 'logs warning when pii pattern with dob slash' do
          expected_message = 'PiiRowCheckerJob: Found dob_with_slash PII in logs.unextracted_events'
          expect(Rails.logger).to receive(:warn).with(expected_message)
          pii_job.perform('unextracted_events')
        end
      end
      context 'when address1' do
        let(:test_pattern) {  '1 MICROSOFT WAY' }
        it 'logs warning when pii pattern address1' do
          expected_message =
            'PiiRowCheckerJob: Found address_without_zipcode1 PII in logs.unextracted_events'
          expect(Rails.logger).to receive(:warn).with(expected_message)
          pii_job.perform('unextracted_events')
        end
      end
      context 'when address2' do
        let(:test_pattern) {  'baySide' }
        it 'logs warning when pii pattern address2' do
          expected_message =
            'PiiRowCheckerJob: Found address_without_zipcode2 PII in logs.unextracted_events'
          expect(Rails.logger).to receive(:warn).with(expected_message)
          pii_job.perform('unextracted_events')
        end
      end
    end

    context 'when target table is unextracted_production' do
      let(:test_pattern) { '314-555-1212' }
      before do
        FactoryBot.create(
          :unextracted_production,
          cloudwatch_timestamp: '2024-04-23T15:07:11.896Z',
          message: {
            method: 'GET',
            path: '/api/health',
            format: 'html',
            controller: 'Health::HealthController',
            action: 'index',
            status: 200,
            duration: 2.31,
            git_sha: '712c0ec1',
            git_branch: 'main',
            timestamp: '2024-04-23T15:07:11Z',
            uuid: '07324b45-e043-45cd-8d17-33e8cc8c54e0',
            pid: 8932,
            user_agent: test_pattern,
            ip: '100.106.65.246',
            host: '100.106.122.235',
            trace_id: nil,
          },
        )
      end
      context 'when phonenumber' do
        it 'log warning with pii phonenumber pattern' do
          expected_message =
            'PiiRowCheckerJob: Found phone_number PII in logs.unextracted_production'
          expect(Rails.logger).to receive(:warn).with(expected_message)
          pii_job.perform('unextracted_production')
        end
      end
      context 'when there is no pii' do
        let(:test_pattern) { 'ELB-HealthChecker/2.0' }
        it 'log info when no pii found' do
          expect(Rails.logger).not_to receive(:warn)
          pii_job.perform('unextracted_production')
        end
      end
    end

    context 'when no data in the table' do
      it 'confirm the job does not execute any queries and enqueue the logs extractor job' do
        allow(Rails.logger).to receive(:info).and_call_original
        expected_message = 'PiiRowCheckerJob: no date in logs.unextracted_events'
        expect(Rails.logger).to receive(:info).with(expected_message)
        pii_job.perform('unextracted_events')
      end
    end

    context 'when table name is not with unextracted_ ' do
      it 'confirm the job does not execute any queries and enqueue the logs column extractor job' do
        allow(Rails.logger).to receive(:info).and_call_original
        expected_message = 'PiiRowCheckerJob: unscoped table name logs.events'
        expected_message1 = 'PiiRowCheckerJob: Executing LogsColumnExtractorJob'
        expect(Rails.logger).to receive(:info).with(expected_message)
        expect(Rails.logger).to receive(:info).with(expected_message1)
        expect(LogsColumnExtractorJob).to receive(:perform_later).with('events')
        pii_job.perform('events')
      end
    end
  end
end
