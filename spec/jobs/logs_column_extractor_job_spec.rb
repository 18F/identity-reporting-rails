require 'rails_helper'
require 'factory_bot'

RSpec.describe LogsColumnExtractorJob, type: :job do
  let(:logs_job) { LogsColumnExtractorJob.new }
  let(:uuids) { ['07324b45-e043-45cd-8d17-33e8cc8c54e0', '07324b45-e043-45cd-8d17-33e8cc8c54e1'] }

  describe 'concurrency key' do
    it 'locks per logs target table' do
      job = LogsColumnExtractorJob.new('events')

      expect(job.good_job_concurrency_key).to eq('LogsColumnExtractorJob-default-logs-events')
    end
  end

  describe '#perform' do
    context 'when target table name is not recognized' do
      it 'confirm the job will not run' do
        allow(Rails.logger).to receive(:info).and_call_original
        msg = '{"job":"LogsColumnExtractorJob",' \
              '"success":false,' \
              '"message":"Invalid table name",' \
              '"schema_name":"logs",' \
              '"source_table_name":"unextracted_random_name"}'
        expect(Rails.logger).to receive(:info).with(msg)
        logs_job.perform('random_name')
      end
    end

    context 'when no data in tables' do
      it 'confirm the job does not execute any query' do
        allow(Rails.logger).to receive(:info).and_call_original
        msg = '{"job":"LogsColumnExtractorJob",' \
              '"success":false,' \
              '"message":"Missing data in source table",' \
              '"schema_name":"logs",' \
              '"source_table_name":"unextracted_events"}'
        expect(Rails.logger).to receive(:info).with(msg)
        logs_job.perform('events')
      end
    end

    context 'when target table is events' do
      before do
        uuids.each do |uuid|
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
                locale: 'en',
                user_ip: '50.116.19.65',
                hostname: 'idp.agnes.identitysandbox.gov',
                pid: 10330,
                service_provider: nil,
                trace_id: 'Root=1-66579145-6437417b34c76feb7f9e767b',
                git_sha: '78b1b6bb',
                git_branch: 'main',
                user_agent:
                  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                browser_name: 'Chrome',
                browser_version: '101.0.4951.61',
                browser_platform_name: 'Windows',
                browser_platform_version: '10.0',
                browser_device_name: 'Unknown',
                browser_mobile: false,
                browser_bot: false,
              },
              time: '2024-05-29T20:34:13.334Z',
              id: uuid,
              visitor_id: 'c41b33a5-f48b-42b9-aabd-617cec0db776',
              visit_id: '4f67f9da-2dc3-4049-86ce-49f005441091',
              log_filename: 'events.log',
            },
          )
        end
      end
      it 'confirm the column extraction is successful' do
        allow(Rails.logger).to receive(:info).and_call_original
        msg = '{"job":"LogsColumnExtractorJob",' \
              '"success":true,' \
              '"message":"Query executed successfully",' \
              '"schema_name":"logs",' \
              '"source_table_name":"unextracted_events",' \
              '"target_table_name":"events"}'
        expect(Rails.logger).to receive(:info).with(msg)
        logs_job.perform('events')
        query = 'Select * from logs.events;'
        query_results = DataWarehouseApplicationRecord.connection.execute(query).to_a
        expect(query_results.length).to eq(2)
        expect(query_results.first['id']).to eq(uuids.first)
        expect(query_results.last['id']).to eq(uuids.last)
        expect(query_results.first['browser_mobile']).to eq(false)
        expect(query_results.first['pid']).to eq(10330)
        expect(query_results.first['service_provider']).to eq(nil)
      end
    end

    context 'when target table is production' do
      before do
        uuids.each do |uuid|
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
              uuid: uuid,
              pid: 8932,
              user_agent: 'ELB-HealthChecker/2.0',
              ip: '100.106.65.246',
              host: '100.106.122.235',
              trace_id: nil,
            },
          )
        end
      end
      it 'confirm the column extraction is successful' do
        allow(Rails.logger).to receive(:info).and_call_original
        msg = '{"job":"LogsColumnExtractorJob",' \
              '"success":true,"message":"Query executed successfully",' \
              '"schema_name":"logs",' \
              '"source_table_name":"unextracted_production",' \
              '"target_table_name":"production"}'
        expect(Rails.logger).to receive(:info).with(msg)
        logs_job.perform('production')
        query = 'Select * from logs.production;'
        query_results = DataWarehouseApplicationRecord.connection.execute(query).to_a
        expect(query_results.length).to eq(2)
        expect(query_results.first['uuid']).to eq(uuids.first)
        expect(query_results.last['uuid']).to eq(uuids.last)
        expect(query_results.first['duration']).to eq(2.31)
        expect(query_results.first['git_branch']).to eq('main')
        expect(query_results.first['trace_id']).to eq(nil)
      end
    end

    context 'when error happens while executing the job' do
      before do
        uuids.each do |uuid|
          FactoryBot.create(
            :unextracted_event,
            cloudwatch_timestamp: '2024-05-29T20:34:17.933Z',
            message: {},
          )
        end
      end
      it 'confirm the column extraction is unsuccessful' do
        allow(Rails.logger).to receive(:info).and_call_original
        msg_regex = /"job":"LogsColumnExtractorJob","success":false,"message":"PG::NotNullViolation/
        expect(Rails.logger).to receive(:info).with(match(msg_regex))
        logs_job.perform('events')
      end
    end
  end
end
