require 'rails_helper'
require 'factory_bot'

RSpec.describe PiiAccessDetectionJob, type: :job do
  let(:rails_job) { PiiAccessDetectionJob.new }
  let(:group_mappings) do
    {
      lg_admins: ['admin_user1', 'admin_user2'],
      lg_users: ['regular_user1', 'regular_user2'],
      lg_powerusers: ['power_user1', 'power_user2'],
    }
  end
  let(:system_users) do
    ['dbt_connector', 'fraud_ops_connector', 'rails_job_connector']
  end

  describe '#perform' do
    before do
      allow(Identity::Hostdata).to receive(:env).and_return('testenv')
      # Create users and assign to groups
      group_mappings.each do |group, users|
        # Create group
        DataWarehouseApplicationRecord.connection.execute(
          <<~SQL,
            CREATE group #{group};
          SQL
        )
        # Create users and assign to group
        users.each do |user|
          DataWarehouseApplicationRecord.connection.execute(
            <<~SQL,
              CREATE USER "#{user}";
            SQL
          )
          DataWarehouseApplicationRecord.connection.execute(
            <<~SQL,
              ALTER GROUP #{group} ADD USER #{user};
            SQL
          )
        end
      end

      # Create system users
      system_users.each do |user|
        DataWarehouseApplicationRecord.connection.execute(
          <<~SQL,
            CREATE USER "#{user}";
          SQL
        )
      end

      # Create sys_query_text & svv_user_info entries to simulate Redshift system tables
      # Using minimal schema for testing
      DataWarehouseApplicationRecord.connection.execute(
        <<~SQL,
          CREATE TABLE sys_query_text (
            user_id INT,
            query_id INT,
            sequence INT,
            text VARCHAR(65535),
            start_time TIMESTAMP
          );
        SQL
      )
      DataWarehouseApplicationRecord.connection.execute(
        <<~SQL,
          CREATE TABLE svv_user_info (
            user_id INT,
            user_name VARCHAR(255)
          );
        SQL
      )
      DataWarehouseApplicationRecord.connection.execute(
        <<~SQL,
          INSERT INTO svv_user_info (user_id, user_name) VALUES
          (1, 'IAM:john.doe'),
          (2, 'IAM:johnny.appleseed'),
          (3, 'IAMR:testenv_dbt_connector');
        SQL
      )
    end

    context 'when pii data was accessed' do
      before do
        allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(true)
        # Insert records into sys_query_text
        DataWarehouseApplicationRecord.connection.execute(
          <<~SQL,
            INSERT INTO sys_query_text (user_id, query_id, sequence, text, start_time) VALUES
            (1, 1001, 1, 'SELECT * FROM fraudops.fraud_ops_events;', NOW() - INTERVAL '5 minutes'),
            (2, 1002, 1, 'SELECT id, email FROM idp_pii.email_addresses_pii;', NOW() - INTERVAL '5 minutes'),
            (3, 1003, 1, 'SELECT * FROM fraudops.fraud_ops_events;', NOW() - INTERVAL '5 minutes');
          SQL
        )
      end

      it 'logs PII access for non-approved service users' do
        allow(Rails.logger).to receive(:warn).and_call_original
        msg1 = {
          job: 'PiiAccessDetectionJob',
          success: false,
          message: 'Potential PII access detected',
          user_name: 'IAM:john.doe',
          query_id: 1001,
          table_accessed: 'fraud_ops_events',
        }
        msg2 = {
          job: 'PiiAccessDetectionJob',
          success: false,
          message: 'Potential PII access detected',
          user_name: 'IAM:johnny.appleseed',
          query_id: 1002,
          table_accessed: 'email_addresses_pii',
        }
        expect(Rails.logger).to receive(:warn).with(msg1.to_json)
        expect(Rails.logger).to receive(:warn).with(msg2.to_json)
        rails_job.perform
      end
    end

    context 'when pii data was not accessed' do
      before do
        allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(true)
        # Insert records into sys_query_text
        DataWarehouseApplicationRecord.connection.execute(
          <<~SQL,
            INSERT INTO sys_query_text (user_id, query_id, sequence, text, start_time) VALUES
            (1, 1001, 1, 'SELECT * FROM idp.email_addresses;', NOW() - INTERVAL '5 minutes'),
            (2, 1002, 1, 'SELECT uuid, user_id, duration FROM logs.production;', NOW() - INTERVAL '5 minutes'),
            (3, 1003, 1, 'SELECT * FROM fraudops.fraud_ops_events;', NOW() - INTERVAL '5 minutes');
          SQL
        )
      end

      it 'does not log PII access' do
        allow(Rails.logger).to receive(:warn).and_call_original
        expect(Rails.logger).not_to receive(:warn)
        rails_job.perform
      end
    end

    context 'when fraud_ops_tracker_enabled flag is false' do
      before do
        allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(false)
      end

      it 'does not run the job' do
        allow(Rails.logger).to receive(:info).and_call_original
        msg = {
          job: 'PiiAccessDetectionJob',
          success: false,
          message: 'fraud_ops_tracker_enabled is false, skipping job.',
        }
        expect(Rails.logger).to receive(:info).with(msg.to_json)
        rails_job.perform
      end
    end
  end
end
