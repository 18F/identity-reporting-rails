require 'rails_helper'

RSpec.describe RedshiftUnexpectedUserDetectionJob, type: :job do
  let(:rails_job) { RedshiftUnexpectedUserDetectionJob.new }
  let(:logger) { instance_double(IdentityJobLogSubscriber) }
  let(:log_entry) { instance_double(Logger) }
  let!(:user_config_path) { Rails.root.join('spec', 'fixtures', 'users.yml') }

  before do
    allow(IdentityJobLogSubscriber).to receive(:new).and_return(logger)
    allow(logger).to receive(:logger).and_return(log_entry)
    allow(Identity::Hostdata).to receive(:env).and_return('testenv')
  end

  describe '#perform' do
    context 'when local users exist in redshift but do not exist in the yaml file' do
      before do
        query = <<~SQL
          CREATE USER "IAM:kobe.bryant";
          CREATE USER "IAM:steph.curry";
          CREATE USER "lebron.james";
        SQL
        DataWarehouseApplicationRecord.connection.execute(query)
      end
      it 'then logs the new users detected' do
        expect(log_entry).to receive(:info).with(
          {
            name: 'RedshiftUnexpectedUserDetectionJob',
            # user lebron.james is in yml file but it doesn't have the IAM: prefix
            # therefore it is flagged as a new user outside of the user sync process
            unexpected_users_detected: 'IAM:kobe.bryant, IAM:steph.curry, lebron.james',
          }.to_json,
        )
        rails_job.perform(user_config_path)
      end
    end

    context 'when local users exist in both redshift and the yaml file' do
      before do
        query = <<~SQL
          CREATE USER "IAM:michael.jordan";
          CREATE USER "IAM:lebron.james";
        SQL
        DataWarehouseApplicationRecord.connection.execute(query)
      end
      it 'then no info log found' do
        expect(log_entry).not_to receive(:info)
        rails_job.perform(user_config_path)
      end
    end

    context 'when lambda and known admin usernames exist' do
      before do
        query = <<~SQL
          CREATE USER "IAMR:testenv_db_consumption";
          CREATE USER "IAMR:testenv_stale_data_check";
          CREATE USER "superuser";
          CREATE USER "rdsadmin";
          CREATE USER "rdsdb";
          CREATE USER "postgres";
          CREATE USER "security_audit";
        SQL
        DataWarehouseApplicationRecord.connection.execute(query)
      end
      it 'then these users should be ignored' do
        expect(log_entry).not_to receive(:info)
        rails_job.perform(user_config_path)
      end
    end

    context 'when the provided user config file path does not exist' do
      it 'then error log found' do
        expect(log_entry).to receive(:error).with(
          /No such file or directory/,
        )
        rails_job.perform('path/to/nonexistent/file.yml')
      end
    end

    context 'when a path of value nil is passed to the set_user_config_path method' do
      it 'return the usersync devops path for the user yaml file' do
        allow(File).to receive(:exist?).and_return(true)
        expect(log_entry).to receive(:error).with(
          /No such file or directory/,
        )
        rails_job.perform
        expect(rails_job.instance_variable_get(:@user_config_path)).to eq(
          '/usersync/identity-devops/terraform/master/global/users.yaml',
        )
      end

      it 'return the usersync devops path for the user yaml file' do
        expect(log_entry).to receive(:error).with(
          /No such file or directory/,
        )
        rails_job.perform
        expect(rails_job.instance_variable_get(:@user_config_path)).to eq(
          '/etc/login.gov/repos/identity-devops/terraform/master/global/users.yaml',
        )
      end
    end
  end
end
