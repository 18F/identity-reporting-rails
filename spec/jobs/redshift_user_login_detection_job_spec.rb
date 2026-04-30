require 'rails_helper'

RSpec.describe RedshiftUserLoginDetectionJob, type: :job do
  let(:rails_job) { RedshiftUserLoginDetectionJob.new }
  let(:logger) { instance_double(IdentityJobLogSubscriber) }
  let(:log_entry) { instance_double(Logger) }

  before do
    allow(IdentityJobLogSubscriber).to receive(:new).and_return(logger)
    allow(logger).to receive(:logger).and_return(log_entry)
  end

  describe '#perform' do
    before do
      # Create test SYS_CONNECTION_LOG entries
      query = <<~SQL
        CREATE TABLE SYS_CONNECTION_LOG (
          user_name VARCHAR(256),
          event VARCHAR(50),
          record_time TIMESTAMP,
          driver_version VARCHAR(256)
        );
        INSERT INTO SYS_CONNECTION_LOG (user_name, event, record_time, driver_version) VALUES
        ('IAM:kobe.bryant', 'authenticated', CURRENT_TIMESTAMP - INTERVAL '10 MINUTES', 'Redshift JDBC Driver 2.1.0.34'),
        ('IAM:steph.curry', 'authenticated', CURRENT_TIMESTAMP - INTERVAL '5 MINUTES', 'Redshift JDBC Driver 2.1.0.34'),
        ('pii_reader', 'authenticated', CURRENT_TIMESTAMP - INTERVAL '2 MINUTES', 'Redshift JDBC Driver 2.2.5'),
        ('superuser', 'authenticated', CURRENT_TIMESTAMP - INTERVAL '1 MINUTE', 'Redshift JDBC Driver 2.1.0.30'),
        ('old_user', 'authenticated', CURRENT_TIMESTAMP - INTERVAL '20 MINUTES', 'Redshift JDBC Driver 2.2.5');
      SQL
      DataWarehouseApplicationRecord.connection.execute(query)
    end

    context 'when users_to_check is set for a pii_reader who logged in recently' do
      before do
        allow(rails_job).to receive(:users_to_check).and_return(['pii_reader'])
      end

      it 'then logs the user login detected' do
        expect(log_entry).to receive(:info).with(
          {
            name: 'RedshiftUserLoginDetectionJob',
            detected_user: 'pii_reader',
          }.to_json,
        )
        rails_job.perform
      end

      context 'when users_to_check is set for a superuser who logged in recently' do
        before do
          allow(rails_job).to receive(:users_to_check).and_return(['superuser'])
        end

        it 'then logs the user login detected' do
          expect(log_entry).to receive(:info).with(
            {
              name: 'RedshiftUserLoginDetectionJob',
              detected_user: 'superuser',
            }.to_json,
          )
          rails_job.perform
        end
      end
    end

    context 'when users_to_check is set to multiple users who logged in recently' do
      before do
        allow(rails_job).to receive(:users_to_check).and_return(
          ['pii_reader', 'superuser', 'old_user'],
        )
      end

      it 'then logs the user login detected for both users' do
        expect(log_entry).to receive(:info).with(
          {
            name: 'RedshiftUserLoginDetectionJob',
            detected_user: 'pii_reader',
          }.to_json,
        )
        expect(log_entry).to receive(:info).with(
          {
            name: 'RedshiftUserLoginDetectionJob',
            detected_user: 'superuser',
          }.to_json,
        )
        # expect not to receive log for old_user since login was over 15 minutes ago
        expect(log_entry).not_to receive(:info).with(
          {
            name: 'RedshiftUserLoginDetectionJob',
            detected_user: 'old_user',
          }.to_json,
        )
        rails_job.perform
      end
    end

    context 'when no users defined in users_to_check have logged in recently' do
      before do
        allow(rails_job).to receive(:users_to_check).and_return(
          ['non_existent_user1', 'non_existent_user2'],
        )
      end

      it 'then no info log found' do
        expect(log_entry).not_to receive(:info)
        rails_job.perform
      end
    end
  end
end
