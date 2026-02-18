require 'rails_helper'

RSpec.describe RedshiftSync do
  let(:mock_connection) { instance_double(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter) }
  let(:secrets_manager_client) { instance_double(Aws::SecretsManager::Client) }

  let(:test_redshift_config) do
    {
      'enabled_aws_groups' => {
        'prod' => ['dwuser', 'dwpoweruser', 'dwadmin'],
        'sandbox' => ['dwuser', 'dwusernonprod', 'dwpoweruser', 'dwpowerusernonprod', 'dwadmin',
                      'dwadminnonprod'],
      },
      'user_groups' => [
        {
          'name' => 'lg_users',
          'aws_groups' => { 'prod' => ['dwuser'], 'sandbox' => ['dwuser', 'dwusernonprod'] },
          'schemas' => [
            { 'schema_name' => 'idp',
              'schema_privileges' => 'USAGE',
              'table_privileges' => 'SELECT' },
            { 'schema_name' => 'logs',
              'schema_privileges' => 'USAGE',
              'table_privileges' => 'SELECT' },
          ],
        },
      ],
      'lambda_users' => [
        { 'user_name' => 'IAMR:testenv_db_consumption', 'schemas' => ['idp', 'fraudops'] },
      ],
      'system_users' => [
        {
          'user_name' => 'security_audit',
          'secret_id' => 'redshift/testenv-analytics-security-audit',
          'schemas' => [
            { 'schema_name' => 'system_tables',
              'schema_privileges' => 'USAGE',
              'table_privileges' => 'SELECT' },
          ],
        },
      ],
    }
  end

  let(:test_users_yaml) do
    {
      'john.doe' => { 'aws_groups' => ['dwuser'] },
      'jane.smith' => { 'aws_groups' => ['dwadmin'] },
      'bob.jones' => { 'aws_groups' => ['other_group'] },
      'project_21_bot' => { 'aws_groups' => ['dwuser'] },
      'root' => { 'aws_groups' => ['dwadmin'] },
    }
  end

  let(:terraform_config) do
    <<~TERRAFORM
      dbt_enabled = true
      redshift_quicksight_connector_enabled = false
      # fraud_ops_tracker_enabled = false
    TERRAFORM
  end

  subject(:sync) { described_class.new }

  before do
    allow(sync).to receive(:redshift_config).and_return(test_redshift_config)
    allow(sync).to receive(:users_yaml).and_return(test_users_yaml)
    allow(sync).to receive(:config_file).and_return(terraform_config)
    allow(sync).to receive(:connection).and_return(mock_connection)
    allow(sync).to receive(:secrets_manager_client).and_return(secrets_manager_client)
    allow(Identity::Hostdata).to receive(:env).and_return('testenv')
    allow(mock_connection).to receive(:execute).and_return(double(any?: false, to_a: [], map: []))
    allow(Rails.logger).to receive(:info)
    allow(Rails.logger).to receive(:error)
  end

  describe 'environment detection' do
    it 'returns prod for production environments' do
      allow(Identity::Hostdata).to receive(:env).and_return('prod')
      expect(sync.send(:env_type)).to eq('prod')
    end

    it 'returns sandbox for non-production environments' do
      allow(Identity::Hostdata).to receive(:env).and_return('int')
      expect(sync.send(:env_type)).to eq('sandbox')
    end
  end

  describe 'feature flag checking' do
    it 'returns true when feature flag is enabled in terraform' do
      expect(sync.send(:feature_enabled?, 'dbt_enabled')).to be true
    end

    it 'returns false when feature flag is disabled' do
      expect(sync.send(:feature_enabled?, 'redshift_quicksight_connector_enabled')).to be false
    end

    it 'returns false when feature flag is commented out' do
      expect(sync.send(:feature_enabled?, 'fraud_ops_tracker_enabled')).to be false
    end

    it 'returns true when feature flag is nil' do
      expect(sync.send(:feature_enabled?, nil)).to be true
    end

    it 'returns true when any flag in a list is enabled' do
      expect(
        sync.send(:feature_enabled?, %w[redshift_quicksight_connector_enabled dbt_enabled]),
      ).to be true
    end

    it 'returns false when no flags in a list are enabled' do
      expect(
        sync.send(
          :feature_enabled?,
          %w[redshift_quicksight_connector_enabled fraud_ops_tracker_enabled],
        ),
      ).to be false
    end
  end

  describe 'user filtering' do
    it 'filters canonical users correctly' do
      allow(sync).to receive(:env_type).and_return('sandbox')
      users = sync.send(:canonical_users)

      expect(users).to include('IAM:john.doe', 'IAM:jane.smith')
      expect(users).not_to include('IAM:bob.jones', 'IAM:project_21_bot', 'IAM:root')
    end

    it 'validates username characters' do
      expect(sync.send(:disallowed_characters?, 'IAM:john.doe')).to be false
      expect(sync.send(:disallowed_characters?, 'IAMR:env_name')).to be false
      expect(sync.send(:disallowed_characters?, 'user;drop')).to be true
    end
  end

  describe 'SQL generation for user groups' do
    it 'generates correct privilege SQL with restricted tables' do
      sql = sync.send(
        :create_user_group_privileges, 'lg_users', 'logs', 'USAGE', 'SELECT',
        ['unextracted_events']
      )

      expect(sql).to include('GRANT USAGE ON SCHEMA logs TO GROUP lg_users')
      expect(sql).to include('GRANT SELECT ON ALL TABLES IN SCHEMA logs TO GROUP lg_users')
      expect(sql).to include(
        'REVOKE ALL PRIVILEGES ON TABLE logs.unextracted_events FROM GROUP lg_users',
      )
    end

    it 'includes ALTER DEFAULT PRIVILEGES for DBT schemas when user exists' do
      allow(sync).to receive(:user_exists?).with('marts').and_return(true)
      sql = sync.send(:create_user_group_privileges, 'lg_users', 'marts', 'USAGE', 'SELECT', [])

      expect(sql).to include('ALTER DEFAULT PRIVILEGES FOR USER marts IN SCHEMA marts')
    end

    it 'does not include ALTER DEFAULT PRIVILEGES for DBT schemas when user does not exist' do
      allow(sync).to receive(:user_exists?).with('marts').and_return(false)
      sql = sync.send(:create_user_group_privileges, 'lg_users', 'marts', 'USAGE', 'SELECT', [])

      expect(sql).not_to include('ALTER DEFAULT PRIVILEGES FOR USER marts IN SCHEMA marts')
    end
  end

  describe 'SQL generation for system users' do
    it 'includes CREATE SCHEMA for DBT users' do
      sql = sync.send(
        :create_system_user_privileges, 'marts', 'marts', 'ALL PRIVILEGES',
        'ALL PRIVILEGES', nil
      )

      expect(sql).to include('CREATE SCHEMA IF NOT EXISTS marts')
    end

    it 'grants on specific tables when provided' do
      sql = sync.send(
        :create_system_user_privileges, 'security_audit', 'pg_catalog', 'USAGE',
        'SELECT', ['pg_user']
      )

      expect(sql).to include('GRANT SELECT ON pg_catalog.pg_user TO security_audit')
      expect(sql).not_to include('ALL TABLES')
    end
  end

  describe '#sync execution order' do
    it 'executes all steps in correct sequence' do
      call_order = []

      allow(sync).to receive(:create_lambda_user) { call_order << :create_lambda_user }
      allow(sync).to receive(:create_system_user) { call_order << :create_system_user }
      allow(sync).to receive(:create_user_group) { call_order << :create_user_group }
      allow(sync).to receive(:drop_users) { call_order << :drop_users }
      allow(sync).to receive(:create_users) { call_order << :create_users }
      allow(sync).to receive(:sync_user_group) { call_order << :sync_user_group }

      sync.sync

      expect(call_order.uniq).to eq(
        [
          :create_lambda_user,
          :create_system_user,
          :create_user_group,
          :drop_users,
          :create_users,
          :sync_user_group,
        ],
      )
    end
  end
end
