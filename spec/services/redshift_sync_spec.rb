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
        {
          'user_name' => 'rails_worker',
          'secret_id' => 'redshift/testenv-analytics-rails-worker',
          'syslog_access' => true,
          'schemas' => [
            { 'schema_name' => 'idp',
              'schema_privileges' => 'USAGE',
              'table_privileges' => 'SELECT' },
          ],
        },
      ],
      'user_roles' => [
        {
          'role_name' => 'dw_ingestion',
          'users' => [
            'rails_worker',
            'IAMR:%{env_name}_db_consumption',
            'IAMR:%{env_name}_log_consumption',
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

  describe '#create_schema_privileges_for_group' do
    let(:user_group) do
      {
        'name' => 'lg_users',
        'schemas' => [
          { 'schema_name' => 'idp',
            'schema_privileges' => 'USAGE',
            'table_privileges' => 'SELECT' },
          { 'schema_name' => 'logs',
            'schema_privileges' => 'USAGE',
            'table_privileges' => 'SELECT' },
        ],
      }
    end
    let(:executed_sql) { [] }

    before do
      allow(sync).to receive(:get_existing_configured_schemas).and_return(%w[idp logs marts])
      allow(mock_connection).to receive(:execute) do |sql|
        executed_sql << sql
        double(any?: true)
      end
    end

    it 'revokes only schemas the group should not have' do
      sync.send(:create_schema_privileges_for_group, user_group)

      sql = executed_sql.join("\n")
      expect(sql).not_to include('REVOKE ALL ON SCHEMA idp FROM GROUP lg_users')
      expect(sql).not_to include('REVOKE ALL ON SCHEMA logs FROM GROUP lg_users')
      expect(sql).to include('REVOKE ALL ON SCHEMA marts FROM GROUP lg_users')
      expect(sql).to include('GRANT USAGE ON SCHEMA idp TO GROUP lg_users')
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
      allow(sync).to receive(:create_users) do
        call_order << :create_users
        []
      end
      allow(sync).to receive(:sync_user_group) { call_order << :sync_user_group }
      allow(sync).to receive(:create_user_role) { call_order << :create_user_role }

      sync.sync

      expect(call_order.uniq).to eq(
        [
          :create_lambda_user,
          :create_system_user,
          :create_user_group,
          :drop_users,
          :create_users,
          :sync_user_group,
          :create_user_role,
        ],
      )
    end
  end

  describe '#sync masking policy application' do
    before do
      allow(sync).to receive(:create_lambda_user)
      allow(sync).to receive(:create_system_user)
      allow(sync).to receive(:create_user_group)
      allow(sync).to receive(:drop_users)
      allow(sync).to receive(:sync_user_group)
      allow(sync).to receive(:create_user_role)
    end

    context 'when new users are created' do
      let(:new_users) { ['IAM:john.doe'] }
      let(:masking_sync) { instance_double(RedshiftMaskingSync) }

      before do
        allow(sync).to receive(:create_users).and_return(new_users)
        allow(RedshiftMaskingSync).to receive(:new).and_return(masking_sync)
        allow(masking_sync).to receive(:sync)
      end

      it 'calls RedshiftMaskingSync with the new users' do
        expect(masking_sync).to receive(:sync).with(user_filter: new_users)
        sync.sync
      end
    end

    context 'when no new users are created' do
      before do
        allow(sync).to receive(:create_users).and_return([])
      end

      it 'does not call RedshiftMaskingSync' do
        expect(RedshiftMaskingSync).not_to receive(:new)
        sync.sync
      end
    end

    context 'when masking service raises an error' do
      let(:new_users) { ['IAM:john.doe'] }
      let(:masking_sync) { instance_double(RedshiftMaskingSync) }

      before do
        allow(sync).to receive(:create_users).and_return(new_users)
        allow(RedshiftMaskingSync).to receive(:new).and_return(masking_sync)
        allow(masking_sync).to receive(:sync).and_raise(StandardError, 'AWS error')
      end

      it 'does not raise an error' do
        expect { sync.sync }.not_to raise_error
      end

      it 'logs a warning' do
        expect(Rails.logger).to receive(:warn).with(a_string_matching(/masking policies/i))
        sync.sync
      end
    end
  end

  describe 'user roles' do
    describe '#user_roles' do
      it 'returns interpolated user roles from config' do
        roles = sync.send(:user_roles)

        expect(roles.length).to eq(1)
        expect(roles.first['role_name']).to eq('dw_ingestion')
        expect(roles.first['users']).to include(
          'rails_worker',
          'IAMR:testenv_db_consumption',
          'IAMR:testenv_log_consumption',
        )
      end

      it 'returns empty array when user_roles is not defined in config' do
        allow(sync).to receive(:redshift_config).and_return(
          {
            'enabled_aws_groups' => { 'sandbox' => ['dwuser'] },
            'user_groups' => [],
            'lambda_users' => [],
            'system_users' => [],
          },
        )

        roles = sync.send(:user_roles)

        expect(roles).to eq([])
      end
    end

    describe '#create_user_role' do
      let(:user_role) do
        {
          'role_name' => 'dw_ingestion',
          'users' => ['rails_worker', 'IAMR:testenv_db_consumption'],
        }
      end

      context 'when role does not exist' do
        before do
          allow(mock_connection).to receive(:execute).
            with(/SELECT role_name FROM svv_roles/).
            and_return(double(any?: false))
          allow(sync).to receive(:sync_user_role)
        end

        it 'creates the role' do
          expect(mock_connection).to receive(:execute).
            with(/CREATE ROLE dw_ingestion;/)

          sync.send(:create_user_role, user_role)
        end

        it 'syncs the role membership' do
          expect(sync).to receive(:sync_user_role).with(user_role)
          sync.send(:create_user_role, user_role)
        end
      end

      context 'when role already exists' do
        before do
          allow(mock_connection).to receive(:execute).
            with(/SELECT role_name FROM svv_roles/).
            and_return(double(any?: true))
          allow(sync).to receive(:sync_user_role)
        end

        it 'does not create the role' do
          expect(mock_connection).not_to receive(:execute).
            with(/CREATE ROLE/)

          sync.send(:create_user_role, user_role)
        end

        it 'syncs the role membership' do
          expect(sync).to receive(:sync_user_role).with(user_role)
          sync.send(:create_user_role, user_role)
        end
      end
    end

    describe '#sync_user_role' do
      let(:user_role) do
        {
          'role_name' => 'dw_ingestion',
          'users' => ['rails_worker', 'IAMR:testenv_db_consumption'],
        }
      end

      context 'when role has existing members' do
        before do
          allow(mock_connection).to receive(:execute).
            with(/SELECT user_name\s+FROM svv_user_grants/).
            and_return([
                         { 'user_name' => 'old_user' },
                         { 'user_name' => 'another_old_user' },
                       ])
        end

        it 'revokes existing memberships and grants new ones' do
          expect(mock_connection).to receive(:execute).
            with(/SELECT user_name\s+FROM svv_user_grants/).ordered
          expect(mock_connection).to receive(:execute).ordered do |sql|
            expect(sql).to include('REVOKE ROLE dw_ingestion FROM "old_user"')
            expect(sql).to include('REVOKE ROLE dw_ingestion FROM "another_old_user"')
            expect(sql).to include('GRANT ROLE dw_ingestion TO "rails_worker"')
            expect(sql).to include('GRANT ROLE dw_ingestion TO "IAMR:testenv_db_consumption"')
          end

          sync.send(:sync_user_role, user_role)
        end
      end

      context 'when role has no existing members' do
        before do
          allow(mock_connection).to receive(:execute).
            with(/SELECT user_name\s+FROM svv_user_grants/).
            and_return([])
        end

        it 'grants role to all specified users' do
          expect(mock_connection).to receive(:execute).
            with(/SELECT user_name\s+FROM svv_user_grants/).ordered
          expect(mock_connection).to receive(:execute).ordered do |sql|
            expect(sql).not_to include('REVOKE')
            expect(sql).to include('GRANT ROLE dw_ingestion TO "rails_worker"')
            expect(sql).to include('GRANT ROLE dw_ingestion TO "IAMR:testenv_db_consumption"')
          end

          sync.send(:sync_user_role, user_role)
        end
      end

      context 'when role has no users configured' do
        let(:user_role) do
          {
            'role_name' => 'dw_ingestion',
            'users' => [],
          }
        end

        before do
          allow(mock_connection).to receive(:execute).
            with(/SELECT user_name\s+FROM svv_user_grants/).
            and_return([{ 'user_name' => 'old_user' }])
        end

        it 'revokes existing memberships' do
          expect(mock_connection).to receive(:execute).
            with(/SELECT user_name\s+FROM svv_user_grants/).ordered
          expect(mock_connection).to receive(:execute).ordered do |sql|
            expect(sql).to include('REVOKE ROLE dw_ingestion FROM "old_user"')
            expect(sql).not_to include('GRANT')
          end

          sync.send(:sync_user_role, user_role)
        end
      end

      context 'when role has no existing members and no users configured' do
        let(:user_role) do
          {
            'role_name' => 'dw_ingestion',
            'users' => [],
          }
        end

        before do
          allow(mock_connection).to receive(:execute).
            with(/SELECT user_name\s+FROM svv_user_grants/).
            and_return([])
        end

        it 'does not execute any SQL' do
          expect(mock_connection).to receive(:execute).
            with(/SELECT user_name\s+FROM svv_user_grants/)
          expect(mock_connection).not_to receive(:execute).
            with(a_string_matching(/GRANT|REVOKE/))

          sync.send(:sync_user_role, user_role)
        end

        it 'logs that role is already in sync' do
          expect(Rails.logger).to receive(:info).
            with(/User role dw_ingestion is already in sync/)

          sync.send(:sync_user_role, user_role)
        end
      end

      context 'with environment variable interpolation in user names' do
        let(:user_role) do
          {
            'role_name' => 'dw_ingestion',
            'users' => ['IAMR:%{env_name}_db_consumption'],
          }
        end

        before do
          allow(mock_connection).to receive(:execute).
            with(/SELECT user_name\s+FROM svv_user_grants/).
            and_return([])
        end

        it 'interpolates environment variables in user names' do
          expect(mock_connection).to receive(:execute).
            with(/SELECT user_name\s+FROM svv_user_grants/).ordered
          expect(mock_connection).to receive(:execute).ordered do |sql|
            expect(sql).to include('GRANT ROLE dw_ingestion TO "IAMR:testenv_db_consumption"')
          end

          sync.send(:sync_user_role, user_role)
        end
      end

      context 'when role membership is already in sync' do
        before do
          allow(mock_connection).to receive(:execute).
            with(/SELECT user_name\s+FROM svv_user_grants/).
            and_return([
                         { 'user_name' => 'rails_worker' },
                         { 'user_name' => 'IAMR:testenv_db_consumption' },
                       ])
        end

        it 'does not make any changes' do
          expect(mock_connection).to receive(:execute).
            with(/SELECT user_name\s+FROM svv_user_grants/)
          expect(mock_connection).not_to receive(:execute).
            with(a_string_matching(/GRANT|REVOKE/))

          sync.send(:sync_user_role, user_role)
        end

        it 'logs that role is already in sync' do
          expect(Rails.logger).to receive(:info).
            with(/User role dw_ingestion is already in sync/)

          sync.send(:sync_user_role, user_role)
        end
      end
    end
  end

  describe 'password rotation' do
    describe '#generate_password' do
      it 'generates a password of the configured length' do
        expect(sync.send(:generate_password).length).to eq(RedshiftSync::PASSWORD_LENGTH)
      end

      it 'only uses characters from the allowed set' do
        allowed = /\A[a-zA-Z0-9#{Regexp.escape(RedshiftSync::PASSWORD_PUNCTUATION)}]+\z/o
        expect(sync.send(:generate_password)).to match(allowed)
      end

      it 'generates a different password on each call' do
        expect(sync.send(:generate_password)).not_to eq(sync.send(:generate_password))
      end
    end

    describe '#md5_password' do
      it 'returns the md5-salted form expected by Redshift' do
        expected = "'md5#{Digest::MD5.hexdigest('secret' + 'rails_worker')}'"
        expect(sync.send(:md5_password, 'secret', 'rails_worker')).to eq(expected)
      end
    end

    describe '#rotation_targets' do
      it 'returns all secret-backed system users when no usernames given' do
        targets = sync.send(:rotation_targets, nil).map { |u| u['user_name'] }
        expect(targets).to contain_exactly('security_audit', 'rails_worker')
      end

      it 'filters to the requested usernames' do
        targets = sync.send(:rotation_targets, ['rails_worker']).map { |u| u['user_name'] }
        expect(targets).to eq(['rails_worker'])
      end

      it 'warns about and skips unknown usernames' do
        expect(Rails.logger).to receive(:warn).with(/nonexistent.*not a known system user/)

        targets = sync.send(:rotation_targets, ['nonexistent']).map { |u| u['user_name'] }
        expect(targets).to be_empty
      end
    end

    describe '#fetch_secret' do
      let(:secret_id) { 'redshift/testenv-analytics-rails-worker' }

      it 'parses and returns the existing secret payload' do
        allow(secrets_manager_client).to receive(:get_secret_value).
          with(secret_id: secret_id).
          and_return(double(secret_string: { 'host' => 'db.example', 'password' => 'old' }.to_json))

        expect(sync.send(:fetch_secret, secret_id)).to eq(
          'host' => 'db.example', 'password' => 'old',
        )
      end

      it 'returns an empty hash when the secret has no string value' do
        allow(secrets_manager_client).to receive(:get_secret_value).
          with(secret_id: secret_id).
          and_return(double(secret_string: nil))

        expect(sync.send(:fetch_secret, secret_id)).to eq({})
      end
    end

    describe '#store_password_secret' do
      let(:secret_id) { 'redshift/testenv-analytics-rails-worker' }

      it 'writes the given payload as JSON' do
        expect(secrets_manager_client).to receive(:put_secret_value) do |args|
          expect(args[:secret_id]).to eq(secret_id)
          expect(JSON.parse(args[:secret_string])).to eq(
            'host' => 'db.example', 'password' => 'new-password',
          )
        end

        sync.send(
          :store_password_secret, secret_id,
          { 'host' => 'db.example', 'password' => 'new-password' }
        )
      end
    end

    describe '#rotate_user_password' do
      let(:secret_id) { 'redshift/testenv-analytics-rails-worker' }

      before do
        allow(sync).to receive(:generate_password).and_return('generated-pw')
        allow(sync).to receive(:store_password_secret)
        allow(sync).to receive(:fetch_secret).with(secret_id).and_return('host' => 'db.example')
      end

      it 'reads the secret, runs ALTER USER, then stores the merged payload' do
        allow(sync).to receive(:user_exists?).with('rails_worker').and_return(true)
        expected_hash = "'md5#{Digest::MD5.hexdigest('generated-pw' + 'rails_worker')}'"

        expect(mock_connection).to receive(:execute).
          with("ALTER USER rails_worker PASSWORD #{expected_hash};")
        expect(sync).to receive(:store_password_secret).
          with(secret_id, 'host' => 'db.example', 'password' => 'generated-pw')

        sync.send(:rotate_user_password, 'rails_worker', secret_id)
      end

      it 'does not alter Redshift when the secret cannot be read' do
        allow(sync).to receive(:user_exists?).with('rails_worker').and_return(true)
        allow(sync).to receive(:fetch_secret).with(secret_id).and_raise(
          Aws::SecretsManager::Errors::ResourceNotFoundException.new(nil, 'missing'),
        )

        expect(mock_connection).not_to receive(:execute).with(/ALTER USER/)
        expect(sync).not_to receive(:store_password_secret)

        expect do
          sync.send(:rotate_user_password, 'rails_worker', secret_id)
        end.to raise_error(Aws::SecretsManager::Errors::ResourceNotFoundException)
      end

      it 'skips users that do not exist in Redshift' do
        allow(sync).to receive(:user_exists?).with('rails_worker').and_return(false)

        expect(mock_connection).not_to receive(:execute).with(/ALTER USER/)
        expect(sync).not_to receive(:store_password_secret)
        expect(Rails.logger).to receive(:warn).with(/does not exist in Redshift/)

        sync.send(:rotate_user_password, 'rails_worker', secret_id)
      end
    end

    describe '#rotate_password' do
      it 'rotates each matching system user' do
        allow(sync).to receive(:rotate_user_password)

        expect(sync).to receive(:rotate_user_password).
          with('rails_worker', 'redshift/testenv-analytics-rails-worker')

        sync.rotate_password(usernames: ['rails_worker'])
      end

      it 'rotates all secret-backed users when no usernames given' do
        expect(sync).to receive(:rotate_user_password).
          with('security_audit', 'redshift/testenv-analytics-security-audit')
        expect(sync).to receive(:rotate_user_password).
          with('rails_worker', 'redshift/testenv-analytics-rails-worker')

        sync.rotate_password
      end

      it 'warns and does nothing when there are no matching users' do
        expect(sync).not_to receive(:rotate_user_password)
        expect(Rails.logger).to receive(:warn).with(/No matching system users/)

        sync.rotate_password(usernames: ['nonexistent'])
      end

      it 'continues rotating other users when one fails, then raises a summary' do
        allow(sync).to receive(:rotate_user_password).
          with('security_audit', anything).
          and_raise(StandardError, 'boom')
        allow(sync).to receive(:rotate_user_password).
          with('rails_worker', anything)

        # rails_worker is still attempted even though security_audit blew up
        expect(sync).to receive(:rotate_user_password).with('rails_worker', anything)
        expect(Rails.logger).to receive(:error).with(/security_audit.*boom/)

        expect { sync.rotate_password }.to raise_error(/failed for: security_audit/)
      end
    end
  end

  describe 'redshift_config.yaml validation' do
    let(:real_config) do
      YAML.safe_load(File.read(Rails.root.join('config/redshift_config.yaml')))
    end

    let(:allowed_role_users) do
      lambda_user_names = real_config['lambda_users'].map { |u| u['user_name'] }
      system_user_names = real_config['system_users'].map { |u| u['user_name'] }
      exceptions = ['superuser']

      (lambda_user_names + system_user_names + exceptions).uniq
    end

    it 'references only users defined in lambda_users, system_users, or known exceptions' do
      return if real_config['user_roles'].nil?

      invalid_references = []

      real_config['user_roles'].each do |role|
        role['users'].each do |user|
          unless allowed_role_users.include?(user)
            invalid_references << "Role '#{role['role_name']}' references unknown user '#{user}'"
          end
        end
      end

      error_message = [
        'Found invalid user references in user_roles:',
        invalid_references.join("\n"),
        '',
        'Users in user_roles must be defined in lambda_users, system_users, ' \
          'or be a known exception (superuser).',
        "Allowed users: #{allowed_role_users.sort.join(', ')}",
      ].join("\n")

      expect(invalid_references).to be_empty, error_message
    end

    it 'includes all system_users in RedshiftUnexpectedUserDetectionJob exclusion list' do
      system_user_names = real_config['system_users'].map { |u| u['user_name'] }
      excluded_users = RedshiftUnexpectedUserDetectionJob::STATIC_EXCLUDED_USERS + ['idp_connector']

      missing_users = system_user_names - excluded_users

      error_message = [
        'RedshiftUnexpectedUserDetectionJob::STATIC_EXCLUDED_USERS is missing system users!',
        '',
        'The following users from redshift_config.yaml system_users are NOT in ' \
          'STATIC_EXCLUDED_USERS:',
        missing_users.map { |u| "  - #{u}" }.join("\n"),
        '',
        'Add these to STATIC_EXCLUDED_USERS in app/jobs/redshift_unexpected_user_detection_job.rb',
        'to prevent false alarms when these users are created by the sync script.',
      ].join("\n")

      expect(missing_users).to be_empty, error_message
    end
  end
end
