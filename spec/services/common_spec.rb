# frozen_string_literal: true

require 'spec_helper'

# Stub AWS SDK gems that are not in the test bundle so require in lib/common.rb is a no-op
%w[aws-sdk-redshiftdataapiservice aws-sdk-secretsmanager].each do |gem_name|
  $LOADED_FEATURES << "#{gem_name}.rb" unless $LOADED_FEATURES.include?("#{gem_name}.rb")
end

module Aws
  module RedshiftDataAPIService
    class Client; end unless defined?(Client)
  end

  module SecretsManager
    class Client; end unless defined?(Client)
  end

  class InstanceProfileCredentials; end unless defined?(InstanceProfileCredentials)
end

require_relative '../../app/services/lib/common'

RSpec.describe RedshiftCommon do
  describe RedshiftCommon::Config do
    let(:config) { described_class.new }

    before do
      allow(File).to receive(:read).with('/etc/login.gov/info/env').and_return("staging\n")
      allow(File).to receive(:read).
        with('/etc/login.gov/repos/identity-devops/terraform/data-warehouse/staging/main.tf').
        and_return("enable_feature = true\n# disabled_feature = true")
    end

    describe '#env_name' do
      it 'reads and chomps the env file' do
        expect(config.env_name).to eq('staging')
      end
    end

    describe '#cluster_identifier' do
      it 'combines env_name and cluster_suffix' do
        expect(config.cluster_identifier).to eq('staging-analytics')
      end
    end

    describe '#env_type' do
      it 'returns prod for prod environments' do
        allow(File).to receive(:read).with('/etc/login.gov/info/env').and_return("prod\n")
        allow(File).to receive(:read).
          with('/etc/login.gov/repos/identity-devops/terraform/data-warehouse/prod/main.tf').
          and_return("enable_feature = true\n")
        expect(described_class.new.env_type).to eq('prod')
      end

      it 'returns prod for staging environment' do
        expect(config.env_type).to eq('prod')
      end

      it 'returns sandbox for non-prod environments' do
        allow(File).to receive(:read).with('/etc/login.gov/info/env').and_return("dev\n")
        allow(File).to receive(:read).
          with('/etc/login.gov/repos/identity-devops/terraform/data-warehouse/dev/main.tf').
          and_return("enable_feature = true\n")
        expect(described_class.new.env_type).to eq('sandbox')
      end
    end

    describe '#feature_enabled?' do
      it 'returns true when feature flag is enabled' do
        expect(config.feature_enabled?('enable_feature')).to be true
      end

      it 'returns false when feature flag is commented out' do
        expect(config.feature_enabled?('disabled_feature')).to be false
      end

      it 'returns true when feature_flag is nil' do
        expect(config.feature_enabled?(nil)).to be true
      end
    end
  end

  describe RedshiftCommon::QueryExecutor do
    let(:config) do
      instance_double(
        RedshiftCommon::Config,
        cluster_identifier: 'test-cluster', database: 'testdb', env_name: 'test',
      )
    end
    let(:aws_clients) { instance_double(RedshiftCommon::AwsClients) }
    let(:logger) { instance_double(Logger) }
    let(:redshift_data) { double('Aws::RedshiftDataAPIService::Client') }
    let(:executor) { described_class.new(config, aws_clients, logger) }

    before do
      allow(aws_clients).to receive(:secret_arn).
        and_return('arn:aws:secretsmanager:us-west-2:123456789:secret:test')
      allow(aws_clients).to receive(:redshift_data).and_return(redshift_data)
    end

    describe '#execute_and_wait' do
      it 'executes query and waits for completion' do
        allow(redshift_data).to receive(:execute_statement).and_return({ 'id' => 'query-123' })
        allow(redshift_data).to receive(:describe_statement).and_return({ 'status' => 'FINISHED' })

        result = executor.execute_and_wait('SELECT 1')
        expect(result).to eq('query-123')
      end

      it 'raises error on query failure' do
        allow(redshift_data).to receive(:execute_statement).and_return({ 'id' => 'query-123' })
        allow(redshift_data).to receive(:describe_statement).and_return(
          {
            'status' => 'FAILED',
            'error' => 'Syntax error',
            'query_string' => 'SELECT bad',
          },
        )

        expect { executor.execute_and_wait('SELECT bad') }.to raise_error(/Redshift query failed/)
      end
    end

    describe '#query_single_column' do
      it 'returns array of string values' do
        allow(redshift_data).to receive(:execute_statement).and_return({ 'id' => 'query-123' })
        allow(redshift_data).to receive(:describe_statement).and_return({ 'status' => 'FINISHED' })
        allow(redshift_data).to receive(:get_statement_result).and_return(
          {
            records: [[{ string_value: 'user1' }], [{ string_value: 'user2' }]],
          },
        )

        result = executor.query_single_column('SELECT usename FROM pg_user')
        expect(result).to eq(['user1', 'user2'])
      end
    end
  end

  describe RedshiftCommon::SqlQuoting do
    describe '.quote_value' do
      it 'quotes string values' do
        expect(described_class.quote_value('test')).to eq("'test'")
      end

      it 'quotes array values' do
        expect(described_class.quote_value(['a', 'b'])).to eq("('a', 'b')")
      end
    end

    describe '.quote_identifier' do
      it 'quotes identifiers with double quotes' do
        expect(described_class.quote_identifier('my_table')).to eq('"my_table"')
      end
    end

    describe '.quote_grantee' do
      it 'does not quote PUBLIC' do
        expect(described_class.quote_grantee('PUBLIC')).to eq('PUBLIC')
      end

      it 'quotes regular grantees' do
        expect(described_class.quote_grantee('myuser')).to eq('"myuser"')
      end

      it 'handles case-insensitive PUBLIC' do
        expect(described_class.quote_grantee('public')).to eq('PUBLIC')
      end
    end
  end

  describe RedshiftCommon::DataTypeUtils do
    let(:logger) { instance_double(Logger) }

    describe '.normalize_data_type' do
      it 'normalizes VARCHAR types' do
        expect(described_class.normalize_data_type('character varying')).to eq('VARCHAR(MAX)')
        expect(described_class.normalize_data_type('varchar')).to eq('VARCHAR(MAX)')
      end

      it 'normalizes CHAR types with length' do
        expect(described_class.normalize_data_type('char', 10)).to eq('CHAR(10)')
      end

      it 'defaults CHAR to length 1 when no length provided' do
        expect(described_class.normalize_data_type('char', nil)).to eq('CHAR(1)')
      end

      it 'normalizes numeric types' do
        expect(described_class.normalize_data_type('integer')).to eq('NUMERIC')
        expect(described_class.normalize_data_type('decimal')).to eq('NUMERIC')
      end

      it 'normalizes date and timestamp types' do
        expect(described_class.normalize_data_type('date')).to eq('DATE')
        expect(described_class.normalize_data_type('timestamp')).to eq('TIMESTAMP')
      end

      it 'defaults unknown types to VARCHAR(MAX) and logs warning' do
        allow(logger).to receive(:warn)
        result = described_class.normalize_data_type('unknown_type', nil, logger: logger)

        expect(result).to eq('VARCHAR(MAX)')
        expect(logger).to have_received(:warn).with(/unknown data type/)
      end
    end
  end

  describe RedshiftCommon::IamRoleUtils do
    describe '.resolve_iam_groups' do
      it 'returns mapped groups for known roles' do
        expect(described_class.resolve_iam_groups('dwuser')).to eq(%w[dwuser dwusernonprod])
      end

      it 'returns role name for unknown roles' do
        expect(described_class.resolve_iam_groups('custom_role')).to eq(['custom_role'])
      end
    end
  end

  describe RedshiftCommon::UserQueries do
    let(:executor) { instance_double(RedshiftCommon::QueryExecutor) }
    let(:logger) { instance_double(Logger, info: nil) }

    describe '.fetch_users' do
      it 'returns array of usernames' do
        allow(executor).to receive(:query_single_column).and_return(['User1', 'user2'])
        result = described_class.fetch_users(executor)

        expect(result).to eq(['User1', 'user2'])
      end
    end

    describe '.user_exists?' do
      it 'returns true when user exists' do
        allow(executor).to receive(:query_single_column).and_return(['1'])
        expect(described_class.user_exists?(executor, 'testuser')).to be true
      end

      it 'returns false when user does not exist' do
        allow(executor).to receive(:query_single_column).and_return([])
        expect(described_class.user_exists?(executor, 'testuser')).to be false
      end
    end

    describe '.group_exists?' do
      it 'returns true when group exists' do
        allow(executor).to receive(:query_single_column).and_return(['1'])
        expect(described_class.group_exists?(executor, 'testgroup')).to be true
      end
    end

    describe '.group_members' do
      it 'returns array of group members' do
        allow(executor).to receive(:query_single_column).and_return(['user1', 'user2'])
        result = described_class.group_members(executor, 'testgroup')

        expect(result).to eq(['user1', 'user2'])
      end
    end
  end

  describe RedshiftCommon::Validation do
    describe '.valid_username?' do
      it 'accepts valid usernames' do
        expect(described_class.valid_username?('user.name')).to be true
        expect(described_class.valid_username?('user-name')).to be true
        expect(described_class.valid_username?('IAM:user')).to be true
      end

      it 'rejects invalid usernames' do
        expect(described_class.valid_username?('user@name')).to be false
        expect(described_class.valid_username?('user name')).to be false
        expect(described_class.valid_username?('user/name')).to be false
      end
    end

    describe '.validate_username!' do
      it 'does not raise for valid username' do
        expect { described_class.validate_username!('valid.user') }.not_to raise_error
      end

      it 'raises for invalid username' do
        expect do
          described_class.validate_username!('invalid user')
        end.to raise_error(/Invalid username/)
      end
    end
  end

  describe RedshiftCommon::BaseScript do
    let(:script_class) do
      Class.new(described_class) do
        def run
          'test run'
        end
      end
    end

    before do
      allow(File).to receive(:read).with('/etc/login.gov/info/env').and_return("test\n")
      allow(File).to receive(:read).
        with('/etc/login.gov/repos/identity-devops/terraform/data-warehouse/test/main.tf').
        and_return("enable_feature = true\n")
    end

    it 'initializes with config, aws clients, and executor' do
      script = script_class.new
      expect(script.config).to be_a(RedshiftCommon::Config)
      expect(script.aws).to be_a(RedshiftCommon::AwsClients)
      expect(script.executor).to be_a(RedshiftCommon::QueryExecutor)
      expect(script.logger).to be_a(Logger)
    end

    it 'requires subclasses to implement run method' do
      expect { described_class.new.run }.to raise_error(NotImplementedError)
    end

    describe '#log_info' do
      it 'logs with script name prefix' do
        script = script_class.new
        allow(script).to receive(:script_name).and_return('test_script.rb')
        allow(script.logger).to receive(:info)

        script.log_info('test message')
        expect(script.logger).to have_received(:info).with('test_script.rb: test message')
      end
    end

    describe '#load_yaml' do
      it 'loads and parses YAML file' do
        script = script_class.new
        allow(File).to receive(:read).with('test.yml').and_return("key: value\n")
        allow(script.logger).to receive(:info)

        result = script.send(:load_yaml, 'test.yml')
        expect(result).to eq({ 'key' => 'value' })
      end
    end
  end
end
