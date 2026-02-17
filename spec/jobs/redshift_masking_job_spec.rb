# frozen_string_literal: true

require 'rails_helper'

RSpec.describe RedshiftMaskingJob, type: :job do
  # lib/common.rb is required lazily inside #perform, and mask.rb uses a
  # Zeitwerk alias (Mask = RedshiftMasking) so it is not autoloaded under
  # the RedshiftMasking constant name. Load both explicitly so constants
  # are available for mocking.
  before(:context) do
    require Rails.root.join('lib/common')
    require Rails.root.join('app/services/mask')
  end

  let(:job) { described_class.new }

  let(:data_controls) do
    {
      'masking_policies' => {
        'user_types' => {
          'iam_role' => ['dwuser'],
          'superuser' => ['admin'],
        },
        'columns' => [
          { 'public.users.ssn' => { 'allowed' => ['dwadmin'], 'masked' => ['dwuser'] } },
        ],
      },
    }
  end
  let(:users_yaml) { { 'alice' => { 'aws_groups' => ['dwuser'] } } }

  let(:redshift_config) { instance_double(RedshiftCommon::Config, env_name: 'test') }
  let(:aws_clients) { instance_double(RedshiftCommon::AwsClients) }
  let(:executor) { instance_double(RedshiftCommon::QueryExecutor) }

  let(:masking_config) do
    instance_double(
      RedshiftMasking::Configuration,
      columns_config: [
        { 'public.users.ssn' => { 'allowed' => ['dwadmin'], 'masked' => ['dwuser'] } },
      ],
      env_name: 'test',
    )
  end
  let(:db_queries) { instance_double(RedshiftMasking::DatabaseQueries) }
  let(:user_resolver) { instance_double(RedshiftMasking::UserResolver) }
  let(:policy_builder) { instance_double(RedshiftMasking::PolicyBuilder) }
  let(:drift_detector) { instance_double(RedshiftMasking::DriftDetector) }
  let(:sql_executor) { instance_double(RedshiftMasking::SqlExecutor) }

  let(:db_users) { ['IAM:alice', 'IAM:bob'] }
  let(:column_types) { { 'public.users.ssn' => 'VARCHAR(MAX)' } }
  let(:expected_policies) do
    [
      RedshiftMasking::PolicyAttachment.new(
        policy_name: 'mask_public_users_ssn',
        schema: 'public',
        table: 'users',
        column: 'ssn',
        grantee: 'IAM:alice',
        priority: 100,
      ),
    ]
  end
  let(:actual_policies) { [] }
  let(:drift) { { missing: expected_policies, extra: [], mismatched: [] } }

  before do
    allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(true)
    allow(File).to receive(:read).and_return('')
    allow(YAML).to receive(:safe_load).with('').and_return(data_controls, 'users' => users_yaml)
    allow(YAML).to receive(:safe_load).with('', symbolize_names: anything).and_return(
      data_controls, 'users' => users_yaml
    )

    allow(RedshiftCommon::Config).to receive(:new).and_return(redshift_config)
    allow(RedshiftCommon::AwsClients).to receive(:new).and_return(aws_clients)
    allow(RedshiftCommon::QueryExecutor).to receive(:new).and_return(executor)
    allow(RedshiftCommon::UserQueries).to receive(:fetch_users).and_return(db_users)

    allow(RedshiftMasking::Configuration).to receive(:new).and_return(masking_config)
    allow(RedshiftMasking::DatabaseQueries).to receive(:new).and_return(db_queries)
    allow(RedshiftMasking::UserResolver).to receive(:new).and_return(user_resolver)
    allow(RedshiftMasking::PolicyBuilder).to receive(:new).and_return(policy_builder)
    allow(RedshiftMasking::DriftDetector).to receive(:new).and_return(drift_detector)
    allow(RedshiftMasking::SqlExecutor).to receive(:new).and_return(sql_executor)

    allow(db_queries).to receive(:fetch_column_types).and_return(column_types)
    allow(db_queries).to receive(:fetch_existing_policies).and_return(actual_policies)
    allow(policy_builder).to receive(:build_expected_state).and_return(expected_policies)
    allow(drift_detector).to receive(:detect).and_return(drift)
    allow(sql_executor).to receive(:create_masking_policies)
    allow(sql_executor).to receive(:apply_corrections)

    allow(Rails.logger).to receive(:info)
    allow(Rails.logger).to receive(:warn)
    allow(Rails.logger).to receive(:debug)
  end

  describe '#perform' do
    context 'when job is disabled' do
      before do
        allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(false)
      end

      it 'skips execution and logs disabled message' do
        expect(Rails.logger).to receive(:info).with(
          a_string_matching(/disabled|skipping/i),
        )
        job.perform
      end

      it 'does not run masking sync' do
        expect(RedshiftCommon::Config).not_to receive(:new)
        job.perform
      end
    end

    context 'when job is enabled' do
      it 'loads data controls YAML from configured path' do
        expect(File).to receive(:read).with(described_class::DATA_CONTROLS_PATH).and_return('')
        job.perform
      end

      it 'loads users YAML from configured path' do
        expect(File).to receive(:read).with(described_class::USERS_YAML_PATH).and_return('')
        job.perform
      end

      it 'builds RedshiftCommon executor with config and aws clients' do
        expect(RedshiftCommon::QueryExecutor).to receive(:new).with(
          redshift_config, aws_clients, anything
        )
        job.perform
      end

      it 'builds masking configuration from data_controls and users_yaml' do
        expect(RedshiftMasking::Configuration).to receive(:new).with(
          data_controls, users_yaml, env_name: 'test'
        )
        job.perform
      end

      it 'fetches database users' do
        expect(RedshiftCommon::UserQueries).to receive(:fetch_users).with(executor)
        job.perform
      end

      it 'fetches column types for configured columns' do
        expect(db_queries).to receive(:fetch_column_types).with(
          an_instance_of(Array),
        )
        job.perform
      end

      it 'builds expected policy state' do
        expect(policy_builder).to receive(:build_expected_state).with(
          column_types, an_instance_of(Set)
        )
        job.perform
      end

      it 'fetches existing policies from database' do
        expect(db_queries).to receive(:fetch_existing_policies)
        job.perform
      end

      it 'detects drift between expected and actual policies' do
        expect(drift_detector).to receive(:detect).with(expected_policies, actual_policies)
        job.perform
      end

      it 'creates masking policy definitions' do
        expect(sql_executor).to receive(:create_masking_policies).with(column_types)
        job.perform
      end

      it 'applies drift corrections' do
        expect(sql_executor).to receive(:apply_corrections).with(drift)
        job.perform
      end

      it 'logs sync start and completion' do
        expect(Rails.logger).to receive(:info).with(a_string_matching(/starting/i)).at_least(:once)
        expect(Rails.logger).to receive(:info).with(a_string_matching(/completed/i)).at_least(:once)
        job.perform
      end

      it 'logs the number of expected and actual attachments' do
        expect(Rails.logger).to receive(:info).with(
          a_string_matching(/expected.*#{expected_policies.size}.*actual.*#{actual_policies.size}/),
        )
        job.perform
      end

      it 'logs database user count' do
        expect(Rails.logger).to receive(:info).with(
          a_string_matching(/#{db_users.size}.*database users/),
        )
        job.perform
      end
    end
  end

  describe '#extract_columns (private)' do
    it 'parses column identifiers from config' do
      columns = job.send(:extract_columns, masking_config)
      expect(columns).to all(be_a(RedshiftMasking::Column))
      expect(columns.map(&:id)).to include('public.users.ssn')
    end

    it 'returns empty array when columns_config is empty' do
      allow(masking_config).to receive(:columns_config).and_return([])
      columns = job.send(:extract_columns, masking_config)
      expect(columns).to be_empty
    end

    it 'skips unparseable column identifiers' do
      allow(masking_config).to receive(:columns_config).and_return([{ 'invalid' => {} }])
      columns = job.send(:extract_columns, masking_config)
      expect(columns).to be_empty
    end

    it 'handles multiple columns across multiple entries' do
      allow(masking_config).to receive(:columns_config).and_return(
        [
          { 'public.users.ssn' => {},
            'public.users.email' => {} },
          { 'public.accounts.card_number' => {} },
        ],
      )
      columns = job.send(:extract_columns, masking_config)
      expect(columns.map(&:id)).to contain_exactly(
        'public.users.ssn',
        'public.users.email',
        'public.accounts.card_number',
      )
    end
  end

  describe '#logger_adapter (private)' do
    let(:adapter) { job.send(:logger_adapter) }

    it 'returns an object with log_info' do
      expect(adapter).to respond_to(:log_info)
    end

    it 'returns an object with log_warn' do
      expect(adapter).to respond_to(:log_warn)
    end

    it 'returns an object with log_debug' do
      expect(adapter).to respond_to(:log_debug)
    end

    it 'delegates log_info to Rails.logger.info' do
      expect(Rails.logger).to receive(:info).with('test message')
      adapter.log_info('test message')
    end

    it 'delegates log_warn to Rails.logger.warn' do
      expect(Rails.logger).to receive(:warn).with('warn message')
      adapter.log_warn('warn message')
    end

    it 'delegates log_debug to Rails.logger.debug' do
      expect(Rails.logger).to receive(:debug).with('debug message')
      adapter.log_debug('debug message')
    end

    it 'memoizes the adapter' do
      expect(job.send(:logger_adapter)).to be(job.send(:logger_adapter))
    end
  end
end
