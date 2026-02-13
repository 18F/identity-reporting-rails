# frozen_string_literal: true

require 'rspec'
require_relative '../users/lib/common'
require_relative '../users/mask'

RSpec.describe RedshiftMasking do
  describe RedshiftMasking::PolicyAttachment do
    let(:policy) do
      described_class.new(
        policy_name: 'mask_schema_table_col',
        schema: 'public',
        table: 'users',
        column: 'ssn',
        grantee: 'testuser',
        priority: 100
      )
    end

    describe '#key' do
      it 'returns unique key for schema.table.column::grantee' do
        expect(policy.key).to eq('public.users.ssn::TESTUSER')
      end
    end

    describe '#matches?' do
      it 'returns true when policy_name and priority match' do
        other = described_class.new(
          policy_name: 'mask_schema_table_col',
          schema: 'public',
          table: 'users',
          column: 'ssn',
          grantee: 'testuser',
          priority: 100
        )
        expect(policy.matches?(other)).to be true
      end

      it 'returns false when policy_name differs' do
        other = described_class.new(
          policy_name: 'unmask_schema_table_col',
          schema: 'public',
          table: 'users',
          column: 'ssn',
          grantee: 'testuser',
          priority: 100
        )
        expect(policy.matches?(other)).to be false
      end

      it 'returns false when priority differs' do
        other = described_class.new(
          policy_name: 'mask_schema_table_col',
          schema: 'public',
          table: 'users',
          column: 'ssn',
          grantee: 'testuser',
          priority: 200
        )
        expect(policy.matches?(other)).to be false
      end
    end

    describe '#column_id' do
      it 'returns schema.table.column identifier' do
        expect(policy.column_id).to eq('public.users.ssn')
      end
    end
  end

  describe RedshiftMasking::Column do
    describe '.parse' do
      it 'parses valid column identifier' do
        column = described_class.parse('public.users.email')
        expect(column.schema).to eq('public')
        expect(column.table).to eq('users')
        expect(column.column).to eq('email')
      end

      it 'returns nil for invalid identifier' do
        expect(described_class.parse('invalid')).to be_nil
        expect(described_class.parse('schema.table')).to be_nil
      end
    end

    describe '#id' do
      it 'returns schema.table.column identifier' do
        column = described_class.new(schema: 'public', table: 'users', column: 'email')
        expect(column.id).to eq('public.users.email')
      end
    end
  end

  describe RedshiftMasking::Configuration do
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
    let(:users_yaml) { { 'user1' => {} } }
    let(:config) { described_class.new(data_controls, users_yaml) }

    describe '#policy_name' do
      it 'generates policy name for allowed permission' do
        expect(config.policy_name('allowed', 'public.users.ssn')).to eq('unmask_public_users_ssn')
      end

      it 'generates policy name for masked permission' do
        expect(config.policy_name('masked', 'public.users.ssn')).to eq('mask_public_users_ssn')
      end

      it 'generates policy name for denied permission' do
        expect(config.policy_name('denied', 'public.users.ssn')).to eq('deny_public_users_ssn')
      end

      it 'returns nil for unknown permission type' do
        expect(config.policy_name('unknown', 'public.users.ssn')).to be_nil
      end
    end

    describe '#policy_priority' do
      it 'returns correct priority for allowed' do
        expect(config.policy_priority('allowed')).to eq(300)
      end

      it 'returns correct priority for masked' do
        expect(config.policy_priority('masked')).to eq(100)
      end

      it 'returns correct priority for denied' do
        expect(config.policy_priority('denied')).to eq(200)
      end
    end
  end

  describe RedshiftMasking::DatabaseQueries do
    let(:executor) { instance_double(RedshiftCommon::QueryExecutor) }
    let(:logger) { double('logger', log_info: nil, log_warn: nil) }
    let(:db_queries) { described_class.new(executor, logger) }

    describe '#fetch_column_types' do
      let(:columns) do
        [
          RedshiftMasking::Column.new(schema: 'public', table: 'users', column: 'email'),
          RedshiftMasking::Column.new(schema: 'public', table: 'users', column: 'ssn'),
        ]
      end

      it 'returns empty hash for empty columns' do
        expect(db_queries.fetch_column_types([])).to eq({})
      end

      it 'fetches and normalizes column types' do
        allow(executor).to receive(:query_records).and_return([
                                                                [
                                                                  { string_value: 'public' },
                                                                  { string_value: 'users' },
                                                                  { string_value: 'email' },
                                                                  { string_value: 'varchar' },
                                                                  { long_value: nil },
                                                                ],
                                                                [
                                                                  { string_value: 'public' },
                                                                  { string_value: 'users' },
                                                                  { string_value: 'ssn' },
                                                                  { string_value: 'char' },
                                                                  { long_value: 11 },
                                                                ],
                                                              ])

        result = db_queries.fetch_column_types(columns)
        expect(result).to eq({
                               'public.users.email' => 'VARCHAR(MAX)',
                               'public.users.ssn' => 'CHAR(11)',
                             })
      end
    end

    describe '#fetch_existing_policies' do
      it 'returns array of PolicyAttachment objects' do
        allow(executor).to receive(:query_records).and_return([
                                                                [
                                                                  { string_value: 'mask_public_users_ssn' },
                                                                  { string_value: 'public' },
                                                                  { string_value: 'users' },
                                                                  { string_value: 'ssn' },
                                                                  { string_value: 'testuser' },
                                                                  { long_value: 100 },
                                                                ],
                                                              ])

        result = db_queries.fetch_existing_policies
        expect(result).to be_an(Array)
        expect(result.first).to be_a(RedshiftMasking::PolicyAttachment)
        expect(result.first.policy_name).to eq('mask_public_users_ssn')
      end
    end
  end

  describe RedshiftMasking::UserResolver do
    let(:config) do
      instance_double(
        RedshiftMasking::Configuration,
        user_types: {
          'iam_role' => ['dwuser'],
          'redshift_user' => ['analyst'],
          'superuser' => ['admin'],
        },
        env_name: 'test'
      )
    end
    let(:users_yaml) do
      {
        'alice' => { 'aws_groups' => ['dwuser'] },
        'bob' => { 'aws_groups' => ['dwpoweruser'] },
      }
    end
    let(:db_user_case_map) { { 'IAM:ALICE' => 'IAM:alice', 'IAM:BOB' => 'IAM:bob', 'ANALYST' => 'analyst' } }
    let(:logger) { double('logger', log_info: nil, log_warn: nil) }
    let(:resolver) { described_class.new(config, users_yaml, db_user_case_map, logger) }

    describe '#resolve_attachable_users' do
      it 'returns empty set for nil role names' do
        expect(resolver.resolve_attachable_users(nil)).to eq(Set.new)
      end

      it 'resolves IAM role to users' do
        result = resolver.resolve_attachable_users(['dwuser'])
        expect(result).to include('IAM:alice')
      end

      it 'resolves redshift user' do
        result = resolver.resolve_attachable_users(['analyst'])
        expect(result).to include('analyst')
      end

      it 'skips superuser roles' do
        result = resolver.resolve_attachable_users(['admin'])
        expect(result).to be_empty
      end
    end

    describe '#superuser_allowed?' do
      it 'returns true when superuser is in allowed list' do
        permissions = { 'allowed' => ['admin'] }
        expect(resolver.superuser_allowed?(permissions)).to be true
      end

      it 'returns false when superuser is not in allowed list' do
        permissions = { 'allowed' => ['dwuser'] }
        expect(resolver.superuser_allowed?(permissions)).to be false
      end

      it 'returns false when permissions is nil' do
        expect(resolver.superuser_allowed?(nil)).to be false
      end
    end

    describe '#find_implicitly_masked_users' do
      it 'returns users not in any explicit permission set' do
        explicit_sets = {
          'allowed' => Set.new(['IAM:alice']),
          'denied' => Set.new,
          'masked' => Set.new,
        }
        all_db_users = Set.new(['IAM:ALICE', 'IAM:BOB', 'ANALYST'])

        result = resolver.find_implicitly_masked_users(explicit_sets, all_db_users)
        expect(result).to contain_exactly('IAM:bob', 'analyst')
      end
    end
  end

  describe RedshiftMasking::DriftDetector do
    let(:logger) { double('logger', log_info: nil, log_warn: nil, log_error: nil) }
    let(:detector) { described_class.new(logger) }

    let(:expected_policy) do
      RedshiftMasking::PolicyAttachment.new(
        policy_name: 'mask_public_users_ssn',
        schema: 'public',
        table: 'users',
        column: 'ssn',
        grantee: 'testuser',
        priority: 100
      )
    end

    describe '#detect' do
      it 'detects missing policies' do
        actual = []
        drift = detector.detect([expected_policy], actual)

        expect(drift[:missing]).to eq([expected_policy])
        expect(drift[:extra]).to be_empty
        expect(drift[:mismatched]).to be_empty
      end

      it 'detects extra policies' do
        expected = []
        drift = detector.detect(expected, [expected_policy])

        expect(drift[:missing]).to be_empty
        expect(drift[:extra]).to eq([expected_policy])
        expect(drift[:mismatched]).to be_empty
      end

      it 'detects mismatched policies' do
        actual_policy = RedshiftMasking::PolicyAttachment.new(
          policy_name: 'unmask_public_users_ssn',
          schema: 'public',
          table: 'users',
          column: 'ssn',
          grantee: 'testuser',
          priority: 300
        )

        drift = detector.detect([expected_policy], [actual_policy])

        expect(drift[:missing]).to be_empty
        expect(drift[:extra]).to be_empty
        expect(drift[:mismatched].length).to eq(1)
        expect(drift[:mismatched].first[:expected]).to eq(expected_policy)
        expect(drift[:mismatched].first[:actual]).to eq(actual_policy)
      end

      it 'returns empty drift when policies match' do
        drift = detector.detect([expected_policy], [expected_policy])

        expect(drift[:missing]).to be_empty
        expect(drift[:extra]).to be_empty
        expect(drift[:mismatched]).to be_empty
      end
    end
  end

  describe RedshiftMasking::SqlExecutor do
    let(:executor) { instance_double(RedshiftCommon::QueryExecutor) }
    let(:config) { instance_double(RedshiftMasking::Configuration) }
    let(:logger) { double('logger', log_info: nil, log_warn: nil) }
    let(:sql_executor) { described_class.new(executor, config, logger, dry_run: false) }

    describe '#create_masking_policies' do
      it 'does nothing for empty column types' do
        allow(executor).to receive(:execute_and_wait)
        sql_executor.create_masking_policies({})
        expect(executor).not_to have_received(:execute_and_wait)
      end

      it 'creates policies for each column type' do
        allow(config).to receive(:policy_name).and_return('test_policy')
        allow(executor).to receive(:execute_and_wait)

        column_types = { 'public.users.ssn' => 'VARCHAR(MAX)' }
        sql_executor.create_masking_policies(column_types)

        expect(executor).to have_received(:execute_and_wait)
      end
    end

    describe '#apply_corrections' do
      let(:policy) do
        RedshiftMasking::PolicyAttachment.new(
          policy_name: 'mask_public_users_ssn',
          schema: 'public',
          table: 'users',
          column: 'ssn',
          grantee: 'testuser',
          priority: 100
        )
      end

      it 'does nothing when no drift' do
        allow(executor).to receive(:execute_and_wait)
        drift = { missing: [], extra: [], mismatched: [] }
        sql_executor.apply_corrections(drift)
        expect(executor).not_to have_received(:execute_and_wait)
      end

      it 'detaches extra policies' do
        allow(executor).to receive(:execute_and_wait)
        drift = { missing: [], extra: [policy], mismatched: [] }

        sql_executor.apply_corrections(drift)
        expect(executor).to have_received(:execute_and_wait).with(/DETACH MASKING POLICY/)
      end

      it 'attaches missing policies' do
        allow(executor).to receive(:execute_and_wait)
        drift = { missing: [policy], extra: [], mismatched: [] }

        sql_executor.apply_corrections(drift)
        expect(executor).to have_received(:execute_and_wait).with(/ATTACH MASKING POLICY/)
      end

      it 'detaches then attaches mismatched policies' do
        allow(executor).to receive(:execute_and_wait)

        expected = policy
        actual = RedshiftMasking::PolicyAttachment.new(
          policy_name: 'unmask_public_users_ssn',
          schema: 'public',
          table: 'users',
          column: 'ssn',
          grantee: 'testuser',
          priority: 300
        )

        drift = { missing: [], extra: [], mismatched: [{ expected: expected, actual: actual }] }

        sql_executor.apply_corrections(drift)
        expect(executor).to have_received(:execute_and_wait).twice
      end

      context 'when dry_run is true' do
        it 'logs without executing' do
          dry_run_executor = described_class.new(executor, config, logger, dry_run: true)
          allow(executor).to receive(:execute_and_wait)
          drift = { missing: [policy], extra: [], mismatched: [] }
          dry_run_executor.apply_corrections(drift)

          expect(executor).not_to have_received(:execute_and_wait)
          expect(logger).to have_received(:log_info).with(/DRY RUN/).twice
        end
      end
    end

    describe 'SQL generation' do
      let(:policy) do
        RedshiftMasking::PolicyAttachment.new(
          policy_name: 'mask_public_users_ssn',
          schema: 'public',
          table: 'users',
          column: 'ssn',
          grantee: 'testuser',
          priority: 100
        )
      end

      it 'generates multi-line detach SQL' do
        sql = sql_executor.send(:detach_sql, policy)
        expect(sql).to include('DETACH MASKING POLICY')
        expect(sql).to include('ON public.users (ssn)')
        expect(sql).to include('FROM "testuser"')
      end

      it 'generates multi-line attach SQL' do
        sql = sql_executor.send(:attach_sql, policy)
        expect(sql).to include('ATTACH MASKING POLICY')
        expect(sql).to include('ON public.users (ssn)')
        expect(sql).to include('TO "testuser"')
        expect(sql).to include('PRIORITY 100')
      end

      it 'does not quote PUBLIC grantee' do
        public_policy = RedshiftMasking::PolicyAttachment.new(
          policy_name: 'mask_public_users_ssn',
          schema: 'public',
          table: 'users',
          column: 'ssn',
          grantee: 'PUBLIC',
          priority: 10
        )

        sql = sql_executor.send(:attach_sql, public_policy)
        expect(sql).to include('TO PUBLIC')
        expect(sql).not_to include('"PUBLIC"')
      end
    end
  end

  describe RedshiftMasking::PolicyBuilder do
    let(:config) do
      instance_double(
        RedshiftMasking::Configuration,
        columns_config: [
          { 'public.users.ssn' => { 'allowed' => ['dwadmin'], 'masked' => ['dwuser'] } },
        ]
      )
    end
    let(:user_resolver) { instance_double(RedshiftMasking::UserResolver) }
    let(:logger) { double('logger', log_info: nil, log_warn: nil) }
    let(:builder) { described_class.new(config, user_resolver, logger) }

    describe '#build_expected_state' do
      let(:column_types) { { 'public.users.ssn' => 'VARCHAR(MAX)' } }
      let(:db_users) { Set.new(['IAM:ALICE', 'IAM:BOB']) }

      before do
        allow(config).to receive(:policy_name).and_return('mask_public_users_ssn')
        allow(config).to receive(:policy_priority).and_return(100)
        allow(user_resolver).to receive(:superuser_allowed?).and_return(false)
        allow(user_resolver).to receive(:resolve_attachable_users).and_return(Set.new)
      end

      it 'builds policies for configured columns' do
        result = builder.build_expected_state(column_types, db_users)
        expect(result).to be_an(Array)
        expect(result).not_to be_empty
      end

      it 'skips columns not in column_types' do
        result = builder.build_expected_state({}, db_users)
        expect(result).to be_empty
      end
    end
  end
end
