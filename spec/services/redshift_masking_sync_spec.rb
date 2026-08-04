# frozen_string_literal: true

require 'rails_helper'

RSpec.describe RedshiftMasking do
  describe RedshiftMasking::PolicyAttachment do
    let(:policy) do
      described_class.new(
        policy_name: 'mask_public_users_ssn',
        database: 'analytics',
        schema: 'public',
        table: 'users',
        column: 'ssn',
        grantee: 'IAM:alice',
        priority: 100,
      )
    end

    describe '#key' do
      it 'combines column_id and upper-cased grantee' do
        expect(policy.key).to eq('analytics.public.users.ssn::IAM:ALICE')
      end

      it 'converts grantee to uppercase in key' do
        policy_lowercase = described_class.new(
          policy_name: 'test',
          database: 'analytics',
          schema: 's',
          table: 't',
          column: 'c',
          grantee: 'iam:bob',
          priority: 100,
        )
        expect(policy_lowercase.key).to eq('analytics.s.t.c::IAM:BOB')
      end
    end

    describe '#column_id' do
      it 'returns database.schema.table.column' do
        expect(policy.column_id).to eq('analytics.public.users.ssn')
      end
    end

    describe '#matches?' do
      let(:matching_policy) do
        described_class.new(
          policy_name: 'mask_public_users_ssn',
          database: 'analytics',
          schema: 'public',
          table: 'users',
          column: 'ssn',
          grantee: 'IAM:alice',
          priority: 100,
        )
      end

      let(:different_name) do
        described_class.new(
          policy_name: 'unmask_public_users_ssn',
          database: 'analytics',
          schema: 'public',
          table: 'users',
          column: 'ssn',
          grantee: 'IAM:alice',
          priority: 100,
        )
      end

      let(:different_priority) do
        described_class.new(
          policy_name: 'mask_public_users_ssn',
          database: 'analytics',
          schema: 'public',
          table: 'users',
          column: 'ssn',
          grantee: 'IAM:alice',
          priority: 300,
        )
      end

      it 'returns true when policy_name and priority match' do
        expect(policy.matches?(matching_policy)).to be true
      end

      it 'returns false when policy_name differs' do
        expect(policy.matches?(different_name)).to be false
      end

      it 'returns false when priority differs' do
        expect(policy.matches?(different_priority)).to be false
      end
    end

    describe '#to_h' do
      it 'returns hash representation of all attributes' do
        expect(policy.to_h).to eq(
          {
            policy_name: 'mask_public_users_ssn',
            database: 'analytics',
            schema: 'public',
            table: 'users',
            column: 'ssn',
            grantee: 'IAM:alice',
            priority: 100,
          },
        )
      end
    end
  end

  describe RedshiftMasking::Column do
    describe '.parse' do
      it 'parses valid schema.table.column identifier scoped to a database' do
        column = described_class.parse('public.users.ssn', database: 'analytics')
        expect(column.database).to eq('analytics')
        expect(column.schema).to eq('public')
        expect(column.table).to eq('users')
        expect(column.column).to eq('ssn')
      end

      it 'returns nil for invalid identifier with fewer than 3 parts' do
        expect(described_class.parse('public.users', database: 'analytics')).to be_nil
      end

      it 'returns nil for invalid identifier with more than 3 parts' do
        expect(
          described_class.parse('catalog.public.users.ssn', database: 'analytics'),
        ).to be_nil
      end
    end

    describe '#id' do
      it 'returns database.schema.table.column' do
        column = described_class.new(
          database: 'analytics', schema: 'public', table: 'users', column: 'email'
        )
        expect(column.id).to eq('analytics.public.users.email')
      end
    end

    describe '#unqualified_id' do
      it 'returns schema.table.column' do
        column = described_class.new(
          database: 'analytics', schema: 'public', table: 'users', column: 'email'
        )
        expect(column.unqualified_id).to eq('public.users.email')
      end
    end

    describe '#to_h' do
      it 'returns hash representation' do
        column = described_class.new(
          database: 'analytics', schema: 'public', table: 'users', column: 'email'
        )
        expect(column.to_h).to eq(
          { database: 'analytics', schema: 'public', table: 'users', column: 'email' },
        )
      end
    end
  end

  describe RedshiftMasking::DatabaseQueries do
    subject(:db_queries) { described_class.new(Rails.logger) }

    let(:connection) { instance_double(ActiveRecord::ConnectionAdapters::AbstractAdapter) }

    before do
      allow(DataWarehouseApplicationRecord).to receive(:connection).and_return(connection)
      allow(connection).to receive(:quote) { |v| "'#{v}'" }
    end

    describe '#fetch_column_types' do
      it 'preserves varchar length from svv_all_columns for policy compatibility' do
        columns = [RedshiftMasking::Column.new(
          database: 'analytics', schema: 'idp', table: 'users',
          column: 'otp_fingerprint'
        )]

        allow(connection).to receive(:execute).and_return(
          [
            {
              'database_name' => 'analytics',
              'schema_name' => 'idp',
              'table_name' => 'users',
              'column_name' => 'otp_fingerprint',
              'data_type' => 'character varying',
              'character_maximum_length' => 65_535,
            },
          ],
        )

        expect(db_queries.fetch_column_types(columns)).to eq(
          'analytics.idp.users.otp_fingerprint' => 'VARCHAR(65535)',
        )
      end

      it 'maps text columns to VARCHAR(MAX)' do
        columns = [RedshiftMasking::Column.new(
          database: 'analytics', schema: 'idp', table: 'users', column: 'notes'
        )]

        allow(connection).to receive(:execute).and_return(
          [
            {
              'database_name' => 'analytics',
              'schema_name' => 'idp',
              'table_name' => 'users',
              'column_name' => 'notes',
              'data_type' => 'text',
              'character_maximum_length' => nil,
            },
          ],
        )

        expect(db_queries.fetch_column_types(columns)).to eq(
          'analytics.idp.users.notes' => 'VARCHAR(MAX)',
        )
      end

      it 'queries svv_all_columns scoped by database_name' do
        columns = [RedshiftMasking::Column.new(
          database: 'analytics_zetl', schema: 'public', table: 'users', column: 'ssn'
        )]

        allow(connection).to receive(:execute).and_return([])

        db_queries.fetch_column_types(columns)

        expect(connection).to have_received(:execute).with(
          a_string_matching(/FROM svv_all_columns/).
            and(a_string_matching(/database_name = 'analytics_zetl'/)),
        )
      end
    end
  end

  describe RedshiftMasking::DriftDetector do
    let(:detector) { described_class.new }

    let(:expected_policy) do
      RedshiftMasking::PolicyAttachment.new(
        policy_name: 'mask_public_users_ssn',
        database: 'analytics',
        schema: 'public',
        table: 'users',
        column: 'ssn',
        grantee: 'IAM:alice',
        priority: 100,
      )
    end

    let(:actual_policy) do
      RedshiftMasking::PolicyAttachment.new(
        policy_name: 'mask_public_users_ssn',
        database: 'analytics',
        schema: 'public',
        table: 'users',
        column: 'ssn',
        grantee: 'IAM:alice',
        priority: 100,
      )
    end

    describe '#detect' do
      context 'when policies match perfectly' do
        it 'returns empty drift' do
          drift = detector.detect([expected_policy], [actual_policy])
          expect(drift[:missing]).to be_empty
          expect(drift[:extra]).to be_empty
          expect(drift[:mismatched]).to be_empty
        end

        it 'does not log any warnings' do
          expect(Rails.logger).not_to receive(:warn)
          detector.detect([expected_policy], [actual_policy])
        end
      end

      context 'when policy is missing' do
        it 'adds to missing drift' do
          drift = detector.detect([expected_policy], [])
          expect(drift[:missing]).to contain_exactly(expected_policy)
        end

        context 'with silent: false (default)' do
          it 'logs warning for missing policy' do
            expect(Rails.logger).to receive(:warn).with(
              'MISSING: IAM:alice on analytics.public.users.ssn',
            )
            detector.detect([expected_policy], [], silent: false)
          end
        end

        context 'with silent: true' do
          it 'does not log warning for missing policy' do
            expect(Rails.logger).not_to receive(:warn)
            detector.detect([expected_policy], [], silent: true)
          end

          it 'still detects missing policy in drift' do
            drift = detector.detect([expected_policy], [], silent: true)
            expect(drift[:missing]).to contain_exactly(expected_policy)
          end
        end
      end

      context 'when policy is extra' do
        it 'adds to extra drift' do
          drift = detector.detect([], [actual_policy])
          expect(drift[:extra]).to contain_exactly(actual_policy)
        end

        context 'with silent: false (default)' do
          it 'logs warning for extra policy' do
            expect(Rails.logger).to receive(:warn).with(
              'EXTRA: IAM:alice on analytics.public.users.ssn',
            )
            detector.detect([], [actual_policy], silent: false)
          end
        end

        context 'with silent: true' do
          it 'does not log warning for extra policy' do
            expect(Rails.logger).not_to receive(:warn)
            detector.detect([], [actual_policy], silent: true)
          end

          it 'still detects extra policy in drift' do
            drift = detector.detect([], [actual_policy], silent: true)
            expect(drift[:extra]).to contain_exactly(actual_policy)
          end
        end
      end

      context 'when policy is mismatched' do
        let(:mismatched_actual) do
          RedshiftMasking::PolicyAttachment.new(
            policy_name: 'unmask_public_users_ssn',
            database: 'analytics',
            schema: 'public',
            table: 'users',
            column: 'ssn',
            grantee: 'IAM:alice',
            priority: 300,
          )
        end

        it 'adds to mismatched drift with both expected and actual' do
          drift = detector.detect([expected_policy], [mismatched_actual])
          expect(drift[:mismatched].size).to eq(1)
          expect(drift[:mismatched].first[:expected]).to eq(expected_policy)
          expect(drift[:mismatched].first[:actual]).to eq(mismatched_actual)
        end

        context 'with silent: false (default)' do
          it 'logs warning for mismatched policy' do
            expect(Rails.logger).to receive(:warn).with(
              'MISMATCH: IAM:alice on analytics.public.users.ssn ' \
              '(Expected mask_public_users_ssn Priority 100)',
            )
            detector.detect([expected_policy], [mismatched_actual], silent: false)
          end
        end

        context 'with silent: true' do
          it 'does not log warning for mismatched policy' do
            expect(Rails.logger).not_to receive(:warn)
            detector.detect([expected_policy], [mismatched_actual], silent: true)
          end

          it 'still detects mismatched policy in drift' do
            drift = detector.detect([expected_policy], [mismatched_actual], silent: true)
            expect(drift[:mismatched].size).to eq(1)
          end
        end
      end

      context 'with multiple drift types' do
        let(:extra_policy) do
          RedshiftMasking::PolicyAttachment.new(
            policy_name: 'mask_public_users_email',
            database: 'analytics',
            schema: 'public',
            table: 'users',
            column: 'email',
            grantee: 'IAM:bob',
            priority: 100,
          )
        end

        let(:missing_policy) do
          RedshiftMasking::PolicyAttachment.new(
            policy_name: 'mask_public_users_phone',
            database: 'analytics',
            schema: 'public',
            table: 'users',
            column: 'phone',
            grantee: 'IAM:charlie',
            priority: 100,
          )
        end

        let(:mismatched_expected) do
          RedshiftMasking::PolicyAttachment.new(
            policy_name: 'unmask_public_users_address',
            database: 'analytics',
            schema: 'public',
            table: 'users',
            column: 'address',
            grantee: 'IAM:dave',
            priority: 300,
          )
        end

        let(:mismatched_actual) do
          RedshiftMasking::PolicyAttachment.new(
            policy_name: 'mask_public_users_address',
            database: 'analytics',
            schema: 'public',
            table: 'users',
            column: 'address',
            grantee: 'IAM:dave',
            priority: 100,
          )
        end

        it 'detects all drift types correctly' do
          expected = [missing_policy, mismatched_expected]
          actual = [extra_policy, mismatched_actual]

          drift = detector.detect(expected, actual)

          expect(drift[:missing]).to contain_exactly(missing_policy)
          expect(drift[:extra]).to contain_exactly(extra_policy)
          expect(drift[:mismatched].size).to eq(1)
          expect(drift[:mismatched].first[:expected]).to eq(mismatched_expected)
          expect(drift[:mismatched].first[:actual]).to eq(mismatched_actual)
        end

        context 'with silent: false' do
          it 'logs all warnings' do
            expect(Rails.logger).to receive(:warn).exactly(3).times
            detector.detect(
              [missing_policy, mismatched_expected],
              [extra_policy, mismatched_actual],
              silent: false,
            )
          end
        end

        context 'with silent: true' do
          it 'does not log any warnings' do
            expect(Rails.logger).not_to receive(:warn)
            detector.detect(
              [missing_policy, mismatched_expected],
              [extra_policy, mismatched_actual],
              silent: true,
            )
          end
        end
      end

      it 'always logs info message about detecting drift' do
        expect(Rails.logger).to receive(:info).with('detecting drift in masking policies')
        detector.detect([expected_policy], [actual_policy])
      end
    end
  end

  describe RedshiftMasking::Configuration do
    # cSpell:ignore dwuser dwadmin
    let(:data_controls) do
      {
        'masking_policies' => {
          'user_types' => {
            'iam_role' => ['dwuser', 'analyst'],
            'redshift_user' => ['etl_user'],
            'superuser' => ['admin'],
          },
          'columns' => [
            {
              'db' => 'analytics',
              'tables' => [
                {
                  'public.users.ssn' => {
                    'allowed' => ['dwadmin'],
                    'masked' => ['dwuser'],
                    'denied' => ['analyst'],
                  },
                },
              ],
            },
            {
              'db' => 'analytics_zetl',
              'tables' => [
                {
                  'public.users.email' => {
                    'allowed' => [],
                    'masked' => ['dwuser'],
                    'denied' => [],
                  },
                },
              ],
            },
          ],
        },
      }
    end

    let(:users_yaml) { { 'alice' => { 'aws_groups' => ['engineers'] } } }
    let(:config) do
      described_class.new(data_controls, users_yaml, env_name: 'test')
    end

    describe '#user_types' do
      it 'returns user_types configuration' do
        expect(config.user_types).to eq(data_controls['masking_policies']['user_types'])
      end
    end

    describe '#databases' do
      it 'returns every configured database name' do
        expect(config.databases).to eq(['analytics', 'analytics_zetl'])
      end
    end

    describe '#each_column' do
      it 'yields [database, column_id, permissions] for every db group' do
        yielded = config.each_column.to_a

        expect(yielded).to contain_exactly(
          [
            'analytics', 'public.users.ssn',
            { 'allowed' => ['dwadmin'], 'masked' => ['dwuser'], 'denied' => ['analyst'] }
          ],
          [
            'analytics_zetl', 'public.users.email',
            { 'allowed' => [], 'masked' => ['dwuser'], 'denied' => [] }
          ],
        )
      end

      it 'returns an Enumerator when called without a block' do
        expect(config.each_column).to be_a(Enumerator)
      end
    end

    describe '#policy_config' do
      it 'returns configuration for allowed permission' do
        result = config.policy_config(RedshiftMasking::Configuration::PERMISSION_ALLOWED)
        expect(result[:policy]).to eq('unmask')
        expect(result[:priority]).to eq(300)
      end

      it 'returns configuration for denied permission' do
        result = config.policy_config(RedshiftMasking::Configuration::PERMISSION_DENIED)
        expect(result[:policy]).to eq('deny')
        expect(result[:priority]).to eq(200)
      end

      it 'returns configuration for masked permission' do
        result = config.policy_config(RedshiftMasking::Configuration::PERMISSION_MASKED)
        expect(result[:policy]).to eq('mask')
        expect(result[:priority]).to eq(100)
      end
    end

    describe '#policy_name' do
      it 'builds policy name for allowed permission' do
        name = config.policy_name(
          RedshiftMasking::Configuration::PERMISSION_ALLOWED,
          'public.users.ssn',
        )
        expect(name).to eq('unmask_public_users_ssn')
      end

      it 'builds policy name for masked permission' do
        name = config.policy_name(
          RedshiftMasking::Configuration::PERMISSION_MASKED,
          'public.users.ssn',
        )
        expect(name).to eq('mask_public_users_ssn')
      end

      it 'replaces dots with underscores in column_id' do
        name = config.policy_name(
          RedshiftMasking::Configuration::PERMISSION_MASKED,
          'schema.table.column',
        )
        expect(name).to eq('mask_schema_table_column')
      end
    end

    describe '#policy_priority' do
      it 'returns priority for allowed permission' do
        permission = RedshiftMasking::Configuration::PERMISSION_ALLOWED
        expect(config.policy_priority(permission)).to eq(300)
      end

      it 'returns priority for denied permission' do
        permission = RedshiftMasking::Configuration::PERMISSION_DENIED
        expect(config.policy_priority(permission)).to eq(200)
      end

      it 'returns priority for masked permission' do
        permission = RedshiftMasking::Configuration::PERMISSION_MASKED
        expect(config.policy_priority(permission)).to eq(100)
      end
    end

    describe '#policy_details' do
      it 'returns name and priority for permission type' do
        details = config.policy_details(
          RedshiftMasking::Configuration::PERMISSION_ALLOWED,
          'public.users.ssn',
        )
        expect(details[:name]).to eq('unmask_public_users_ssn')
        expect(details[:priority]).to eq(300)
      end

      it 'returns nil for invalid permission type' do
        expect(config.policy_details('invalid', 'public.users.ssn')).to be_nil
      end
    end
  end

  describe RedshiftMasking::SqlExecutor do
    subject(:executor) { described_class.new(config) }

    let(:connection) { instance_double(ActiveRecord::ConnectionAdapters::AbstractAdapter) }
    let(:config) { instance_double(RedshiftMasking::Configuration) }

    before do
      allow(DataWarehouseApplicationRecord).to receive(:connection).and_return(connection)
      allow(connection).to receive(:execute)
      allow(connection).to receive(:quote_column_name) { |v| "\"#{v}\"" }
      allow(config).to receive(:policy_name) do |permission_type, column_id|
        "#{permission_type}_#{column_id.tr('.', '_')}"
      end
    end

    it 'uses a valid timestamp masked literal for TIMESTAMP columns' do
      executor.create_masking_policies('analytics.idp.events.created_at' => 'TIMESTAMP')

      expect(connection).to have_received(:execute).with(
        a_string_including("'1970-01-01 00:00:00'::TIMESTAMP"),
      )
    end

    it 'uses a numeric masked literal for NUMERIC columns' do
      executor.create_masking_policies('analytics.idp.events.count' => 'NUMERIC')

      expect(connection).to have_received(:execute).with(
        a_string_including('USING (0::NUMERIC)'),
      )
    end

    it 'creates a policy for every permission type, including masked' do
      executor.create_masking_policies('analytics.idp.users.ssn' => 'VARCHAR(65535)')

      expect(connection).to have_received(:execute).with(
        a_string_matching(/CREATE MASKING POLICY allowed_idp_users_ssn/).
          and(a_string_matching(/CREATE MASKING POLICY denied_idp_users_ssn/)).
          and(a_string_matching(/CREATE MASKING POLICY masked_idp_users_ssn/)),
      )
    end

    it 'creates policies once for a column shared across databases (db-less names)' do
      executor.create_masking_policies(
        'analytics.idp.users.ssn' => 'VARCHAR(65535)',
        'analytics_zetl.idp.users.ssn' => 'VARCHAR(65535)',
      )

      create_sql = nil
      expect(connection).to have_received(:execute) { |sql| create_sql = sql }
      expect(create_sql.scan(/CREATE MASKING POLICY masked_idp_users_ssn/).size).to eq(1)
      expect(create_sql).not_to include('analytics.masked')
    end

    it 'attaches cross-database using database-qualified policy and relation' do
      policy = RedshiftMasking::PolicyAttachment.new(
        policy_name: 'mask_public_users_email',
        database: 'analytics_zetl',
        schema: 'public',
        table: 'users',
        column: 'email',
        grantee: 'IAM:alice',
        priority: 100,
      )

      executor.apply_corrections(missing: [policy], extra: [], mismatched: [])

      expect(connection).to have_received(:execute).with(
        a_string_matching(/ATTACH MASKING POLICY analytics_zetl\.mask_public_users_email/).
          and(a_string_matching(/ON analytics_zetl\.public\.users \(email\)/)),
      )
    end
  end

  describe RedshiftMaskingSync do
    subject(:service) { described_class.new }

    let(:data_controls) do
      {
        'masking_policies' => {
          'user_types' => {
            'redshift_user' => ['pii_reader'],
            'iam_role' => ['dwadmin'],
            'superuser' => ['superuser'],
          },
          'columns' => [
            {
              'db' => 'analytics',
              'tables' => [
                {
                  'idp.users.dw_secret' => {
                    'allowed' => ['pii_reader'],
                    'masked' => ['dwadmin'],
                    'denied' => [],
                  },
                },
              ],
            },
            {
              'db' => 'analytics_zetl',
              'tables' => [
                {
                  'idp.users.zetl_secret' => {
                    'allowed' => ['pii_reader'],
                    'masked' => ['dwadmin'],
                    'denied' => [],
                  },
                },
              ],
            },
          ],
        },
      }
    end

    let(:users_yaml) { { 'users' => { 'alice' => { 'aws_groups' => ['dwadmin'] } } } }

    let(:connection) { instance_double(ActiveRecord::ConnectionAdapters::AbstractAdapter) }
    let(:executed_sql) { [] }

    before do
      allow(Identity::Hostdata).to receive(:env).and_return('test')

      allow(File).to receive(:read).with(RedshiftMaskingSync::DATA_CONTROLS_PATH).
        and_return(data_controls.to_yaml)
      allow(File).to receive(:read).with(IdentityConfig.identity_devops_users_yaml_path).
        and_return(users_yaml.to_yaml)

      # All databases are synced through the single data_warehouse connection.
      allow(DataWarehouseApplicationRecord).to receive(:connection).and_return(connection)
      allow(connection).to receive(:quote) { |v| "'#{v}'" }
      allow(connection).to receive(:quote_column_name) { |v| "\"#{v}\"" }

      allow(connection).to receive(:execute) do |sql|
        executed_sql << sql
        case sql
        when /FROM pg_user/
          [{ 'usename' => 'pii_reader' }, { 'usename' => 'dwadmin' }]
        when /svv_all_columns/
          [
            {
              'database_name' => 'analytics',
              'schema_name' => 'idp', 'table_name' => 'users',
              'column_name' => 'dw_secret',
              'data_type' => 'character varying', 'character_maximum_length' => 255
            },
            {
              'database_name' => 'analytics_zetl',
              'schema_name' => 'idp', 'table_name' => 'users',
              'column_name' => 'zetl_secret',
              'data_type' => 'character varying', 'character_maximum_length' => 255
            },
          ]
        when /svv_attached_masking_policy/
          []
        else
          []
        end
      end
    end

    it 'reads column metadata across databases with a single connection' do
      service.sync

      expect(DataWarehouseApplicationRecord).to have_received(:connection).at_least(:once)
      column_sql = executed_sql.find { |s| s.include?('svv_all_columns') }
      expect(column_sql).to include("database_name = 'analytics'").
        and include("database_name = 'analytics_zetl'")
    end

    it 'creates policies (db-less names) for columns in every db group' do
      service.sync

      create_sql = executed_sql.find { |s| s.include?('CREATE MASKING POLICY') }
      expect(create_sql).to include('mask_idp_users_dw_secret').
        and include('mask_idp_users_zetl_secret')
    end

    it 'attaches policies cross-database with db-qualified names and relations' do
      service.sync

      attach_sql = executed_sql.select { |s| s.include?('ATTACH MASKING POLICY') }

      # No superuser in +allowed+, so this takes the public-baseline path:
      # PUBLIC is masked at priority 10, and the +allowed+ redshift_user
      # (pii_reader) is unmasked at priority 300 — in each database.
      expect(attach_sql).to include(
        a_string_matching(/analytics\.mask_idp_users_dw_secret/).
          and(a_string_matching(/ON analytics\.idp\.users/)).
          and(a_string_matching(/PUBLIC/)).
          and(a_string_matching(/PRIORITY 10/)),
      )
      expect(attach_sql).to include(
        a_string_matching(/analytics_zetl\.unmask_idp_users_zetl_secret/).
          and(a_string_matching(/ON analytics_zetl\.idp\.users/)).
          and(a_string_matching(/"pii_reader"/)).
          and(a_string_matching(/PRIORITY 300/)),
      )
    end

    it 'logs sync completion' do
      allow(Rails.logger).to receive(:info)

      service.sync

      expect(Rails.logger).to have_received(:info).with('sync completed')
    end
  end
end
