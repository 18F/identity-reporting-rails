# frozen_string_literal: true

require 'rails_helper'

# Explicitly load RedshiftMasking classes for testing
load Rails.root.join('app/services/redshift_masking/models.rb')
load Rails.root.join('app/services/redshift_masking/database.rb')
load Rails.root.join('app/services/redshift_masking/user_resolver.rb')
load Rails.root.join('app/services/redshift_masking/policy_builder.rb')
load Rails.root.join('app/services/redshift_masking/drift_detector.rb')

RSpec.describe RedshiftMasking do
  describe RedshiftMasking::PolicyAttachment do
    let(:policy) do
      described_class.new(
        policy_name: 'mask_public_users_ssn',
        schema: 'public',
        table: 'users',
        column: 'ssn',
        grantee: 'IAM:alice',
        priority: 100,
      )
    end

    describe '#key' do
      it 'combines column_id and upper-cased grantee' do
        expect(policy.key).to eq('public.users.ssn::IAM:ALICE')
      end

      it 'converts grantee to uppercase in key' do
        policy_lowercase = described_class.new(
          policy_name: 'test',
          schema: 's',
          table: 't',
          column: 'c',
          grantee: 'iam:bob',
          priority: 100,
        )
        expect(policy_lowercase.key).to eq('s.t.c::IAM:BOB')
      end
    end

    describe '#column_id' do
      it 'returns schema.table.column' do
        expect(policy.column_id).to eq('public.users.ssn')
      end
    end

    describe '#matches?' do
      let(:matching_policy) do
        described_class.new(
          policy_name: 'mask_public_users_ssn',
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
      it 'parses valid schema.table.column identifier' do
        column = described_class.parse('public.users.ssn')
        expect(column.schema).to eq('public')
        expect(column.table).to eq('users')
        expect(column.column).to eq('ssn')
      end

      it 'returns nil for invalid identifier with fewer than 3 parts' do
        expect(described_class.parse('public.users')).to be_nil
      end

      it 'returns nil for invalid identifier with more than 3 parts' do
        expect(described_class.parse('catalog.public.users.ssn')).to be_nil
      end
    end

    describe '#id' do
      it 'returns schema.table.column' do
        column = described_class.new(schema: 'public', table: 'users', column: 'email')
        expect(column.id).to eq('public.users.email')
      end
    end

    describe '#to_h' do
      it 'returns hash representation' do
        column = described_class.new(schema: 'public', table: 'users', column: 'email')
        expect(column.to_h).to eq({ schema: 'public', table: 'users', column: 'email' })
      end
    end
  end

  describe RedshiftMasking::DriftDetector do
    let(:logger) { double('logger', log_info: nil, log_warn: nil, log_debug: nil) }
    let(:detector) { described_class.new(logger) }

    let(:expected_policy) do
      RedshiftMasking::PolicyAttachment.new(
        policy_name: 'mask_public_users_ssn',
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
          expect(logger).not_to receive(:log_warn)
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
            expect(logger).to receive(:log_warn).with(
              'MISSING: IAM:alice on public.users.ssn',
            )
            detector.detect([expected_policy], [], silent: false)
          end
        end

        context 'with silent: true' do
          it 'does not log warning for missing policy' do
            expect(logger).not_to receive(:log_warn)
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
            expect(logger).to receive(:log_warn).with(
              'EXTRA: IAM:alice on public.users.ssn',
            )
            detector.detect([], [actual_policy], silent: false)
          end
        end

        context 'with silent: true' do
          it 'does not log warning for extra policy' do
            expect(logger).not_to receive(:log_warn)
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
            expect(logger).to receive(:log_warn).with(
              'MISMATCH: IAM:alice on public.users.ssn ' \
              '(Expected mask_public_users_ssn Priority 100)',
            )
            detector.detect([expected_policy], [mismatched_actual], silent: false)
          end
        end

        context 'with silent: true' do
          it 'does not log warning for mismatched policy' do
            expect(logger).not_to receive(:log_warn)
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
            expect(logger).to receive(:log_warn).exactly(3).times
            detector.detect(
              [missing_policy, mismatched_expected],
              [extra_policy, mismatched_actual],
              silent: false,
            )
          end
        end

        context 'with silent: true' do
          it 'does not log any warnings' do
            expect(logger).not_to receive(:log_warn)
            detector.detect(
              [missing_policy, mismatched_expected],
              [extra_policy, mismatched_actual],
              silent: true,
            )
          end
        end
      end

      it 'always logs info message about detecting drift' do
        expect(logger).to receive(:log_info).with('detecting drift in masking policies')
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
              'public.users.ssn' => {
                'allowed' => ['dwadmin'],
                'masked' => ['dwuser'],
                'denied' => ['analyst'],
              },
            },
          ],
        },
      }
    end

    let(:users_yaml) { { 'alice' => { 'aws_groups' => ['engineers'] } } }
    let(:config) { described_class.new(data_controls, users_yaml, env_name: 'test') }

    describe '#user_types' do
      it 'returns user_types configuration' do
        expect(config.user_types).to eq(data_controls['masking_policies']['user_types'])
      end
    end

    describe '#columns_config' do
      it 'returns columns configuration' do
        expect(config.columns_config).to eq(data_controls['masking_policies']['columns'])
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
end
