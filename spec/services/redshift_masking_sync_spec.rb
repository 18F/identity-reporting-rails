# frozen_string_literal: true

require 'rails_helper'

RSpec.describe RedshiftMasking do
  describe RedshiftMaskingSync do
    let(:sync) { described_class.new }
    let(:config) do
      instance_double(
        RedshiftMasking::Configuration, columns_config: [{ 'public.users.email' => {} }]
      )
    end
    let(:queries) { instance_double(RedshiftMasking::DatabaseQueries) }
    let(:builder) { instance_double(RedshiftMasking::PolicyBuilder) }
    let(:executor) { instance_double(RedshiftMasking::SqlExecutor) }
    let(:roles) do
      [{ 'role_name' => 'masking_worker', 'member_users' => ['fraudops_marts'] }]
    end
    let(:expected) { [] }
    let(:actual) { [attachment] }
    let(:grantor) { 'FRAUDOPS_MARTS' }
    let(:preservation_details) { [] }
    let(:attachment) do
      RedshiftMasking::PolicyAttachment.new(
        policy_name: 'dbt_mask', schema: 'public', table: 'users', column: 'email',
        grantee: 'IAM:alice', grantor: grantor, priority: 100
      )
    end

    before do
      allow(Rails.logger).to receive(:info)
      allow(Rails.logger).to receive(:debug) do |&message|
        preservation_details << message.call if message
      end
      allow(Rails.logger).to receive(:warn)
      allow(sync).to receive(:feature_enabled?).and_return(true)
      allow(RedshiftMasking::Configuration).to receive(:new).and_return(config)
      allow(RedshiftMasking::DatabaseQueries).to receive(:new).and_return(queries)
      allow(queries).to receive_messages(
        fetch_users: ['IAM:alice'], fetch_column_types: {}, fetch_existing_policies: actual,
      )
      allow(RedshiftMasking::UserResolver).to receive(:new).and_return(nil)
      allow(RedshiftMasking::PolicyBuilder).to receive(:new).and_return(builder)
      allow(builder).to receive(:build_expected_state).and_return(expected)
      allow(RedshiftMasking::SqlExecutor).to receive(:new).and_return(executor)
      allow(executor).to receive(:create_masking_policies)
      allow(executor).to receive(:apply_corrections)
    end

    def run_masking_sync(user_filter: nil)
      sync.send(:sync_masking_policies, {}, {}, roles, user_filter: user_filter)
    end

    it 'logs preservation without warnings or corrections for protected extra attachments' do
      run_masking_sync

      expect(Rails.logger).not_to have_received(:warn)
      expect(preservation_details).to contain_exactly(
        'Preserving dbt-managed masking attachment: policy=dbt_mask ' \
        'column=public.users.email grantee=IAM:alice grantor=FRAUDOPS_MARTS ' \
        'reason=protected_masking_worker_grantor',
      )
      expect(Rails.logger).not_to have_received(:info).with(/Preserving dbt-managed/)
      expect(Rails.logger).to have_received(:info).with(
        'Preserved 1 dbt-managed masking attachments; excluded from detach/replace corrections.',
      )
      expect(executor).to have_received(:apply_corrections).with(
        missing: [], extra: [], mismatched: [],
      )
    end

    context 'when a protected attachment mismatches the expected policy' do
      let(:expected) do
        [RedshiftMasking::PolicyAttachment.new(**attachment.to_h.merge(policy_name: 'rails_mask'))]
      end

      it 'preserves the attachment without logging an actionable mismatch or replacing it' do
        run_masking_sync

        expect(Rails.logger).not_to have_received(:warn)
        expect(preservation_details).to contain_exactly(a_string_matching(/Preserving dbt-managed/))
        expect(Rails.logger).not_to have_received(:info).with(/Preserving dbt-managed/)
        expect(executor).to have_received(:apply_corrections).with(
          missing: [], extra: [], mismatched: [],
        )
      end
    end

    context 'when an attachment has an unprotected grantor' do
      let(:grantor) { 'superuser' }

      it 'logs actionable drift and sends the attachment for correction' do
        run_masking_sync

        expect(Rails.logger).to have_received(:warn).with('EXTRA: IAM:alice on public.users.email')
        expect(Rails.logger).not_to have_received(:info).with(/Preserving dbt-managed/)
        expect(preservation_details).to be_empty
        expect(executor).to have_received(:apply_corrections).with(
          missing: [], extra: [attachment], mismatched: [],
        )
      end

      it 'keeps actionable warnings silent when a user filter is supplied' do
        run_masking_sync(user_filter: ['IAM:alice'])

        expect(Rails.logger).not_to have_received(:warn)
        expect(executor).to have_received(:apply_corrections).with(
          missing: [], extra: [attachment], mismatched: [],
        )
      end
    end

    context 'when an attachment is outside the configured schemas' do
      let(:grantor) { 'superuser' }
      let(:attachment) do
        RedshiftMasking::PolicyAttachment.new(
          policy_name: 'dbt_mask_varchar_max', schema: 'fraudops_qa_marts',
          table: 'orphan', column: 'email', grantee: 'PUBLIC', priority: 0,
          grantor: grantor
        )
      end

      it 'does not detach an admin-created dbt repair' do
        run_masking_sync

        expect(executor).to have_received(:apply_corrections).with(
          missing: [], extra: [], mismatched: [],
        )
      end
    end
  end

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

    describe '#grantor' do
      it 'defaults to nil for expected attachments' do
        expect(policy.grantor).to be_nil
      end

      it 'preserves the grantor for existing attachments' do
        existing_policy = described_class.new(**policy.to_h.merge(grantor: 'fraudops_marts'))

        expect(existing_policy.grantor).to eq('fraudops_marts')
        expect(existing_policy.to_h[:grantor]).to eq('fraudops_marts')
      end
    end

    describe '#key' do
      it 'keeps identifier components and grantee type separate' do
        expect(policy.key).to eq(['public', 'users', 'ssn', 'user', 'IAM:ALICE'])
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
        expect(policy_lowercase.key).to eq(['s', 't', 'c', 'user', 'IAM:BOB'])
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
            grantee_type: 'user',
            grantor: nil,
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
    let(:detector) { described_class.new }

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
              'MISSING: IAM:alice on public.users.ssn',
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
              'EXTRA: IAM:alice on public.users.ssn',
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
              'MISMATCH: IAM:alice on public.users.ssn ' \
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
