require 'rails_helper'

RSpec.describe QuicksightSync do
  let(:quicksight_client) { instance_double(Aws::QuickSight::Client) }
  let(:sts_client) { instance_double(Aws::STS::Client) }
  let(:account_id) { '123456789012' }

  let(:test_quicksight_config) do
    {
      'quicksight_aws_role' => {
        'DWAdmin' => 'ADMIN',
        'DWPowerUser' => 'AUTHOR',
        'DWUser' => 'AUTHOR',
        'default' => 'READER',
      },
      'quicksight_group' => {
        'DWAdmin' => 'QSAdmin',
        'DWPowerUser' => 'QSPowerUser',
        'DWUser' => 'QSUser',
        'default' => 'QSUser',
      },
      'protected_accounts' => ['identity-devops@login.gov'],
      'non_human_accounts' => ['root', 'project_21_bot'],
      'default_email_domain' => 'gsa.gov',
    }
  end

  let(:test_redshift_config) do
    {
      'aws_role_map' => {
        'dwadmin' => 'DWAdmin',
        'dwadminnonprod' => 'DWAdmin',
        'dwpoweruser' => 'DWPowerUser',
        'dwpowerusernonprod' => 'DWPowerUser',
        'dwuser' => 'DWUser',
        'dwusernonprod' => 'DWUser',
      },
      'role_priority' => {
        'DWAdmin' => 3,
        'DWPowerUser' => 2,
        'DWUser' => 1,
      },
      'enabled_aws_groups' => {
        'prod' => ['dwuser', 'dwpoweruser', 'dwadmin'],
        'sandbox' => ['dwuser', 'dwusernonprod', 'dwpoweruser', 'dwpowerusernonprod', 'dwadmin',
                      'dwadminnonprod'],
      },
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

  subject(:sync) { described_class.new }

  # Builds a QuickSight user struct similar to Aws::QuickSight::Types::User
  def qs_user(user_name:, email:, role: 'READER')
    Struct.new(:user_name, :email, :role, keyword_init: true).new(
      user_name: user_name, email: email, role: role,
    )
  end

  before do
    allow(sync).to receive(:quicksight_config).and_return(test_quicksight_config)
    allow(sync).to receive(:redshift_config).and_return(test_redshift_config)
    allow(sync).to receive(:users_yaml).and_return(test_users_yaml)
    allow(sync).to receive(:multi_account_allowlist).and_return({})
    allow(sync).to receive(:quicksight_client).and_return(quicksight_client)
    allow(sync).to receive(:sts_client).and_return(sts_client)
    allow(Identity::Hostdata).to receive(:env).and_return('int')
    allow(sts_client).to receive(:get_caller_identity).and_return(
      double(account: account_id),
    )
    allow(Rails.logger).to receive(:info)
    allow(Rails.logger).to receive(:warn)
    allow(Rails.logger).to receive(:error)
  end

  def stub_list_users(users)
    allow(quicksight_client).to receive(:list_users).and_return(
      [double(user_list: users)],
    )
  end

  describe 'environment detection' do
    it 'returns prod for production environments' do
      allow(Identity::Hostdata).to receive(:env).and_return('prod')
      expect(sync.send(:env_type)).to eq('prod')
    end

    it 'treats dm and staging as prod' do
      allow(Identity::Hostdata).to receive(:env).and_return('dm')
      expect(sync.send(:env_type)).to eq('prod')

      allow(Identity::Hostdata).to receive(:env).and_return('staging')
      expect(sync.send(:env_type)).to eq('prod')
    end

    it 'returns sandbox for non-production environments' do
      allow(Identity::Hostdata).to receive(:env).and_return('int')
      expect(sync.send(:env_type)).to eq('sandbox')
    end
  end

  describe 'QS username construction' do
    it 'strips only the @gsa.gov domain' do
      expect(sync.send(:build_qs_username, 'DWUser', 'john.doe@gsa.gov')).
        to eq('DWUser/john.doe')
    end

    it 'keeps a non-gsa.gov domain in the username' do
      expect(sync.send(:build_qs_username, 'DWUser', 'john.doe@example.com')).
        to eq('DWUser/john.doe@example.com')
    end
  end

  describe 'role normalization' do
    it 'maps aws groups to DW roles' do
      expect(sync.send(:normalize_aws_role, 'dwadmin')).to eq('DWAdmin')
      expect(sync.send(:normalize_aws_role, 'dwpoweruser')).to eq('DWPowerUser')
      expect(sync.send(:normalize_aws_role, 'dwuser')).to eq('DWUser')
    end

    it 'excludes nonprod groups in prod' do
      allow(Identity::Hostdata).to receive(:env).and_return('prod')
      expect(sync.send(:normalize_aws_role, 'dwadminnonprod')).to be_nil
    end

    it 'maps nonprod groups in sandbox' do
      expect(sync.send(:normalize_aws_role, 'dwadminnonprod')).to eq('DWAdmin')
    end
  end

  describe 'expected user mapping' do
    it 'skips non-human accounts and users without enabled groups' do
      mapping = sync.send(:expected_qs_username_to_email)

      expect(mapping).to include(
        'DWUser/john.doe' => 'john.doe@gsa.gov',
        'DWAdmin/jane.smith' => 'jane.smith@gsa.gov',
      )
      expect(mapping.keys).not_to include(
        a_string_matching(%r{bob.jones}),
        a_string_matching(%r{project_21_bot}),
        a_string_matching(%r{root}),
      )
    end

    it 'uses the explicit email when present' do
      allow(sync).to receive(:users_yaml).and_return(
        'john.doe' => { 'aws_groups' => ['dwuser'], 'email' => ['jdoe@example.com'] },
      )
      mapping = sync.send(:expected_qs_username_to_email)
      expect(mapping).to eq('DWUser/jdoe@example.com' => 'jdoe@example.com')
    end

    it 'creates an entry only for the highest-priority role for multi-role users' do
      allow(sync).to receive(:users_yaml).and_return(
        'multi.role' => { 'aws_groups' => ['dwuser', 'dwadmin'] },
      )
      mapping = sync.send(:expected_qs_username_to_email)
      expect(mapping).to eq('DWAdmin/multi.role' => 'multi.role@gsa.gov')
    end

    it 'keeps the first user and warns when two users share an email' do
      allow(sync).to receive(:users_yaml).and_return(
        'first.user' => { 'aws_groups' => ['dwuser'], 'email' => ['shared@gsa.gov'] },
        'second.user' => { 'aws_groups' => ['dwadmin'], 'email' => ['shared@gsa.gov'] },
      )
      expect(Rails.logger).to receive(:warn).with(/duplicate email shared@gsa.gov/)
      mapping = sync.send(:expected_qs_username_to_email)
      expect(mapping).to eq('DWUser/shared' => 'shared@gsa.gov')
    end

    it 'creates the configured extra accounts for an allowlisted user' do
      allow(sync).to receive(:multi_account_allowlist).and_return(
        'jane.smith' => ['FullAdministrator'],
      )
      mapping = sync.send(:expected_qs_username_to_email)
      expect(mapping).to include(
        'DWAdmin/jane.smith' => 'jane.smith@gsa.gov',
        'FullAdministrator/jane.smith' => 'jane.smith@gsa.gov',
      )
    end

    it 'ignores the allowlist for users not in users.yaml or not enabled' do
      allow(sync).to receive(:multi_account_allowlist).and_return(
        'bob.jones' => ['FullAdministrator'],
      )
      mapping = sync.send(:expected_qs_username_to_email)
      expect(mapping.keys).not_to include(a_string_matching(%r{bob.jones}))
    end
  end

  describe '#multi_account_allowlist' do
    it 'reads from IdentityConfig so usernames stay out of this repo' do
      allow(sync).to receive(:multi_account_allowlist).and_call_original
      allow(IdentityConfig.store).to receive(:quicksight_multi_account_allowlist).
        and_return('jane.smith' => ['FullAdministrator'])

      expect(sync.send(:multi_account_allowlist)).to eq(
        'jane.smith' => ['FullAdministrator'],
      )
    end

    it 'defaults to an empty hash when unset' do
      allow(sync).to receive(:multi_account_allowlist).and_call_original
      allow(IdentityConfig.store).to receive(:quicksight_multi_account_allowlist).
        and_return(nil)

      expect(sync.send(:multi_account_allowlist)).to eq({})
    end
  end

  describe '#sync' do
    context 'creating a new user' do
      before { stub_list_users([]) }

      it 'registers the user and assigns group membership' do
        expect(quicksight_client).to receive(:register_user).with(
          hash_including(
            identity_type: 'IAM',
            email: 'john.doe@gsa.gov',
            user_role: 'AUTHOR',
            iam_arn: "arn:aws:iam::#{account_id}:role/DWUser",
            session_name: 'john.doe',
            aws_account_id: account_id,
            namespace: 'default',
          ),
        )
        expect(quicksight_client).to receive(:create_group_membership).with(
          hash_including(
            member_name: 'DWUser/john.doe',
            group_name: 'QSUser',
            aws_account_id: account_id,
            namespace: 'default',
          ),
        )
        allow(quicksight_client).to receive(:register_user)
        allow(quicksight_client).to receive(:create_group_membership)

        allow(sync).to receive(:users_yaml).and_return(
          'john.doe' => { 'aws_groups' => ['dwuser'] },
        )

        sync.sync
      end
    end

    context 'removing a user' do
      before do
        allow(sync).to receive(:users_yaml).and_return({})
        stub_list_users(
          [
            qs_user(user_name: 'DWUser/old.user', email: 'old.user@gsa.gov'),
          ],
        )
      end

      it 'deletes removed users' do
        expect(quicksight_client).to receive(:delete_user).with(
          hash_including(
            user_name: 'DWUser/old.user',
            aws_account_id: account_id,
            namespace: 'default',
          ),
        )
        sync.sync
      end
    end

    context 'when QuickSight users span multiple pages' do
      before do
        allow(sync).to receive(:users_yaml).and_return({})
        allow(quicksight_client).to receive(:list_users).with(
          aws_account_id: account_id,
          namespace: 'default',
        ).and_return(
          [
            double(
              user_list: [qs_user(user_name: 'DWUser/first.page', email: 'first.page@gsa.gov')],
            ),
            double(
              user_list: [qs_user(user_name: 'DWUser/second.page', email: 'second.page@gsa.gov')],
            ),
          ],
        )
      end

      it 'syncs users from every page' do
        expect(quicksight_client).to receive(:delete_user).with(
          hash_including(user_name: 'DWUser/first.page'),
        )
        expect(quicksight_client).to receive(:delete_user).with(
          hash_including(user_name: 'DWUser/second.page'),
        )

        sync.sync
      end
    end

    context 'protected accounts' do
      before do
        allow(sync).to receive(:users_yaml).and_return({})
        stub_list_users(
          [
            qs_user(
              user_name: 'DWAdmin/identity-devops',
              email: 'identity-devops@login.gov',
            ),
            qs_user(
              user_name: 'FullAdministrator/someone',
              email: 'someone@gsa.gov',
            ),
          ],
        )
      end

      it 'does not delete protected or FullAdministrator accounts' do
        expect(quicksight_client).not_to receive(:delete_user)
        sync.sync
      end
    end

    context 'role upgrade with no existing account' do
      before do
        allow(sync).to receive(:users_yaml).and_return(
          'multi.role' => { 'aws_groups' => ['dwuser', 'dwadmin'] },
        )
        stub_list_users([])
      end

      it 'creates only the highest-priority account' do
        expect(quicksight_client).to receive(:register_user).once.with(
          hash_including(email: 'multi.role@gsa.gov', user_role: 'ADMIN'),
        )
        allow(quicksight_client).to receive(:create_group_membership)
        sync.sync
      end
    end

    context 'allowlisted multi-account user' do
      before do
        allow(sync).to receive(:multi_account_allowlist).and_return(
          'jane.smith' => ['FullAdministrator'],
        )
        allow(sync).to receive(:users_yaml).and_return(
          'jane.smith' => { 'aws_groups' => ['dwadmin'] },
        )
        stub_list_users([])
        allow(quicksight_client).to receive(:create_group_membership)
      end

      it 'creates both the highest-priority and the allowlisted account' do
        expect(quicksight_client).to receive(:register_user).with(
          hash_including(iam_arn: "arn:aws:iam::#{account_id}:role/DWAdmin"),
        )
        expect(quicksight_client).to receive(:register_user).with(
          hash_including(iam_arn: "arn:aws:iam::#{account_id}:role/FullAdministrator"),
        )

        sync.sync
      end
    end

    context 'when a higher-priority account already exists' do
      before do
        allow(sync).to receive(:users_yaml).and_return(
          'multi.role' => { 'aws_groups' => ['dwuser', 'dwadmin'] },
        )
        stub_list_users(
          [
            qs_user(user_name: 'DWAdmin/multi.role', email: 'multi.role@gsa.gov'),
          ],
        )
      end

      it 'does not create the lower-priority account' do
        expect(quicksight_client).not_to receive(:register_user)
        sync.sync
      end
    end

    context 'when a lower-priority account already exists' do
      before do
        allow(sync).to receive(:users_yaml).and_return(
          'multi.role' => { 'aws_groups' => ['dwuser', 'dwadmin'] },
        )
        stub_list_users(
          [
            qs_user(user_name: 'DWUser/multi.role', email: 'multi.role@gsa.gov'),
          ],
        )
        allow(quicksight_client).to receive(:register_user)
        allow(quicksight_client).to receive(:create_group_membership)
      end

      it 'replaces it with the highest-priority account' do
        expect(quicksight_client).to receive(:delete_user).with(
          hash_including(user_name: 'DWUser/multi.role'),
        )
        expect(quicksight_client).to receive(:register_user).with(
          hash_including(email: 'multi.role@gsa.gov', user_role: 'ADMIN'),
        )

        sync.sync
      end
    end

    context 'when a user is demoted to a lower-priority role' do
      before do
        allow(sync).to receive(:users_yaml).and_return(
          'demoted.user' => { 'aws_groups' => ['dwuser'] },
        )
        stub_list_users(
          [
            qs_user(user_name: 'DWAdmin/demoted.user', email: 'demoted.user@gsa.gov'),
          ],
        )
        allow(quicksight_client).to receive(:register_user)
        allow(quicksight_client).to receive(:create_group_membership)
      end

      it 'deletes the old higher-priority account and creates the new lower-priority one' do
        expect(quicksight_client).to receive(:delete_user).with(
          hash_including(user_name: 'DWAdmin/demoted.user'),
        )
        expect(quicksight_client).to receive(:register_user).with(
          hash_including(email: 'demoted.user@gsa.gov', user_role: 'AUTHOR'),
        )

        sync.sync
      end
    end

    context 'nonprod groups in prod' do
      before do
        allow(Identity::Hostdata).to receive(:env).and_return('prod')
        allow(sync).to receive(:users_yaml).and_return(
          'np.user' => { 'aws_groups' => ['dwusernonprod'] },
        )
        stub_list_users([])
      end

      it 'does not create users from nonprod-only groups' do
        expect(quicksight_client).not_to receive(:register_user)
        sync.sync
      end
    end

    context 'flagging PRO roles' do
      before do
        allow(sync).to receive(:users_yaml).and_return({})
        stub_list_users(
          [
            qs_user(
              user_name: 'FullAdministrator/admin', email: 'admin@gsa.gov',
              role: 'ADMIN_PRO'
            ),
          ],
        )
      end

      it 'logs a structured warning with pro_users_detected' do
        expect(Rails.logger).to receive(:warn).with(
          {
            name: 'QuicksightSyncJob',
            pro_users_detected: 'FullAdministrator/admin',
          }.to_json,
        )
        sync.sync
      end
    end

    context 'with no PRO roles' do
      before do
        allow(sync).to receive(:users_yaml).and_return({})
        stub_list_users(
          [
            qs_user(
              user_name: 'FullAdministrator/admin', email: 'admin@gsa.gov',
              role: 'ADMIN'
            ),
          ],
        )
      end

      it 'does not log a PRO roles warning' do
        expect(Rails.logger).not_to receive(:warn)
        sync.sync
      end
    end

    context 'per-user error aggregation' do
      before do
        allow(sync).to receive(:users_yaml).and_return(
          'john.doe' => { 'aws_groups' => ['dwuser'] },
        )
        stub_list_users([])
        allow(quicksight_client).to receive(:create_group_membership)
        allow(quicksight_client).to receive(:register_user).and_raise(
          Aws::QuickSight::Errors::ServiceError.new(nil, 'boom'),
        )
      end

      it 'raises at the end and logs the failure' do
        expect(Rails.logger).to receive(:error).with(/failed to create user/)
        expect { sync.sync }.to raise_error(/QuickSight sync failed for 1 user/)
      end
    end
  end
end
