require 'rails_helper'

RSpec.describe RedshiftPasswordRotator do
  let(:mock_connection) { instance_double(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter) }
  let(:secrets_manager_client) { instance_double(Aws::SecretsManager::Client) }

  let(:test_redshift_config) do
    {
      'system_users' => [
        {
          'user_name' => 'security_audit',
          'secret_id' => 'redshift/%{env_name}-analytics-security-audit',
        },
        {
          'user_name' => 'rails_worker',
          'secret_id' => 'redshift/%{env_name}-analytics-rails-worker',
        },
        {
          # No secret_id -> not eligible for rotation
          'user_name' => 'passwordless_user',
          'secret_id' => nil,
        },
      ],
    }
  end

  subject(:rotator) { described_class.new }

  before do
    allow(rotator).to receive(:redshift_config).and_return(test_redshift_config)
    allow(rotator).to receive(:connection).and_return(mock_connection)
    allow(rotator).to receive(:secrets_manager_client).and_return(secrets_manager_client)
    allow(Identity::Hostdata).to receive(:env).and_return('testenv')
    allow(mock_connection).to receive(:execute).and_return(double(any?: false, to_a: []))
    allow(Rails.logger).to receive(:info)
    allow(Rails.logger).to receive(:warn)
    allow(Rails.logger).to receive(:error)
  end

  describe '#generate_password' do
    it 'generates a password of the configured length' do
      expect(rotator.send(:generate_password).length).to eq(described_class::PASSWORD_LENGTH)
    end

    it 'only uses characters from the allowed set' do
      allowed = /\A[a-zA-Z0-9#{Regexp.escape(described_class::PASSWORD_PUNCTUATION)}]+\z/o
      expect(rotator.send(:generate_password)).to match(allowed)
    end

    it 'generates a different password on each call' do
      expect(rotator.send(:generate_password)).not_to eq(rotator.send(:generate_password))
    end
  end

  describe '#md5_password' do
    it 'returns the md5-salted form expected by Redshift' do
      expected = "'md5#{Digest::MD5.hexdigest('secret' + 'rails_worker')}'"
      expect(rotator.send(:md5_password, 'secret', 'rails_worker')).to eq(expected)
    end
  end

  describe '#rotation_targets' do
    it 'returns all secret-backed system users when no usernames given' do
      targets = rotator.send(:rotation_targets, nil).map { |u| u['user_name'] }
      expect(targets).to contain_exactly('security_audit', 'rails_worker')
    end

    it 'excludes system users without a secret_id' do
      targets = rotator.send(:rotation_targets, nil).map { |u| u['user_name'] }
      expect(targets).not_to include('passwordless_user')
    end

    it 'filters to the requested usernames' do
      targets = rotator.send(:rotation_targets, ['rails_worker']).map { |u| u['user_name'] }
      expect(targets).to eq(['rails_worker'])
    end

    it 'raises when a requested username is not a known rotatable system user' do
      expect do
        rotator.send(:rotation_targets, ['nonexistent'])
      end.to raise_error(/Unknown rotation target.*nonexistent/)
    end
  end

  describe '#fetch_secret' do
    let(:secret_id) { 'redshift/testenv-analytics-rails-worker' }

    it 'parses and returns the existing secret payload' do
      allow(secrets_manager_client).to receive(:get_secret_value).
        with(secret_id: secret_id).
        and_return(double(secret_string: { 'host' => 'db.example', 'password' => 'old' }.to_json))

      expect(rotator.send(:fetch_secret, secret_id)).to eq(
        'host' => 'db.example', 'password' => 'old',
      )
    end

    it 'returns an empty hash when the secret has no string value' do
      allow(secrets_manager_client).to receive(:get_secret_value).
        with(secret_id: secret_id).
        and_return(double(secret_string: nil))

      expect(rotator.send(:fetch_secret, secret_id)).to eq({})
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

      rotator.send(
        :store_password_secret, secret_id,
        { 'host' => 'db.example', 'password' => 'new-password' }
      )
    end
  end

  describe '#rotate_user_password' do
    let(:secret_id) { 'redshift/testenv-analytics-rails-worker' }

    before do
      allow(rotator).to receive(:generate_password).and_return('generated-pw')
      allow(rotator).to receive(:store_password_secret)
      allow(rotator).to receive(:fetch_secret).with(secret_id).and_return('host' => 'db.example')
    end

    it 'reads the secret, runs ALTER USER, then stores the merged payload' do
      allow(rotator).to receive(:user_exists?).with('rails_worker').and_return(true)
      expected_hash = "'md5#{Digest::MD5.hexdigest('generated-pw' + 'rails_worker')}'"

      expect(mock_connection).to receive(:execute).
        with("ALTER USER rails_worker PASSWORD #{expected_hash};")
      expect(rotator).to receive(:store_password_secret).
        with(secret_id, { 'host' => 'db.example', 'password' => 'generated-pw' })

      rotator.send(:rotate_user_password, 'rails_worker', secret_id)
    end

    it 'does not alter Redshift when the secret cannot be read' do
      allow(rotator).to receive(:user_exists?).with('rails_worker').and_return(true)
      allow(rotator).to receive(:fetch_secret).with(secret_id).and_raise(
        Aws::SecretsManager::Errors::ResourceNotFoundException.new(nil, 'missing'),
      )

      expect(mock_connection).not_to receive(:execute).with(/ALTER USER/)
      expect(rotator).not_to receive(:store_password_secret)

      expect do
        rotator.send(:rotate_user_password, 'rails_worker', secret_id)
      end.to raise_error(Aws::SecretsManager::Errors::ResourceNotFoundException)
    end

    it 'skips users that do not exist in Redshift' do
      allow(rotator).to receive(:user_exists?).with('rails_worker').and_return(false)

      expect(mock_connection).not_to receive(:execute).with(/ALTER USER/)
      expect(rotator).not_to receive(:store_password_secret)
      expect(Rails.logger).to receive(:warn).with(/does not exist in Redshift/)

      rotator.send(:rotate_user_password, 'rails_worker', secret_id)
    end
  end

  describe '#rotate' do
    it 'rotates all secret-backed users when no usernames given' do
      expect(rotator).to receive(:rotate_user_password).
        with('security_audit', 'redshift/testenv-analytics-security-audit')
      expect(rotator).to receive(:rotate_user_password).
        with('rails_worker', 'redshift/testenv-analytics-rails-worker')

      rotator.rotate
    end

    it 'rotates only the requested user' do
      allow(rotator).to receive(:rotate_user_password)

      expect(rotator).to receive(:rotate_user_password).
        with('rails_worker', 'redshift/testenv-analytics-rails-worker')

      rotator.rotate(usernames: ['rails_worker'])
    end

    it 'warns and does nothing when there are no matching users' do
      allow(rotator).to receive(:redshift_config).and_return('system_users' => [])

      expect(rotator).not_to receive(:rotate_user_password)
      expect(Rails.logger).to receive(:warn).with(/No matching system users/)

      rotator.rotate
    end

    it 'continues rotating other users when one fails, then raises a summary' do
      allow(rotator).to receive(:rotate_user_password).
        with('security_audit', anything).
        and_raise(StandardError, 'boom')
      allow(rotator).to receive(:rotate_user_password).
        with('rails_worker', anything)

      # rails_worker is still attempted even though security_audit blew up
      expect(rotator).to receive(:rotate_user_password).with('rails_worker', anything)
      expect(Rails.logger).to receive(:error).with(/security_audit.*boom/)

      expect { rotator.rotate }.to raise_error(/failed for: security_audit/)
    end
  end
end
