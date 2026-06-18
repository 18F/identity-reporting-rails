# frozen_string_literal: true

require 'yaml'
require 'aws-sdk-secretsmanager'
require 'digest'
require 'json'
require 'securerandom'

# Rotates the Redshift login passwords for system users defined in
# config/redshift_config.yaml. For each target user it:
class RedshiftPasswordRotator
  PASSWORD_LENGTH = 32
  PASSWORD_PUNCTUATION = '!#$%&*+-=?@^_'

  def rotate(usernames: nil)
    targets = rotation_targets(usernames)

    if targets.empty?
      Rails.logger.warn('No matching system users with a secret_id to rotate')
      return
    end

    failures = []
    targets.each do |system_user|
      rotate_user_password(system_user['user_name'], system_user['secret_id'])
    rescue StandardError => e
      failures << system_user['user_name']
      Rails.logger.error("Failed to rotate password for #{system_user['user_name']}: #{e.message}")
    end

    if failures.any?
      raise "Redshift password rotation failed for: #{failures.join(', ')}"
    end

    Rails.logger.info('Redshift password rotation completed successfully')
  end

  private

  def rotation_targets(usernames)
    candidates = system_users.reject { |u| u['secret_id'].nil? }

    return candidates if usernames.nil? || usernames.empty?

    requested = Array(usernames)
    selected = candidates.select { |u| requested.include?(u['user_name']) }

    missing = requested - selected.map { |u| u['user_name'] }
    raise "Unknown rotation target(s): #{missing.join(', ')}" if missing.any?

    selected
  end

  def rotate_user_password(user_name, secret_id)
    Rails.logger.info("Rotating password for system user #{user_name}")

    unless user_exists?(user_name)
      Rails.logger.warn("Skipping #{user_name}: user does not exist in Redshift")
      return
    end

    existing_secret = fetch_secret(secret_id)

    new_password = generate_password

    execute_query(
      "ALTER USER #{user_name} PASSWORD #{md5_password(new_password, user_name)};",
    )

    store_password_secret(secret_id, existing_secret.merge('password' => new_password))

    Rails.logger.info("Successfully rotated password for #{user_name}")
  end

  def fetch_secret(secret_id)
    raw = secrets_manager_client.get_secret_value(secret_id: secret_id).secret_string
    raw ? JSON.parse(raw) : {}
  end

  def store_password_secret(secret_id, payload)
    secrets_manager_client.put_secret_value(
      secret_id: secret_id,
      secret_string: payload.to_json,
    )
  end

  def generate_password
    charset = [*'a'..'z', *'A'..'Z', *'0'..'9', *PASSWORD_PUNCTUATION.chars]
    SecureRandom.alphanumeric(PASSWORD_LENGTH, chars: charset)
  end

  def md5_password(password, user_name)
    "'md5#{Digest::MD5.hexdigest(password + user_name)}'"
  end

  def user_exists?(user_name)
    result = execute_query("SELECT usename FROM pg_user WHERE usename = '#{user_name}'")
    result.any?
  end

  def system_users
    redshift_config['system_users'].map { |user| interpolate_config_hash(user) }
  end

  def redshift_config
    @redshift_config ||= YAML.safe_load(File.read(Rails.root.join('config/redshift_config.yaml')))
  end

  def interpolate_config_hash(hash)
    case hash
    when Hash
      hash.transform_values { |v| interpolate_config_hash(v) }
    when Array
      hash.map { |v| interpolate_config_hash(v) }
    when String
      hash.gsub('%{env_name}', env_name)
    else
      hash
    end
  end

  def env_name
    @env_name ||= Identity::Hostdata.env
  end

  def execute_query(sql)
    connection.execute(sql)
  end

  def connection
    @connection ||= DataWarehouseApplicationRecord.connection
  end

  def secrets_manager_client
    @secrets_manager_client ||= Aws::SecretsManager::Client.new(
      region: Identity::Hostdata.config.aws_region,
    )
  end
end
