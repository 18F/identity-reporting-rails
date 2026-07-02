# frozen_string_literal: true

# Shared environment and config-file accessors for data warehouse user sync
# services (RedshiftSync, QuicksightSync). Reads from redshift_config.yaml and
# the identity-devops users.yaml.
module UserSyncConfig
  extend ActiveSupport::Concern

  private

  def env_name
    @env_name ||= Identity::Hostdata.env
  end

  def env_type
    return 'prod' if ['prod', 'dm', 'staging'].include?(env_name)

    'sandbox'
  end

  def redshift_config
    @redshift_config ||= YAML.safe_load(File.read(redshift_config_path))
  end

  def redshift_config_path
    Rails.root.join('config/redshift_config.yaml')
  end

  def users_yaml
    @users_yaml ||= YAML.safe_load(File.read(users_yaml_path))['users']
  end

  def users_yaml_path
    @users_yaml_path ||= IdentityConfig.identity_devops_users_yaml_path
  end

  def enabled_aws_groups
    redshift_config['enabled_aws_groups'][env_type]
  end
end
