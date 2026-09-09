# RedshiftUnexpectedUserDetectionJob
#
# Checks if there are local users created in Redshift that are not defined in the users.yml file
# of the identity-devops repository.

require 'yaml'

class RedshiftUnexpectedUserDetectionJob < ApplicationJob
  queue_as :default

  # System users that should be excluded from unexpected user detection
  # These are defined in config/redshift_config.yaml under system_users
  STATIC_EXCLUDED_USERS = [
    'rdsdb',
    'rdsadmin',
    'superuser',
    'postgres',
    'security_audit',
    'quicksight_connector',
    'marts',
    'qa_marts',
    'rails_worker',
    'fraudops_marts',
    'fraudops_qa_marts',
    'pii_reader',
  ].freeze

  def perform(user_config_path = nil)
    @user_config_path = set_user_config_path(user_config_path)
    log_unexpected_local_users
  end

  private

  def set_user_config_path(path)
    if !path.nil?
      path
    else
      IdentityConfig.identity_devops_users_yaml_path
    end
  end

  def using_redshift_adapter?
    DataWarehouseApplicationRecord.connection.adapter_name.downcase.include?('redshift')
  end

  def lambda_users
    env_name = Identity::Hostdata.env
    ["IAMR:#{env_name}_db_consumption", "IAMR:#{env_name}_stale_data_check",
     "IAMR:#{env_name}_log_consumption"]
  end

  def local_users_query
    excluded_list = STATIC_EXCLUDED_USERS.map { |user| "'#{user}'" }.join(', ')
    <<~SQL
      SELECT usename
      FROM pg_user
      WHERE usename NOT IN (#{excluded_list})
    SQL
  end

  def local_users_from_redshift
    result = DataWarehouseApplicationRecord.connection.execute(local_users_query)
    users = result.map(&:values).flatten
    unless using_redshift_adapter?
      # Exclude the local cluster's connection user (varies by environment:
      # devenv, CI, Homebrew); it is never a real Redshift user.
      users.delete(
        DataWarehouseApplicationRecord.connection.select_value('SELECT current_user'),
      )
    end
    lambda_users.each { |lambda_user_name| users.delete(lambda_user_name) }
    users
  end

  def local_users_from_yml
    yml_config = YAML.load_file(@user_config_path)
    yml_users = yml_config['users'].keys
    yml_users.map { |user_name| 'IAM:' + user_name }
  end

  def log_unexpected_local_users
    unexpected_redshift_users = local_users_from_redshift - local_users_from_yml
    unless unexpected_redshift_users.empty?
      logger.info(
        {
          name: 'RedshiftUnexpectedUserDetectionJob',
          unexpected_users_detected: unexpected_redshift_users.join(', '),
        }.to_json,
      )
    end
  rescue Errno::ENOENT => e
    log_error(e.message)
  end

  def log_error(message)
    logger.error(
      {
        name: 'RedshiftUnexpectedUserDetectionJob',
        error: message,
      }.to_json,
    )
  end

  def logger
    @logger ||= IdentityJobLogSubscriber.new.logger
  end
end
