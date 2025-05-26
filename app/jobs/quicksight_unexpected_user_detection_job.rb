# QuicksightUnexpectedUserDetectionJob
#
# This job is responsible for detecting unexpected users in Amazon QuickSight.
# # It compares the users in QuickSight with those defined in the users.yaml configuration file.
#

require 'yaml'
require 'aws-sdk-quicksight'

class QuicksightUnexpectedUserDetectionJob < ApplicationJob
  queue_as :default

  def perform(user_config_path = nil)
    @user_config_path = set_user_config_path(user_config_path)
    log_unexpected_quicksight_users
  end

  private

  def set_user_config_path(path)
    if !path.nil?
      path
    else
      user_yml_relative_path = 'terraform/master/global/users.yaml'
      user_sync_devops_yaml = IdentityConfig.local_devops_path(
        :user_sync_identity_devops, user_yml_relative_path
      )
      devops_yaml = IdentityConfig.local_devops_path(
        :identity_devops, user_yml_relative_path
      )
      if File.exist?(user_sync_devops_yaml)
        user_sync_devops_yaml
      else
        devops_yaml
      end
    end
  end

  def log_unexpected_quicksight_users
    # Logic to detect unexpected users in QuickSight and log them
    # This would typically involve fetching users from QuickSight and comparing with the YAML file.
  end
end
