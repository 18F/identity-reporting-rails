#!/usr/bin/env ruby
# frozen_string_literal: true

require 'yaml'
require 'aws-sdk-quicksight'
require 'aws-sdk-core'

require_relative '../../config/environment'

class QuicksightSync
  def sync
    Rails.logger.info('Starting QuickSight user sync')

    quicksight_users = list_quicksight_users
    expected_users = expected_qs_username_to_email

    flag_users_with_pro_roles(quicksight_users)

    errors = []
    errors.concat(drop_users(expected_users, quicksight_users))
    errors.concat(create_users(expected_users, quicksight_users))

    if errors.any?
      messages = errors.map { |e| "#{e[:user]}: #{e[:error].message}" }
      raise "QuickSight sync failed for #{errors.size} user(s): #{messages.join('; ')}"
    end

    Rails.logger.info('QuickSight user sync completed successfully')
  end

  private

  def env_name
    @env_name ||= Identity::Hostdata.env
  end

  def env_type
    return 'prod' if ['prod', 'dm', 'staging'].include?(env_name)

    'sandbox'
  end

  def quicksight_config
    @quicksight_config ||= YAML.safe_load(File.read(quicksight_config_path))
  end

  def quicksight_config_path
    Rails.root.join('config/quicksight_config.yaml')
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

  def non_human_accounts
    quicksight_config['non_human_accounts']
  end

  def protected_accounts
    quicksight_config['protected_accounts']
  end

  def default_email_domain
    quicksight_config['default_email_domain']
  end

  def quicksight_client
    @quicksight_client ||= Aws::QuickSight::Client.new(
      region: Identity::Hostdata.config.aws_region,
    )
  end

  def sts_client
    @sts_client ||= Aws::STS::Client.new(region: Identity::Hostdata.config.aws_region)
  end

  def aws_account_id
    @aws_account_id ||= sts_client.get_caller_identity.account
  end

  def list_quicksight_users
    users = []
    next_token = nil

    loop do
      params = {
        aws_account_id: aws_account_id,
        namespace: 'default',
      }
      params[:next_token] = next_token if next_token.present?

      response = quicksight_client.list_users(**params)
      users.concat(response.user_list)
      next_token = response.next_token
      break if next_token.blank?
    end

    users
  end

  def strip_email_domain(email)
    email.sub("@#{default_email_domain}", '')
  end

  def build_qs_username(aws_role, email)
    "#{aws_role}/#{strip_email_domain(email)}"
  end

  def normalize_aws_role(aws_group)
    return nil if env_type == 'prod' && aws_group.end_with?('nonprod')

    redshift_config['aws_role_map'][aws_group]
  end

  def role_priority(aws_role)
    redshift_config['role_priority'].fetch(aws_role, 0)
  end

  def quicksight_aws_role(aws_role)
    quicksight_config['quicksight_aws_role'].fetch(
      aws_role,
      quicksight_config['quicksight_aws_role']['default'],
    )
  end

  def quicksight_group(aws_role)
    quicksight_config['quicksight_group'].fetch(
      aws_role,
      quicksight_config['quicksight_group']['default'],
    )
  end

  # email -> users.yaml username, filtered to enabled groups and human accounts.
  # The first user encountered wins a given email; later collisions are skipped
  # and logged, so two users.yaml entries can never claim the same email.
  def filtered_yaml_email_mapping
    email_to_username = {}

    users_yaml.each do |username, user_data|
      next if non_human_accounts.include?(username)

      aws_groups = user_data['aws_groups']
      next if aws_groups.nil?
      next unless aws_groups.intersect?(enabled_aws_groups)

      email = user_data.fetch('email', ["#{username}@#{default_email_domain}"]).first

      if email_to_username.key?(email)
        Rails.logger.warn(
          "QS: skipping user #{username} with duplicate email #{email} " \
          "(already claimed by #{email_to_username[email]})",
        )
        next
      end

      email_to_username[email] = username
    end

    email_to_username
  end

  def multi_account_allowlist
    quicksight_config.fetch('multi_account_allowlist', {})
  end

  # qs_username -> email for each user's highest-priority valid role, plus any
  # extra roles configured for allowlisted users.
  def expected_qs_username_to_email
    mapping = {}

    filtered_yaml_email_mapping.each do |email, yaml_username|
      aws_groups = users_yaml[yaml_username]['aws_groups'] || []
      normalized_roles = aws_groups.filter_map { |aws_group| normalize_aws_role(aws_group) }
      highest_role = normalized_roles.max_by { |role| role_priority(role) }
      next unless highest_role

      mapping[build_qs_username(highest_role, email)] = email

      multi_account_allowlist.fetch(yaml_username, []).each do |extra_role|
        mapping[build_qs_username(extra_role, email)] = email
      end
    end

    mapping
  end

  def create_quicksight_user(email, aws_role)
    qs_username = build_qs_username(aws_role, email)
    qs_role = quicksight_aws_role(aws_role)
    Rails.logger.info("QS: Creating user #{qs_username} with role #{qs_role}")

    quicksight_client.register_user(
      identity_type: 'IAM',
      email: email,
      user_role: qs_role,
      iam_arn: "arn:aws:iam::#{aws_account_id}:role/#{aws_role}",
      session_name: strip_email_domain(email),
      aws_account_id: aws_account_id,
      namespace: 'default',
    )

    assign_group_membership(email, aws_role)
  end

  def assign_group_membership(email, aws_role)
    qs_username = build_qs_username(aws_role, email)
    qs_group = quicksight_group(aws_role)
    Rails.logger.info("QS: Assigning user #{qs_username} to the #{qs_group} group")

    quicksight_client.create_group_membership(
      member_name: qs_username,
      group_name: qs_group,
      aws_account_id: aws_account_id,
      namespace: 'default',
    )
  end

  def delete_quicksight_user(username)
    Rails.logger.info("QS: Deleting user #{username}")

    quicksight_client.delete_user(
      user_name: username,
      aws_account_id: aws_account_id,
      namespace: 'default',
    )
  end

  # One account per user, highest-priority role, upgrade only if no higher-priority account exists.
  # Accounts whose role is explicitly allowlisted for a user are exempt from
  # the one-account-per-user collapse and are always created when missing.
  def create_users(expected_users, quicksight_users)
    Rails.logger.info('QS: creating new users')
    errors = []

    expected = expected_users.keys.to_set
    existing = quicksight_users.map(&:user_name).to_set

    users_to_create = expected - existing
    return errors if users_to_create.empty?

    allowlisted, standard = users_to_create.partition do |qs_username|
      allowlisted_account?(qs_username)
    end

    allowlisted.each do |qs_username|
      aws_role = qs_username.split('/')[0]
      errors << safe_create(expected_users[qs_username], aws_role, qs_username)
    end

    new_accounts_by_user = Hash.new { |h, k| h[k] = [] }
    standard.each do |qs_username|
      username_part = qs_username.split('/')[1]
      new_accounts_by_user[username_part] << qs_username
    end

    # drop_users has already deleted any existing account not in expected, so
    # ignore those here to avoid treating a just-dropped account as still present.
    # Allowlisted accounts are also ignored so they don't suppress a user's
    # highest-priority account via the higher_exists check.
    existing_by_user = Hash.new { |h, k| h[k] = [] }
    quicksight_users.each do |user|
      next unless expected.include?(user.user_name)
      next if allowlisted_account?(user.user_name)

      username_part = user.user_name.split('/')[1]
      existing_by_user[username_part] << user.user_name
    end

    new_accounts_by_user.each do |username_part, accounts|
      existing_accounts = existing_by_user[username_part]

      if existing_accounts.empty?
        highest = accounts.max_by { |acc| role_priority(acc.split('/')[0]) }
        email = expected_users[highest]
        aws_role = highest.split('/')[0]
        errors << safe_create(email, aws_role, highest)
      else
        accounts.each do |qs_username|
          aws_role = qs_username.split('/')[0]
          priority = role_priority(aws_role)

          higher_exists = existing_accounts.any? do |existing_account|
            role_priority(existing_account.split('/')[0]) > priority
          end

          next if higher_exists

          email = expected_users[qs_username]
          errors << safe_create(email, aws_role, qs_username)
        end
      end
    end

    errors.compact
  end

  # Whether a qs_username's role is allowlisted for that user (username part).
  def allowlisted_account?(qs_username)
    aws_role, username_part = qs_username.split('/')
    multi_account_allowlist.fetch(username_part, []).include?(aws_role)
  end

  def safe_create(email, aws_role, qs_username)
    create_quicksight_user(email, aws_role)
    nil
  rescue Aws::QuickSight::Errors::ServiceError => e
    Rails.logger.error("QS: failed to create user #{qs_username}: #{e.message}")
    { user: qs_username, error: e }
  end

  def drop_users(expected_users, quicksight_users)
    Rails.logger.info('QS: dropping removed users')
    errors = []

    expected = expected_users.keys.to_set
    existing = quicksight_users.reject do |user|
      protected_accounts.include?(user.email) ||
        user.user_name.start_with?('FullAdministrator/')
    end.map(&:user_name).to_set

    (existing - expected).each do |qs_username|
      delete_quicksight_user(qs_username)
    rescue Aws::QuickSight::Errors::ServiceError => e
      Rails.logger.error("QS: failed to delete user #{qs_username}: #{e.message}")
      errors << { user: qs_username, error: e }
    end

    errors
  end

  def flag_users_with_pro_roles(quicksight_users)
    pro_users = quicksight_users.select { |user| user.role.to_s.end_with?('_PRO') }
    if pro_users.any?
      Rails.logger.warn(
        "Quicksight users with PRO roles found: #{pro_users.map(&:user_name)}",
      )
    end
  end
end

if $PROGRAM_NAME == __FILE__
  QuicksightSync.new.sync
end
