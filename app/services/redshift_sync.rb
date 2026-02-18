#!/usr/bin/env ruby
# frozen_string_literal: true

require 'yaml'
require 'aws-sdk-secretsmanager'
require 'optparse'
require 'digest'
require 'json'

require_relative '../../config/environment'
class RedshiftSync
  def sync
    Rails.logger.info('Starting Redshift user sync')

    lambda_users.each do |lambda_user|
      create_lambda_user(lambda_user['user_name'], lambda_user['schemas'])
    end

    system_users.each do |system_user|
      if feature_enabled?(system_user['feature_flag'])
        create_system_user(
          system_user['user_name'],
          system_user['schemas'],
          system_user['secret_id'],
          system_user['syslog_access'],
        )
      end
    end

    user_groups.each do |user_group|
      create_user_group(user_group)
    end

    drop_users
    create_users

    user_groups.each do |user_group|
      sync_user_group(user_group)
    end

    Rails.logger.info('Redshift user sync completed successfully')
  end

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

  def config_file
    @config_file ||= begin
      terraform_config_path = IdentityConfig.local_devops_path(
        :identity_devops,
        "terraform/data-warehouse/#{env_name}/main.tf",
      )
      File.read(terraform_config_path)
    end
  end

  def users_yaml_path
    @users_yaml_path ||= IdentityConfig.local_devops_path(
      :identity_devops,
      'terraform/master/global/users.yaml',
    )
  end

  def interpolate_env_name(str)
    return str unless str.is_a?(String)
    str.gsub('%{env_name}', env_name)
  end

  def interpolate_config_hash(hash)
    case hash
    when Hash
      hash.transform_values { |v| interpolate_config_hash(v) }
    when Array
      hash.map { |v| interpolate_config_hash(v) }
    when String
      interpolate_env_name(hash)
    else
      hash
    end
  end

  def enabled_aws_groups
    redshift_config['enabled_aws_groups'][env_type]
  end

  def feature_enabled?(feature_flag)
    return true if feature_flag.nil?

    flags_to_check = feature_flag.is_a?(Array) ? feature_flag : [feature_flag]

    flags_to_check.any? do |flag|
      config_file.match?(/^\s*(?!#|\/\/)#{flag}\s+=\s+true/m)
    end
  end

  def connection
    @connection ||= DataWarehouseApplicationRecord.connection
  end

  def secrets_manager_client
    @secrets_manager_client ||= Aws::SecretsManager::Client.new(
      region: Identity::Hostdata.config.aws_region,
    )
  end

  def redshift_secret(user_name, secret_id)
    secret_value = secrets_manager_client.get_secret_value(secret_id: secret_id)
    password = JSON.parse(secret_value['secret_string'])['password']
    "'md5#{Digest::MD5.hexdigest(password + user_name)}'"
  end

  def user_groups
    redshift_config['user_groups'].map { |group| interpolate_config_hash(group) }
  end

  def lambda_users
    redshift_config['lambda_users'].map { |user| interpolate_config_hash(user) }
  end

  def system_users
    redshift_config['system_users'].map { |user| interpolate_config_hash(user) }
  end

  def canonical_users
    @canonical_users ||= begin
      redshift_users = users_yaml.filter_map do |username, user_data|
        username if user_data['aws_groups']&.intersect?(enabled_aws_groups)
      end

      non_human_accounts = ['project_21_bot', 'root']
      (redshift_users - non_human_accounts).map { |name| "IAM:#{name}" }
    end
  end

  def execute_query(sql)
    connection.execute(sql)
  end

  def quote(val)
    if val.is_a?(Array)
      "(#{val.map { |v| quote(v) }.join(', ')})"
    else
      "'#{val}'"
    end
  end

  def disallowed_characters?(username)
    username.match?(/[^A-Za-z0-9.\-:_]/)
  end

  def current_users
    excluded_users = [
      'superuser',
      'rdsdb',
      *lambda_users.map { |lambda_user| lambda_user['user_name'] },
      *system_users.map { |system_user| system_user['user_name'] },
    ]

    result = execute_query(
      "SELECT usename FROM pg_user WHERE usename NOT IN #{quote(excluded_users)}",
    )

    result.to_a.map { |row| row['usename'] }
  end

  def users_to_create(yaml, redshift)
    yaml - redshift
  end

  def users_to_drop(yaml, redshift)
    redshift - yaml
  end

  def get_all_configured_schemas
    all_schemas = []

    user_groups.each do |group|
      group['schemas'].each do |schema|
        all_schemas << schema['schema_name'] if feature_enabled?(schema.fetch('feature_flag', nil))
      end
    end

    system_users.each do |user|
      user['schemas'].each do |schema|
        all_schemas << schema['schema_name'] if feature_enabled?(schema.fetch('feature_flag', nil))
      end
    end

    all_schemas.uniq
  end

  def get_existing_schemas
    result = execute_query(
      <<~SQL.squish,
        SELECT DISTINCT schemaname FROM pg_tables
        WHERE schemaname NOT LIKE 'pg_%' AND schemaname != 'information_schema'
      SQL
    )
    result.map { |row| row['schemaname'] }
  end

  def get_existing_configured_schemas
    configured_schemas = get_all_configured_schemas
    existing_schemas = get_existing_schemas
    configured_schemas & existing_schemas
  end

  def get_schemas_for_user_drop
    get_existing_configured_schemas.reject { |schema| ['idp', 'pg_catalog'].include?(schema) }
  end

  def build_drop_user_sql(user_name, schemas)
    revoke_statements = schemas.map do |schema|
      <<~SQL
        REVOKE ALL ON SCHEMA #{schema} FROM "#{user_name}";
        REVOKE ALL ON ALL TABLES IN SCHEMA #{schema} FROM "#{user_name}";
      SQL
    end.join("\n")

    <<~SQL
      REVOKE ALL ON DATABASE analytics FROM "#{user_name}";
      #{revoke_statements}
      DROP USER "#{user_name}";
    SQL
  end

  def drop_users
    Rails.logger.info('Dropping removed users')

    schemas = get_schemas_for_user_drop

    user_sql = users_to_drop(canonical_users, current_users).filter_map do |name|
      next if disallowed_characters?(name)
      next unless name.start_with?('IAM:')

      Rails.logger.info("Removing user #{name}")
      build_drop_user_sql(name, schemas)
    end

    return if user_sql.empty?

    execute_query(user_sql.join("\n"))
  end

  def create_users
    Rails.logger.info('Creating new users')

    user_sql = users_to_create(canonical_users, current_users).filter_map do |name|
      next if disallowed_characters?(name)

      Rails.logger.info("Creating user #{name}")
      "CREATE USER \"#{name}\" WITH PASSWORD DISABLE SESSION TIMEOUT 900;"
    end

    return if user_sql.empty?

    execute_query(user_sql.join("\n"))
  end

  def create_lambda_user(user_name, schemas)
    Rails.logger.info("Creating lambda user #{user_name}")

    result = execute_query("SELECT usename FROM pg_user WHERE usename = '#{user_name}'")
    user_exists = result.any?

    schema_privileges = schemas.map do |schema|
      create_lambda_user_privileges(user_name, schema)
    end

    sql = [
      *("CREATE USER #{user_name} WITH PASSWORD DISABLE SESSION TIMEOUT 900;" unless user_exists),
      schema_privileges,
    ]

    execute_query(sql.flatten.join("\n"))
  end

  def create_lambda_user_privileges(user_name, schema)
    <<~SQL
      CREATE SCHEMA IF NOT EXISTS #{schema};
      GRANT CREATE ON SCHEMA #{schema} TO "#{user_name}";
      GRANT USAGE ON SCHEMA #{schema} TO "#{user_name}";
      GRANT ALL PRIVILEGES ON SCHEMA #{schema} TO "#{user_name}";
      GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA #{schema} TO "#{user_name}";
    SQL
  end

  def create_system_user(user_name, schemas, secret_id, syslog_access)
    Rails.logger.info("Creating system user #{user_name}")

    result = execute_query("SELECT usename FROM pg_user WHERE usename = '#{user_name}'")
    user_exists = result.any?

    active_schemas = schemas.select { |s| feature_enabled?(s['feature_flag']) }
    schema_privileges = active_schemas.map do |schema|
      create_system_user_privileges(
        user_name,
        schema['schema_name'],
        schema['schema_privileges'],
        schema['table_privileges'],
        schema['tables'],
      )
    end

    password_option = secret_id.nil? ? 'DISABLE' : redshift_secret(user_name, secret_id)
    syslog_access_option = syslog_access ? 'SYSLOG ACCESS UNRESTRICTED' : 'SYSLOG ACCESS RESTRICTED'

    create_user_sql = "CREATE USER #{user_name} WITH PASSWORD #{password_option} " \
                      "#{syslog_access_option} SESSION TIMEOUT 900;"

    sql = [
      *(create_user_sql unless user_exists),
      schema_privileges,
    ]

    execute_query(sql.flatten.join("\n"))
  end

  def user_exists?(user_name)
    result = execute_query("SELECT usename FROM pg_user WHERE usename = '#{user_name}'")
    result.any?
  end

  def dbt_user?(user_name)
    ['marts', 'qa_marts', 'fraudops_marts', 'fraudops_qa_marts'].include?(user_name)
  end

  def dbt_user_schema?(schema_name)
    ['marts', 'qa_marts', 'fraudops_marts', 'fraudops_qa_marts'].include?(schema_name)
  end

  def should_create_schema?(user_name, schema_name, schema_privileges)
    dbt_user?(user_name) && dbt_user_schema?(schema_name) && schema_privileges == 'ALL PRIVILEGES'
  end

  def create_system_user_privileges(user_name, schema_name, schema_privileges, table_privileges,
                                    tables)
    table_list = if tables.nil? || tables.empty?
                   "ALL TABLES IN SCHEMA #{schema_name}"
                 else
                   tables.map { |table| "#{schema_name}.#{table}" }.join(', ')
                 end

    schema_creation = should_create_schema?(
      user_name, schema_name,
      schema_privileges
    ) ? "CREATE SCHEMA IF NOT EXISTS #{schema_name};\n" : ''

    <<~SQL
      #{schema_creation}GRANT #{schema_privileges} ON SCHEMA #{schema_name} TO #{user_name};
      GRANT #{table_privileges} ON #{table_list} TO #{user_name};
    SQL
  end

  def create_user_group(user_group)
    Rails.logger.info("Creating user group #{user_group['name']}")

    result = execute_query(
      "SELECT groname FROM pg_group WHERE groname = #{quote(user_group['name'])}",
    )

    if !result.any?
      sql = "CREATE group #{user_group['name']};"
      execute_query(sql)
    end

    create_schema_privileges_for_group(user_group)
  end

  def revoke_all_privileges_for_group(group_name, schema_name)
    <<~SQL
      REVOKE ALL ON SCHEMA #{schema_name} FROM GROUP #{group_name};
      REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA #{schema_name} FROM GROUP #{group_name};
    SQL
  end

  def create_schema_privileges_for_group(user_group)
    Rails.logger.info("Updating schema privileges for user group #{user_group['name']}")

    result = execute_query(
      "SELECT groname FROM pg_group WHERE groname = #{quote(user_group['name'])}",
    )
    return if !result.any?

    active_schemas = user_group['schemas'].select do |s|
      feature_enabled?(s.fetch('feature_flag', nil))
    end

    schemas_to_revoke = get_existing_configured_schemas

    revoke_statements = schemas_to_revoke.map do |schema_name|
      revoke_all_privileges_for_group(user_group['name'], schema_name)
    end

    grant_statements = active_schemas.map do |schema|
      create_user_group_privileges(
        user_group['name'],
        schema['schema_name'],
        schema['schema_privileges'],
        schema['table_privileges'],
        schema.fetch('restricted_tables', []),
      )
    end

    return if revoke_statements.empty? && grant_statements.empty?

    sql = <<~SQL
      #{revoke_statements.join("\n")}
      #{grant_statements.join("\n")}
    SQL

    execute_query(sql)
  end

  def create_user_group_privileges(group_name, schema_name, schema_privileges, table_privileges,
                                   restricted_tables = [])
    sql = <<~SQL
      GRANT #{schema_privileges} ON SCHEMA #{schema_name} TO GROUP #{group_name};
      GRANT #{table_privileges} ON ALL TABLES IN SCHEMA #{schema_name} TO GROUP #{group_name};
    SQL

    if dbt_user_schema?(schema_name) && user_exists?(schema_name)
      sql += <<~SQL
        ALTER DEFAULT PRIVILEGES FOR USER #{schema_name} IN SCHEMA #{schema_name} GRANT #{table_privileges} ON TABLES TO GROUP #{group_name};
      SQL
    end

    restricted_tables.each do |table|
      sql += "REVOKE ALL PRIVILEGES ON TABLE #{schema_name}.#{table} FROM GROUP #{group_name};\n"
    end

    sql
  end

  def sync_user_group(group)
    Rails.logger.info("Syncing users for #{group['name']}")

    current_group_users_statement = <<~SQL
      SELECT usename FROM pg_user, pg_group
      WHERE pg_user.usesysid = ANY(pg_group.grolist)
      AND pg_group.groname='#{group['name']}'
    SQL

    result = execute_query(current_group_users_statement)
    user_group_sql = []

    if result.any?
      current_group_users = result.map { |row| row['usename'] }
      user_group_sql.append(
        "ALTER GROUP #{group['name']} DROP USER #{current_group_users.map do |v|
          "\"#{v}\""
        end.join(', ')};",
      )
    end

    new_group_users = canonical_users.select do |user|
      users_yaml[user.gsub('IAM:', '')]['aws_groups'].any? do |aws_group|
        group['aws_groups'][env_type].include?(aws_group)
      end
    end

    if new_group_users.any?
      user_group_sql.append(
        "ALTER GROUP #{group['name']} ADD USER #{new_group_users.map do |v|
          "\"#{v}\""
        end.join(', ')}",
      )
    end

    if user_group_sql.any?
      execute_query(user_group_sql.join("\n"))
    else
      Rails.logger.info("User group #{group['name']} is empty")
    end
  end
end

if $PROGRAM_NAME == __FILE__
  basename = File.basename($PROGRAM_NAME)

  optparse = OptionParser.new do |opts|
    opts.banner = <<~EOM
      usage: #{basename} [OPTIONS] CONFIG_YAML_FILE USERS_YAML_FILE
    EOM
  end

  args = optparse.parse!

  unless args.length == 2
    warn optparse
    abort
  end

  config_file_path = args[0]
  users_yaml_path = args[1]

  sync = RedshiftSync.new(
    config_file_path: config_file_path,
    users_yaml_path: users_yaml_path,
  )

  sync.sync
end
