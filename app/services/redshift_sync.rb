#!/usr/bin/env ruby
# frozen_string_literal: true

require 'yaml'
require 'aws-sdk-secretsmanager'
require 'digest'
require 'json'

require_relative '../../config/environment'
class RedshiftSync
  include UserSyncConfig

  DATABASES = ['analytics', 'analytics_zetl'].freeze

  attr_reader :database

  def initialize(database: nil)
    @database = database
  end

  # Redshift users, groups, and roles are cluster-global,schema/table grants are database-scoped
  def sync
    self.class.new(database: DATABASES.first).sync_cluster

    DATABASES.each do |name|
      self.class.new(database: name).sync_database_grants
    end
  end

  # Create identities, groups, and roles, and sync memberships.
  def sync_cluster
    Rails.logger.info('Starting Redshift cluster-level user sync')

    lambda_users.each do |lambda_user|
      create_lambda_user_identity(lambda_user['user_name'])
    end

    system_users.each do |system_user|
      next unless feature_enabled?(system_user['feature_flag'])

      create_system_user_identity(
        system_user['user_name'],
        system_user['secret_id'],
        system_user['syslog_access'],
      )
    end

    groups.each { |group| create_group(group['name']) }

    drop_users
    new_users = create_users

    groups.each { |group| sync_user_group(group) }

    apply_masking_for_new_users(new_users) if new_users.any? && analytics_database?

    roles.each do |role|
      create_user_role(role) if feature_enabled?(role['feature_flag'])
    end

    Rails.logger.info('Redshift cluster-level user sync completed successfully')
  end

  # Database-level pass: apply schema/table grants for the current database.
  def sync_database_grants
    unless feature_enabled?(database_config['feature_flag'])
      Rails.logger.info(
        "Skipping Redshift grants for database=#{database}: feature flag disabled",
      )
      return
    end

    Rails.logger.info("Applying Redshift grants for database=#{database}")

    # system-user grants create the DBT schemas (CREATE SCHEMA)
    # Iterate over system users in cluster-list order (dbt marts users before rails_worker)
    lambda_users.each do |lambda_user|
      schemas = schemas_for(lambda_user_grants, 'user_name', lambda_user['user_name'])
      apply_lambda_user_grants(lambda_user['user_name'], schemas) if schemas.present?
    end

    system_users.each do |system_user|
      next unless feature_enabled?(system_user['feature_flag'])

      schemas = schemas_for(system_user_grants, 'user_name', system_user['user_name'])
      apply_system_user_grants(system_user['user_name'], schemas) if schemas.present?
    end

    groups.each do |group|
      schemas = schemas_for(group_grants, 'name', group['name'])
      apply_group_grants(group, schemas) if schemas
    end

    Rails.logger.info("Redshift grants applied successfully for database=#{database}")
  end

  private

  def config_file
    @config_file ||= begin
      terraform_config_path = IdentityConfig.local_devops_path(
        :identity_devops,
        "terraform/data-warehouse/#{env_name}/main.tf",
      )
      File.read(terraform_config_path)
    end
  end

  def interpolate_env_name(str)
    return str unless str.is_a?(String)
    str.gsub('%{env_name}', env_name)
  end

  def interpolate_config_hash(hash)
    case hash
    when Hash
      hash.each_with_object({}) do |(key, value), result|
        result[interpolate_env_name(key)] = interpolate_config_hash(value)
      end
    when Array
      hash.map { |v| interpolate_config_hash(v) }
    when String
      interpolate_env_name(hash)
    else
      hash
    end
  end

  def feature_enabled?(feature_flag)
    return true if feature_flag.nil?

    flags_to_check = feature_flag.is_a?(Array) ? feature_flag : [feature_flag]

    flags_to_check.any? do |flag|
      # Check both Terraform config and IdentityConfig.store; flag is enabled if either is true
      config_file.match?(/^\s*(?!#|\/\/)#{flag}\s+=\s+true/m) ||
        (IdentityConfig.store.respond_to?(flag) && IdentityConfig.store.public_send(flag) == true)
    end
  end

  def analytics_database?
    database == 'analytics'
  end

  def connection_class
    case database
    when 'analytics' then DataWarehouseApplicationRecord
    when 'analytics_zetl' then DataWarehouseApplicationRecordZeroEtl
    else raise ArgumentError, "unknown database #{database.inspect}"
    end
  end

  def connection
    connection_class.connection
  end

  def database_config
    redshift_config['databases'].fetch(database)
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

  # --- Cluster-level accessors (defined once, not per database) ---

  def lambda_users
    cluster_config.fetch('lambda_users', []).map { |user| interpolate_config_hash(user) }
  end

  def system_users
    cluster_config.fetch('system_users', []).map { |user| interpolate_config_hash(user) }
  end

  def groups
    cluster_config.fetch('user_groups', []).map { |group| interpolate_config_hash(group) }
  end

  def roles
    cluster_config.fetch('user_roles', []).map { |role| interpolate_config_hash(role) }
  end

  # --- Database-level grant accessors (per database) ---
  def lambda_user_grants
    interpolate_config_hash(database_config.fetch('lambda_users', []))
  end

  def system_user_grants
    interpolate_config_hash(database_config.fetch('system_users', []))
  end

  def group_grants
    interpolate_config_hash(database_config.fetch('user_groups', []))
  end

  def schemas_for(grant_list, key, name)
    entry = grant_list.find { |e| e[key] == name }
    entry && entry['schemas']
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
  rescue ActiveRecord::StatementInvalid => e
    Rails.logger.error(
      {
        name: 'RedshiftSync',
        error: 'SQL execution failed',
        database: database,
        message: e.message,
        failed_sql: redact_secrets(sql),
      }.to_json,
    )
    raise
  end

  def redact_secrets(sql)
    sql.gsub(/(PASSWORD\s+)'[^']*'/i, "\\1'[REDACTED]'")
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

    group_grants.each do |group|
      group['schemas'].each do |schema|
        all_schemas << schema['schema_name'] if feature_enabled?(schema.fetch('feature_flag', nil))
      end
    end

    system_user_grants.each do |user|
      user['schemas'].each do |schema|
        all_schemas << schema['schema_name'] if feature_enabled?(schema.fetch('feature_flag', nil))
      end
    end

    all_schemas.uniq
  end

  def get_existing_schemas
    # schema finds with tables and views to ensure view-only schemas are included.
    result = execute_query(
      <<~SQL.squish,
        SELECT DISTINCT schemaname FROM pg_tables
        WHERE schemaname NOT LIKE 'pg_%' AND schemaname != 'information_schema'
        UNION
        SELECT DISTINCT schemaname FROM pg_views
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
      REVOKE ALL ON DATABASE #{connection.current_database} FROM "#{user_name}";
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

    newly_created = []
    user_sql = users_to_create(canonical_users, current_users).filter_map do |name|
      next if disallowed_characters?(name)

      Rails.logger.info("Creating user #{name}")
      newly_created << name
      "CREATE USER \"#{name}\" WITH PASSWORD DISABLE SESSION TIMEOUT 900;"
    end

    return [] if user_sql.empty?

    execute_query(user_sql.join("\n"))
    newly_created
  end

  def apply_masking_for_new_users(new_users)
    Rails.logger.info("Applying masking policies for new user(s): #{new_users.join(', ')}")
    RedshiftMaskingSync.new.sync(user_filter: new_users)
  rescue StandardError => e
    Rails.logger.warn("Failed to apply masking policies for new users: #{e.message}")
  end

  # --- Lambda users ---

  def create_lambda_user_identity(user_name)
    return if user_exists?(user_name)

    Rails.logger.info("Creating lambda user #{user_name}")
    execute_query("CREATE USER #{user_name} WITH PASSWORD DISABLE SESSION TIMEOUT 900;")
  end

  def apply_lambda_user_grants(user_name, schemas)
    Rails.logger.info("Applying grants for lambda user #{user_name} on database=#{database}")

    sql = schemas.map { |schema| create_lambda_user_privileges(user_name, schema) }
    execute_query(sql.join("\n"))
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

  # --- System users ---

  def create_system_user_identity(user_name, secret_id, syslog_access)
    return if user_exists?(user_name)

    Rails.logger.info("Creating system user #{user_name}")

    password_option = secret_id.nil? ? 'DISABLE' : redshift_secret(user_name, secret_id)
    syslog_access_option = syslog_access ? 'SYSLOG ACCESS UNRESTRICTED' : 'SYSLOG ACCESS RESTRICTED'

    execute_query(
      "CREATE USER #{user_name} WITH PASSWORD #{password_option} " \
        "#{syslog_access_option} SESSION TIMEOUT 900;",
    )
  end

  def apply_system_user_grants(user_name, schemas)
    active_schemas = schemas.select { |s| feature_enabled?(s['feature_flag']) }
    return if active_schemas.empty?

    Rails.logger.info("Applying grants for system user #{user_name} on database=#{database}")

    schema_privileges = active_schemas.map do |schema|
      create_system_user_privileges(
        user_name,
        schema['schema_name'],
        schema['schema_privileges'],
        schema['table_privileges'],
        schema['tables'],
      )
    end

    execute_query(schema_privileges.flatten.join("\n"))

    active_schemas.each do |schema|
      Rails.logger.info(
        "Granted privileges on schema #{schema['schema_name']} to user #{user_name}",
      )
    end
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
    schema_creation = should_create_schema?(
      user_name, schema_name,
      schema_privileges
    ) ? "CREATE SCHEMA IF NOT EXISTS #{schema_name};\n" : ''

    sql = <<~SQL
      #{schema_creation}GRANT #{schema_privileges} ON SCHEMA #{schema_name} TO #{user_name};
    SQL

    return sql if tables.blank? && dbt_user?(user_name) && user_name == schema_name

    if tables.blank? && dbt_user_schema?(schema_name) && user_exists?(schema_name)
      sql += <<~SQL
        ALTER DEFAULT PRIVILEGES FOR USER #{schema_name} IN SCHEMA #{schema_name} GRANT #{table_privileges} ON TABLES TO #{user_name};
      SQL
      return sql
    end

    table_list = if tables.blank?
                   "ALL TABLES IN SCHEMA #{schema_name}"
                 else
                   tables.map { |table| "#{schema_name}.#{table}" }.join(', ')
                 end

    sql += <<~SQL
      GRANT #{table_privileges} ON #{table_list} TO #{user_name};
    SQL

    sql
  end

  # --- Groups ---

  def create_group(group_name)
    result = execute_query(
      "SELECT groname FROM pg_group WHERE groname = #{quote(group_name)}",
    )
    return if result.any?

    Rails.logger.info("Creating user group #{group_name}")
    execute_query("CREATE group #{group_name};")
  end

  def apply_group_grants(user_group, schemas)
    create_schema_privileges_for_group(user_group, schemas)
  end

  def revoke_all_privileges_for_group(group_name, schema_name)
    <<~SQL
      REVOKE ALL ON SCHEMA #{schema_name} FROM GROUP #{group_name};
      REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA #{schema_name} FROM GROUP #{group_name};
    SQL
  end

  def create_schema_privileges_for_group(user_group, schemas)
    Rails.logger.info(
      "Updating schema privileges for user group #{user_group['name']} on database=#{database}",
    )

    result = execute_query(
      "SELECT groname FROM pg_group WHERE groname = #{quote(user_group['name'])}",
    )
    return if !result.any?

    active_schemas = schemas.select do |s|
      feature_enabled?(s.fetch('feature_flag', nil))
    end

    active_schema_names = active_schemas.map { |s| s['schema_name'] }
    schemas_to_revoke = get_existing_configured_schemas - active_schema_names

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

    active_schemas.each do |schema|
      Rails.logger.info(
        "Granted privileges on schema #{schema['schema_name']} to group #{user_group['name']}",
      )
    end
  end

  def create_user_group_privileges(group_name, schema_name, schema_privileges, table_privileges,
                                   restricted_tables = [])
    sql = <<~SQL
      GRANT #{schema_privileges} ON SCHEMA #{schema_name} TO GROUP #{group_name};
    SQL

    if dbt_user_schema?(schema_name) && user_exists?(schema_name)
      sql += <<~SQL
        ALTER DEFAULT PRIVILEGES FOR USER #{schema_name} IN SCHEMA #{schema_name} GRANT #{table_privileges} ON TABLES TO GROUP #{group_name};
      SQL
    else
      sql += <<~SQL
        GRANT #{table_privileges} ON ALL TABLES IN SCHEMA #{schema_name} TO GROUP #{group_name};
      SQL
    end

    restricted_tables.each do |table|
      sql += <<~SQL
        REVOKE ALL PRIVILEGES ON TABLE #{schema_name}.#{table} FROM GROUP #{group_name};
      SQL
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
      quoted_new_users = new_group_users.map { |v| "\"#{v}\"" }.join(', ')
      user_group_sql.append("ALTER GROUP #{group['name']} ADD USER #{quoted_new_users};")

      group['system_roles']&.each do |role|
        user_group_sql.append("GRANT ROLE #{role} TO #{quoted_new_users};")
      end
    end

    if user_group_sql.any?
      execute_query(user_group_sql.join("\n"))
    else
      Rails.logger.info("User group #{group['name']} is empty")
    end
  end

  # --- Roles ---

  def create_user_role(user_role)
    Rails.logger.info("Checking user role #{user_role['role_name']}...")

    result = execute_query(
      "SELECT role_name FROM svv_roles WHERE role_name = #{quote(user_role['role_name'])}",
    )

    if !result.any?
      sql = "CREATE ROLE #{user_role['role_name']};"
      execute_query(sql)
      Rails.logger.info("Created user role #{user_role['role_name']}")
    end

    sync_user_role(user_role)
  end

  def sync_user_role(user_role)
    Rails.logger.info("Syncing users for role #{user_role['role_name']}")

    current_role_users_statement = <<~SQL
      SELECT user_name
      FROM svv_user_grants
      WHERE role_name = #{quote(user_role['role_name'])}
    SQL

    result = execute_query(current_role_users_statement)
    current_role_users = result.any? ? result.map { |row| row['user_name'] } : []
    desired_role_users = user_role['users'].map { |user| interpolate_env_name(user) }

    users_to_revoke = current_role_users - desired_role_users
    users_to_grant = desired_role_users - current_role_users

    user_role_sql = []

    users_to_revoke.each do |user|
      Rails.logger.info("Revoking role #{user_role['role_name']} from user #{user}")
      user_role_sql.append("REVOKE ROLE #{user_role['role_name']} FROM \"#{user}\";")
    end

    users_to_grant.each do |user|
      Rails.logger.info("Granting role #{user_role['role_name']} to user #{user}")
      user_role_sql.append("GRANT ROLE #{user_role['role_name']} TO \"#{user}\";")
    end

    if user_role_sql.any?
      execute_query(user_role_sql.join("\n"))
    else
      Rails.logger.info("User role #{user_role['role_name']} is already in sync")
    end
  end
end

if $PROGRAM_NAME == __FILE__
  RedshiftSync.new.sync
end
