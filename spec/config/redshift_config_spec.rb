require 'rails_helper'

# RedshiftSync reads config/redshift_config.yaml with a default for every key,
# so a hand edit that is misspelled, misplaced, or points at nothing is silently
# ignored in PROD rather than raising. These are the rules the file must follow;
# the role-membership checks live in spec/services/redshift_sync_spec.rb.
RSpec.describe 'config/redshift_config.yaml' do
  let(:real_config) do
    YAML.safe_load(File.read(Rails.root.join('config/redshift_config.yaml')))
  end

  # The data_warehouse migrations create every other schema; RedshiftSync creates
  # these itself, from the matching DBT user's ALL PRIVILEGES grant.
  let(:sync_created_schemas) { %w[fraudops_marts fraudops_qa_marts] }

  # The shape RedshiftSync expects. A Hash lists every allowed key (a trailing
  # '?' marks it optional), [x] is a list of x, { String => x } is a map with
  # free-form keys, and a leaf is a Class or a lambda returning a problem.
  let(:config_shape) do
    # Pasted into SQL unquoted, so Redshift case-folds or rejects anything else.
    identifier = lambda do |v|
      unless v.is_a?(String) && v.match?(/\A[a-z_][a-z0-9_]*\z/)
        'must be a lowercase SQL identifier'
      end
    end
    lambda_user = lambda do |v|
      unless v.is_a?(String) && v.match?(/\AIAMR:%\{env_name\}_[a-z0-9_]+\z/)
        'must look like IAMR:%{env_name}_<name>'
      end
    end
    secret_id = lambda do |v|
      'must include %{env_name}' unless v.is_a?(String) && v.include?('%{env_name}')
    end
    # feature_enabled? treats any name it cannot find in IdentityConfig or the
    # Terraform main.tf as off, so a misspelled flag silently disables the entry.
    known_flags = IdentityConfig.store.to_h.filter_map do |key, value|
      key.to_s if [true, false].include?(value)
    end + %w[dbt_enabled redshift_idp_connector_enabled redshift_quicksight_connector_enabled]
    flag = lambda do |v|
      unless (v.is_a?(String) || (v.is_a?(Array) && v.any?)) && (Array(v) - known_flags).empty?
        "must be one, or a non-empty list, of: #{known_flags.sort.join(', ')}"
      end
    end
    boolean = ->(v) { 'must be true or false' unless [true, false].include?(v) }
    # Not the same as omitting the key: `tables: []` grants ALL TABLES, and an
    # empty enabled_aws_groups list drops every IAM user in that environment.
    non_empty_list = lambda do |item|
      lambda do |v|
        unless v.is_a?(Array) && v.any? && v.all? { |x| shape_errors(x, item, '').empty? }
          'must be a non-empty list'
        end
      end
    end
    # No bare ALL: should_create_schema? matches 'ALL PRIVILEGES' exactly.
    privileges = lambda do |*allowed|
      keyword = Regexp.union(allowed)
      lambda do |v|
        unless v.is_a?(String) && v.match?(/\A#{keyword}(\s*,\s*#{keyword})*\z/)
          "must be a comma-separated list of: #{allowed.join(', ')}"
        end
      end
    end

    # Both required: a missing env_type resolves to zero members there, and the
    # sync then revokes everyone. Write `prod: []` if that is really intended.
    env_aws_groups = { 'prod' => [String], 'sandbox' => [String] }
    schema_grant = {
      'schema_name' => identifier,
      'schema_privileges' => privileges.call(
        'ALL PRIVILEGES', 'USAGE', 'CREATE', 'ALTER', 'DROP'
      ),
      'table_privileges' => privileges.call(
        'ALL PRIVILEGES', 'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'REFERENCES',
        'ALTER', 'TRUNCATE'
      ),
      'feature_flag?' => flag,
    }
    tables = non_empty_list.call(identifier)
    database = {
      'feature_flag?' => flag,
      'lambda_users' => [{ 'user_name' => lambda_user, 'schemas' => [identifier] }],
      'system_users' => [{
        'user_name' => identifier,
        'schemas' => [schema_grant.merge('tables?' => tables)],
      }],
      'user_groups' => [{
        'name' => identifier,
        'schemas' => [schema_grant.merge('restricted_tables?' => tables)],
      }],
    }

    {
      'aws_role_map' => { String => String },
      'role_priority' => { String => Integer },
      'enabled_aws_groups' => env_aws_groups.transform_values { non_empty_list.call(String) },
      'cluster' => {
        'lambda_users' => [{ 'user_name' => lambda_user }],
        'system_users' => [{
          'user_name' => identifier,
          'secret_id?' => secret_id,
          'syslog_access?' => boolean,
          'feature_flag?' => flag,
        }],
        'user_groups' => [{ 'name' => identifier, 'aws_groups' => env_aws_groups }],
        'user_roles' => [{
          'role_name' => identifier,
          'member_users?' => [String],
          'aws_groups?' => env_aws_groups,
          'assigned_roles?' => [String],
          'feature_flag?' => flag,
        }],
      },
      'databases' => RedshiftSync::DATABASES.index_with { database },
    }
  end

  # Lists every "path: problem" where value departs from shape.
  def shape_errors(value, shape, path)
    case shape
    when Array
      return ["#{path}: must be a list, got #{value.inspect}"] unless value.is_a?(Array)

      value.each_with_index.flat_map do |item, i|
        shape_errors(item, shape.first, "#{path}[#{entry_name(item) || i}]")
      end
    when Hash
      return ["#{path}: must be a mapping, got #{value.inspect}"] unless value.is_a?(Hash)
      if shape.key?(String)
        return value.flat_map { |key, v| shape_errors(v, shape[String], "#{path}.#{key}") }
      end

      fields = shape.transform_keys { |key| key.delete_suffix('?') }
      required = shape.keys.reject { |key| key.end_with?('?') }

      missing = (required - value.keys).map { |key| "#{path}: missing required key '#{key}'" }
      unknown = (value.keys - fields.keys).map { |key| "#{path}: unrecognized key '#{key}'" }
      nested = value.slice(*fields.keys).flat_map do |key, v|
        shape_errors(v, fields[key], "#{path}.#{key}")
      end
      missing + unknown + nested
    when Class
      value.is_a?(shape) ? [] : ["#{path}: must be a #{shape}, got #{value.inspect}"]
    else
      Array(shape.call(value)).map { |problem| "#{path}: #{value.inspect} #{problem}" }
    end
  end

  def entry_name(entry)
    return unless entry.is_a?(Hash)

    entry.values_at('user_name', 'name', 'role_name', 'schema_name').compact.first
  end

  it 'has no duplicate keys' do
    # YAML keeps only the last of two identical keys in a mapping, silently
    # dropping everything under the first (e.g. a second `system_users:`).
    document = YAML.parse_file(Rails.root.join('config/redshift_config.yaml'))
    mappings = document.select { |node| node.is_a?(Psych::Nodes::Mapping) }

    duplicates = mappings.flat_map do |mapping|
      keys = mapping.children.each_slice(2).map(&:first)
      keys.group_by(&:value).values.select { |same| same.size > 1 }.map do |same|
        "'#{same.first.value}' repeated on lines " \
          "#{same.map { |key| key.start_line + 1 }.join(', ')}"
      end
    end

    expect(duplicates).to be_empty, duplicates.join("\n")
  end

  it 'matches the expected shape' do
    # The sync reads every key with a default, so a misspelled or misplaced key
    # is silently ignored rather than raising: `feature_flags:` creates the user
    # in every environment, `tables:` on a group schema grants ALL TABLES, and
    # `restricted_tables:` on a system user schema restricts nothing.
    errors = shape_errors(real_config, config_shape, 'redshift_config.yaml')

    expect(errors).to be_empty, [
      'redshift_config.yaml does not match the shape RedshiftSync expects ' \
        '(see config_shape in this spec):',
      errors.join("\n"),
    ].join("\n")
  end

  it 'scopes every group aws_groups entry to an enabled aws_group' do
    enabled = real_config['enabled_aws_groups']

    invalid = real_config['cluster']['user_groups'].flat_map do |group|
      group['aws_groups'].flat_map do |env_type, aws_groups|
        (aws_groups - enabled.fetch(env_type, [])).map do |aws_group|
          "Group '#{group['name']}' references aws_group '#{aws_group}' " \
            "that is not enabled for '#{env_type}'"
        end
      end
    end

    expect(invalid).to be_empty, [
      'Group aws_groups must appear in enabled_aws_groups, otherwise they resolve',
      'to zero members:',
      invalid.join("\n"),
    ].join("\n")
  end

  it 'nests only Redshift built-in roles via assigned_roles' do
    nested = (real_config['cluster']['user_roles'] || []).
      flat_map { |role| role.fetch('assigned_roles', []) }

    # CREATE ROLE is never run for these, so they must be Redshift built-ins.
    built_in = %w[sys:dba sys:monitor sys:operator sys:secadmin sys:superuser]
    expect(nested - built_in).to be_empty, "Not Redshift system-defined: #{nested - built_in}"
  end

  it 'gates each role on the feature flags of its flag-gated member users' do
    # GRANT ROLE aborts the sync if a member was never created because its flag is off.
    user_flags = real_config['cluster']['system_users'].
      to_h { |user| [user['user_name'], Array(user['feature_flag'])] }

    ungated = (real_config['cluster']['user_roles'] || []).flat_map do |role|
      role_flags = Array(role['feature_flag'])
      role.fetch('member_users', []).filter_map do |name|
        flags = user_flags.fetch(name, [])
        next if flags.empty? || (role_flags.any? && (role_flags - flags).empty?)

        "Role '#{role['role_name']}' must be gated on #{flags.join(' or ')}, " \
          "like its member '#{name}'"
      end
    end

    expect(ungated).to be_empty, ungated.join("\n")
  end

  it 'grants to exactly the identities defined under cluster' do
    # The grant pass walks the cluster lists and looks each identity up here by
    # name, so a database entry whose name matches nothing is never applied, and
    # a cluster identity with no database entry (e.g. an emptied section) gets
    # no grants, while its old grants stay on the cluster.
    cluster = real_config['cluster']

    problems = %w[lambda_users system_users user_groups].flat_map do |section|
      defined = cluster[section].map { |entry| entry_name(entry) }
      granted = real_config['databases'].transform_values do |db_config|
        (db_config[section] || []).map { |entry| entry_name(entry) }
      end

      granted.flat_map do |db, names|
        (names - defined).map { |name| "databases.#{db}.#{section}[#{name}]: not in cluster" }
      end + (defined - granted.values.flatten).map do |name|
        "cluster.#{section}[#{name}]: not in databases"
      end
    end

    expect(problems).to be_empty, problems.join("\n")
  end

  it 'grants only on schemas and tables that exist' do
    # Names are pasted into GRANT/REVOKE, so a typo aborts the sync, except under
    # lambda_users, where CREATE SCHEMA IF NOT EXISTS quietly adds a junk schema.
    schema_rb = Rails.root.join('db/data_warehouse_schema.rb').read
    # idp_core holds IdpZeroEtlBindingViewSync's views over analytics_zetl's public.
    known = {
      'analytics' => schema_rb.scan(/create_(?:schema|table) "([\w.]+)"/).flatten +
                     %w[pg_catalog pg_catalog.pg_user idp_core] + sync_created_schemas,
      'analytics_zetl' => %w[pg_catalog public],
    }

    unknown = real_config['databases'].flat_map do |db, db_config|
      entries = db_config.values_at('lambda_users', 'system_users', 'user_groups').compact.flatten
      entries.flat_map do |entry|
        entry['schemas'].flat_map do |schema|
          name = entry_name(schema) || schema
          tables = schema.is_a?(Hash) ? schema.values_at('tables', 'restricted_tables') : []
          refs = [name] + tables.compact.flatten.map { |table| "#{name}.#{table}" }
          (refs - known.fetch(db)).map { |ref| "databases.#{db}.#{entry_name(entry)}: '#{ref}'" }
        end
      end
    end

    expect(unknown).to be_empty, [
      'Not in db/data_warehouse_schema.rb or the exceptions listed in this spec:',
      unknown.join("\n"),
    ].join("\n")
  end

  it 'grants on sync-created schemas only after, and only when, their DBT user creates them' do
    # Grants run in cluster.system_users order, then groups, so an earlier or
    # more widely enabled grant on the schema fails on a new cluster.
    users = real_config['cluster']['system_users']
    order = users.map { |user| user['user_name'] }
    user_flags = users.to_h { |user| [user['user_name'], Array(user['feature_flag'])] }

    problems = real_config['databases'].flat_map do |db, db_config|
      # Groups are granted after every system user.
      grantees = db_config['system_users'] + db_config['user_groups']
      grantees.flat_map do |grantee|
        name = entry_name(grantee)
        position = order.index(name) || order.size
        grantee['schemas'].filter_map do |schema|
          creator = schema['schema_name']
          next if sync_created_schemas.exclude?(creator) || name == creator

          creator_flags = user_flags.fetch(creator, [])
          gated = creator_flags.empty? ||
                  [user_flags.fetch(name, []), Array(schema['feature_flag'])].
                    any? { |flags| flags.any? && (flags - creator_flags).empty? }
          if (order.index(creator) || order.size) >= position
            "databases.#{db}.#{name} grants on #{creator} before its DBT user creates it"
          elsif !gated
            "databases.#{db}.#{name} grant on #{creator} must be gated on " \
              "#{creator_flags.join(' or ')}, like the DBT user that creates it"
          end
        end
      end
    end

    expect(problems).to be_empty, problems.join("\n")
  end

  it 'lists each name once per section' do
    # A repeated database entry is ignored (lookups take the first match), a
    # repeated cluster entry or schema is synced twice with the last one winning,
    # and users sharing a secret_id share a password the rotator changes twice.
    sections = real_config['cluster'].transform_keys { |key| "cluster.#{key}" }
    sections['cluster.system_users secret_id'] =
      real_config['cluster']['system_users'].filter_map { |user| user['secret_id'] }
    real_config['databases'].each do |db, db_config|
      %w[lambda_users system_users user_groups].each do |section|
        sections["databases.#{db}.#{section}"] = db_config[section]
        (db_config[section] || []).each do |entry|
          sections["databases.#{db}.#{section}[#{entry_name(entry)}].schemas"] = entry['schemas']
        end
      end
    end

    duplicates = sections.flat_map do |path, entries|
      names = (entries || []).map { |entry| entry_name(entry) || entry }
      names.tally.select { |_, count| count > 1 }.keys.map do |name|
        "#{path}: '#{name}' is listed more than once"
      end
    end

    expect(duplicates).to be_empty, duplicates.join("\n")
  end

  it 'keeps nonprod aws_groups out of prod' do
    # QuicksightSync drops *nonprod groups in prod itself, but RedshiftSync
    # trusts this list, so a nonprod group here would reach the prod warehouse.
    expect(real_config['enabled_aws_groups']['prod'].grep(/nonprod\z/)).to be_empty
  end

  it 'maps every enabled aws_group to a prioritized role' do
    # QuicksightSync silently skips an aws_group missing from aws_role_map, and
    # ranks a role missing from role_priority below every other.
    role_map = real_config['aws_role_map']
    enabled = real_config['enabled_aws_groups'].values.flatten.uniq

    problems =
      (enabled - role_map.keys).map { |g| "aws_group '#{g}' is missing from aws_role_map" } +
      (role_map.values.uniq - real_config['role_priority'].keys).map do |role|
        "role '#{role}' is missing from role_priority"
      end

    expect(problems).to be_empty, problems.join("\n")
  end
end
