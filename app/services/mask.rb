# frozen_string_literal: true

module RedshiftMasking
  # Represents a masking policy attachment to a database column for a specific grantee
  class PolicyAttachment
    attr_reader :policy_name, :schema, :table, :column, :grantee, :priority

    def initialize(policy_name:, schema:, table:, column:, grantee:, priority:)
      @policy_name = policy_name
      @schema = schema
      @table = table
      @column = column
      @grantee = grantee
      @priority = priority
    end

    def key
      "#{column_id}::#{grantee.upcase}"
    end

    def column_id
      "#{schema}.#{table}.#{column}"
    end

    def matches?(other)
      policy_name == other.policy_name && priority == other.priority
    end

    def to_h
      {
        policy_name: policy_name,
        schema: schema,
        table: table,
        column: column,
        grantee: grantee,
        priority: priority,
      }
    end
  end

  # Represents a database column with schema, table, and column identifiers
  class Column
    attr_reader :schema, :table, :column

    def initialize(schema:, table:, column:)
      @schema = schema
      @table = table
      @column = column
    end

    def id
      "#{schema}.#{table}.#{column}"
    end

    def to_h
      { schema: schema, table: table, column: column }
    end

    def self.parse(identifier)
      parts = identifier.split('.')
      return nil unless parts.length == 3

      new(schema: parts[0], table: parts[1], column: parts[2])
    end
  end

  # Manages masking policy configuration including user types, column permissions,
  # and policy templates
  class Configuration
    # Permission type constants
    PERMISSION_ALLOWED = 'allowed'
    PERMISSION_DENIED = 'denied'
    PERMISSION_MASKED = 'masked'
    PERMISSION_TYPES = [PERMISSION_ALLOWED, PERMISSION_DENIED, PERMISSION_MASKED].freeze

    PERMISSION_POLICY_MAP = {
      PERMISSION_ALLOWED => { policy: 'unmask', priority: 300 },
      PERMISSION_DENIED => { policy: 'deny',   priority: 200 },
      PERMISSION_MASKED => { policy: 'mask',   priority: 100 },
    }.freeze

    UNATTACHABLE_USER_TYPES = %w[superuser].freeze

    attr_reader :data_controls, :users_yaml, :env_name

    def initialize(data_controls, users_yaml, env_name: nil)
      @data_controls = data_controls
      @users_yaml = users_yaml
      @env_name = env_name
    end

    def masking_config
      @data_controls['masking_policies']
    end

    def user_types
      masking_config['user_types']
    end

    def columns_config
      masking_config['columns']
    end

    def policy_config(permission_type)
      PERMISSION_POLICY_MAP[permission_type]
    end

    def policy_name(permission_type, column_id)
      policy_details(permission_type, column_id)&.dig(:name)
    end

    def policy_priority(permission_type)
      policy_config(permission_type)&.dig(:priority)
    end

    def policy_details(permission_type, column_id)
      config = policy_config(permission_type)
      return nil unless config

      {
        name: build_policy_name(config[:policy], column_id),
        priority: config[:priority],
      }
    end

    private

    def build_policy_name(policy_type, column_id)
      "#{policy_type}_#{column_id.tr('.', '_')}"
    end
  end

  # Handles database queries for fetching column types and existing masking policies
  class DatabaseQueries
    attr_reader :executor, :logger

    def initialize(executor, logger)
      @executor = executor
      @logger = logger
    end

    def fetch_column_types(columns)
      return {} if columns.empty?

      logger.log_info("fetching data types for #{columns.size} columns")

      conditions = columns.map do |col|
        "(table_schema = #{RedshiftCommon::SqlQuoting.quote_value(col.schema)} " \
        "AND table_name = #{RedshiftCommon::SqlQuoting.quote_value(col.table)} " \
        "AND column_name = #{RedshiftCommon::SqlQuoting.quote_value(col.column)})"
      end.join("\n OR ")

      sql = <<~SQL
        SELECT table_schema, table_name, column_name, data_type, character_maximum_length
        FROM information_schema.columns
        WHERE #{conditions}
      SQL

      executor.query_records(sql).each_with_object({}) do |record, hash|
        key = "#{record[0][:string_value]}.#{record[1][:string_value]}.#{record[2][:string_value]}"
        hash[key] = RedshiftCommon::DataTypeUtils.normalize_data_type(
          record[3][:string_value],
          record[4][:long_value],
          logger: logger,
        )
      end
    end

    def fetch_existing_policies
      sql = <<~SQL
        SELECT policy_name, schema_name, table_name,
               JSON_EXTRACT_ARRAY_ELEMENT_TEXT(input_columns, 0) AS column_name,
               grantee, priority
        FROM svv_attached_masking_policy
      SQL

      executor.query_records(sql).map do |r|
        PolicyAttachment.new(
          policy_name: r[0][:string_value],
          schema: r[1][:string_value],
          table: r[2][:string_value],
          column: r[3][:string_value],
          grantee: r[4][:string_value],
          priority: r[5][:long_value],
        )
      end
    end
  end

  # Resolves user roles to database users and handles user permission logic
  class UserResolver
    attr_reader :config, :users_yaml, :db_user_case_map, :logger

    def initialize(config, users_yaml, db_user_case_map, logger)
      @config = config
      @users_yaml = users_yaml
      @db_user_case_map = db_user_case_map
      @logger = logger
    end

    def resolve_attachable_users(role_names)
      return Set.new if role_names.nil? || role_names.empty?

      role_names.flat_map do |role_name|
        next [] if unattachable_role?(role_name)

        resolve_role_users(role_name)
      end.to_set
    end

    def find_implicitly_masked_users(explicit_permission_sets, all_db_users)
      explicit_users = explicit_permission_sets.values.reduce(Set.new, :|)
      all_attachable_users = all_db_users.
        reject { |u| superuser_db_user?(u) }.
        map { |u| db_user_case_map[u.upcase] }.
        to_set

      all_attachable_users - explicit_users
    end

    def superuser_allowed?(permissions)
      permissions&.dig(Configuration::PERMISSION_ALLOWED)&.
        any? { |role| unattachable_role?(role) } || false
    end

    private

    def resolve_role_users(role_name)
      user_type = find_user_type(role_name)
      return [] unless user_type

      if user_type == 'superuser'
        logger.log_info("skipping superuser '#{role_name}' - policies cannot be attached")
        return []
      end

      users = case user_type
              when 'iam_role' then resolve_iam_role_to_users(role_name)
              when 'redshift_user' then resolve_redshift_user_to_users(role_name)
              else
                logger.log_warn("unknown user type '#{user_type}' for '#{role_name}'")
                []
              end

      users.each { |user| logger.log_warn("user '#{user}' not found in database") unless user }
      users.compact
    end

    def resolve_iam_role_to_users(role_name)
      target_groups = RedshiftCommon::IamRoleUtils.resolve_iam_groups(role_name)

      users_yaml.
        select { |_, data| user_has_aws_group?(data, target_groups) }.
        keys.
        filter_map { |username| db_user_case_map["IAM:#{username.upcase}"] }
    end

    def resolve_redshift_user_to_users(role_name)
      processed_name = preprocess_role_name(role_name)
      [db_user_case_map[processed_name.upcase]].compact
    end

    def preprocess_role_name(role_name)
      role_name.gsub('{env_name}', config.env_name)
    end

    def user_has_aws_group?(user_data, target_groups)
      user_data&.dig('aws_groups')&.any? { |group| target_groups.include?(group) }
    end

    def find_user_type(role_name)
      config.user_types.find { |_type, names| names.include?(role_name) }&.first.tap do |user_type|
        logger.log_warn("role '#{role_name}' not found in user_types") unless user_type
      end
    end

    def unattachable_role?(role_name)
      user_type = find_user_type(role_name)
      Configuration::UNATTACHABLE_USER_TYPES.include?(user_type)
    end

    def superuser_db_user?(username)
      db_user = db_user_case_map[username.upcase]
      return false unless db_user

      config.user_types.fetch('superuser', []).any? { |name| db_user.upcase == name.upcase }
    end
  end

  # Builds masking policy attachments for database columns based on configuration
  # and user permissions
  class PolicyBuilder
    attr_reader :config, :user_resolver, :logger

    def initialize(config, user_resolver, logger)
      @config = config
      @user_resolver = user_resolver
      @logger = logger
    end

    def build_expected_state(column_types, db_users)
      config.columns_config.flat_map do |entry|
        entry.map do |column_id, permissions|
          build_policies_for_column(column_id, permissions, column_types, db_users)
        end
      end.flatten
    end

    def build_policies_for_column(column_id, permissions, column_types, db_users)
      column = Column.parse(column_id)
      return [] unless column && column_types[column_id]

      if user_resolver.superuser_allowed?(permissions)
        build_per_user_policies(column_id, column, permissions, db_users)
      else
        build_public_baseline_policies(column_id, column, permissions)
      end
    end

    private

    def empty_permission_sets
      Configuration::PERMISSION_TYPES.index_with { Set.new }
    end

    def build_public_baseline_policies(column_id, column, permissions)
      policies = [
        build_policy_entry(
          config.policy_name(Configuration::PERMISSION_MASKED, column_id),
          column,
          'PUBLIC',
          10,
        ),
      ]

      return policies unless permissions

      sets = apply_permission_precedence(resolve_permission_user_sets(permissions))

      policies += build_policy_entries_for_users(
        sets[Configuration::PERMISSION_ALLOWED],
        Configuration::PERMISSION_ALLOWED,
        column_id,
        column,
      )
      policies += build_policy_entries_for_users(
        sets[Configuration::PERMISSION_DENIED],
        Configuration::PERMISSION_DENIED,
        column_id,
        column,
      )
      policies
    end

    def build_per_user_policies(column_id, column, permissions, db_users)
      sets = apply_permission_precedence(resolve_permission_user_sets(permissions))
      implicitly_masked = user_resolver.find_implicitly_masked_users(sets, db_users)

      Configuration::PERMISSION_TYPES.flat_map do |type|
        build_policy_entries_for_users(sets[type], type, column_id, column)
      end + build_policy_entries_for_users(
        implicitly_masked,
        Configuration::PERMISSION_MASKED,
        column_id,
        column,
      )
    end

    def resolve_permission_user_sets(permissions)
      Configuration::PERMISSION_TYPES.index_with do |type|
        user_resolver.resolve_attachable_users(permissions[type])
      end
    end

    def apply_permission_precedence(sets)
      allowed = sets[Configuration::PERMISSION_ALLOWED] || Set.new
      masked  = (sets[Configuration::PERMISSION_MASKED]  || Set.new) - allowed
      denied  = (sets[Configuration::PERMISSION_DENIED]  || Set.new) - allowed - masked
      {
        Configuration::PERMISSION_ALLOWED => allowed,
        Configuration::PERMISSION_MASKED => masked,
        Configuration::PERMISSION_DENIED => denied,
      }
    end

    def build_policy_entries_for_users(users, perm_type, column_id, column)
      pname = config.policy_name(perm_type, column_id)
      priority = config.policy_priority(perm_type)
      users.map { |user| build_policy_entry(pname, column, user, priority) }
    end

    def build_policy_entry(p_name, col, grantee, priority)
      PolicyAttachment.new(
        policy_name: p_name,
        schema: col.schema,
        table: col.table,
        column: col.column,
        grantee: grantee,
        priority: priority,
      )
    end
  end

  # Detects differences between expected and actual masking policy attachments
  class DriftDetector
    attr_reader :logger

    def initialize(logger)
      @logger = logger
    end

    def detect(expected_list, actual_list)
      logger.log_info('detecting drift in masking policies')

      expected_map = expected_list.index_by(&:key)
      actual_map = actual_list.index_by(&:key)

      drift = { missing: [], extra: [], mismatched: [] }

      find_missing_and_mismatched(expected_map, actual_map, drift)
      find_extra(expected_map, actual_map, drift)

      drift
    end

    private

    def find_missing_and_mismatched(expected_map, actual_map, drift)
      expected_map.each do |key, expected|
        actual = actual_map[key]
        if actual.nil?
          drift[:missing] << expected
          logger.log_warn("MISSING: #{expected.grantee} on #{expected.column_id}")
        elsif !expected.matches?(actual)
          drift[:mismatched] << { expected: expected, actual: actual }
          logger.log_warn(
            "MISMATCH: #{expected.grantee} on #{expected.column_id} " \
            "(Expected #{expected.policy_name} P#{expected.priority})",
          )
        end
      end
    end

    def find_extra(expected_map, actual_map, drift)
      actual_map.each do |key, actual|
        unless expected_map.key?(key)
          drift[:extra] << actual
          logger.log_warn("EXTRA: #{actual.grantee} on #{actual.column_id}")
        end
      end
    end
  end

  # Executes SQL commands for creating masking policies and applying policy attachment corrections
  class SqlExecutor
    POLICY_USING_CLAUSES = {
      Configuration::PERMISSION_MASKED => "('XXXX'::%<type>s)",
      Configuration::PERMISSION_ALLOWED => '(value)',
      Configuration::PERMISSION_DENIED => '(NULL::%<type>s)',
    }.freeze

    POLICY_TEMPLATES = POLICY_USING_CLAUSES.transform_values do |using_clause|
      "CREATE MASKING POLICY %<name>s IF NOT EXISTS WITH(value %<type>s) USING #{using_clause}"
    end.freeze

    attr_reader :executor, :config, :logger, :dry_run

    def initialize(executor, config, logger, dry_run: false)
      @executor = executor
      @config = config
      @logger = logger
      @dry_run = dry_run
    end

    def create_masking_policies(column_types)
      return if column_types.empty?

      logger.log_info('creating masking policies')

      sql_parts = column_types.flat_map do |column_id, data_type|
        Configuration::PERMISSION_TYPES.map do |perm_type|
          build_policy_sql(perm_type, column_id, data_type)
        end
      end

      sql = "#{sql_parts.join(";\n")};"

      execute_or_log_dry_run(
        "created/verified policies for #{column_types.size} columns",
        sql,
      ) { executor.execute_and_wait(sql) }
    end

    def apply_corrections(drift)
      to_detach = drift[:extra] + drift[:mismatched].map { |m| m[:actual] }
      to_attach = drift[:missing] + drift[:mismatched].map { |m| m[:expected] }

      return logger.log_info('no changes needed') if (to_detach + to_attach).empty?

      to_detach.each do |p|
        execute_correction(detach_sql(p), "Detaching #{p.policy_name} from #{p.grantee}")
      end
      to_attach.each do |p|
        execute_correction(attach_sql(p), "Attaching #{p.policy_name} to #{p.grantee}")
      end
    end

    private

    def build_policy_sql(permission_type, column_id, data_type)
      format(
        POLICY_TEMPLATES[permission_type],
        name: config.policy_name(permission_type, column_id),
        type: data_type,
      )
    end

    def execute_or_log_dry_run(description, sql, &block)
      if dry_run
        logger.log_info("[DRY RUN] Would #{description.downcase}")
        logger.log_info("[DRY RUN] SQL: #{sql.strip.gsub(/\s+/, ' ')}")
      else
        logger.log_info(description)
        block.call(&block)
      end
    end

    def execute_correction(sql, description)
      execute_or_log_dry_run(description, sql) do
        executor.execute_and_wait(sql)
      end
    rescue RuntimeError => e
      logger.log_warn("Failed to apply correction: #{e.message}")
      logger.log_debug("Failed SQL: #{sql}")
    end

    def detach_sql(policy)
      <<~SQL
        DETACH MASKING POLICY #{policy.policy_name}
        ON #{policy.schema}.#{policy.table} (#{policy.column})
        FROM #{RedshiftCommon::SqlQuoting.quote_grantee(policy.grantee)};
      SQL
    end

    def attach_sql(policy)
      <<~SQL
        ATTACH MASKING POLICY #{policy.policy_name}
        ON #{policy.schema}.#{policy.table} (#{policy.column})
        TO #{RedshiftCommon::SqlQuoting.quote_grantee(policy.grantee)}
        PRIORITY #{policy.priority};
      SQL
    end
  end
end

# Zeitwerk expects app/services/mask.rb to define Mask
Mask = RedshiftMasking
