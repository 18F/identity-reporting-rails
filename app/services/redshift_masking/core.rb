# frozen_string_literal: true

module RedshiftMasking
  # Resolves user roles to database users and handles user permission logic
  class UserResolver
    # IAM role to AWS group mappings
    IAM_ROLE_GROUPS = {
      'dwuser' => %w[dwuser dwusernonprod],
      'dwpoweruser' => %w[dwpoweruser dwpowerusernonprod],
      'dwadmin' => %w[dwadmin dwadminnonprod],
    }.freeze

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
        logger.info("skipping superuser '#{role_name}' - policies cannot be attached")
        return []
      end

      users = case user_type
              when 'iam_role' then resolve_iam_role_to_users(role_name)
              when 'redshift_user' then resolve_redshift_user_to_users(role_name)
              else
                logger.warn("unknown user type '#{user_type}' for '#{role_name}'")
                []
              end

      users.each { |user| logger.warn("user '#{user}' not found in database") unless user }
      users.compact
    end

    def resolve_iam_role_to_users(role_name)
      target_groups = resolve_iam_groups(role_name)

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
        logger.warn("role '#{role_name}' not found in user_types") unless user_type
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

    # Resolve an IAM role name to the corresponding AWS groups
    def resolve_iam_groups(role_name)
      IAM_ROLE_GROUPS.fetch(role_name, [role_name])
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
        build_public_baseline_policies(column_id, column, permissions, db_users)
      end
    end

    private

    def empty_permission_sets
      Configuration::PERMISSION_TYPES.index_with { Set.new }
    end

    def build_public_baseline_policies(column_id, column, permissions, db_users)
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
        sets[Configuration::PERMISSION_ALLOWED].select { |u| db_users.include?(u.upcase) },
        Configuration::PERMISSION_ALLOWED,
        column_id,
        column,
      )
      policies += build_policy_entries_for_users(
        sets[Configuration::PERMISSION_DENIED].select { |u| db_users.include?(u.upcase) },
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
        build_policy_entries_for_users(
          sets[type].select { |u| db_users.include?(u.upcase) },
          type,
          column_id,
          column,
        )
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

    def detect(expected_list, actual_list, silent: false)
      logger.log_info('detecting drift in masking policies')

      expected_map = expected_list.index_by(&:key)
      actual_map = actual_list.index_by(&:key)

      drift = { missing: [], extra: [], mismatched: [] }

      find_missing_and_mismatched(expected_map, actual_map, drift, silent: silent)
      find_extra(expected_map, actual_map, drift, silent: silent)

      drift
    end

    private

    def find_missing_and_mismatched(expected_map, actual_map, drift, silent: false)
      expected_map.each do |key, expected|
        actual = actual_map[key]
        if actual.nil?
          drift[:missing] << expected
          unless silent
            logger.log_warn("MISSING: #{expected.grantee} on #{expected.column_id}")
          end
        elsif !expected.matches?(actual)
          drift[:mismatched] << { expected: expected, actual: actual }
          unless silent
            logger.log_warn(
              "MISMATCH: #{expected.grantee} on #{expected.column_id} " \
              "(Expected #{expected.policy_name} Priority #{expected.priority})",
            )
          end
        end
      end
    end

    def find_extra(expected_map, actual_map, drift, silent: false)
      actual_map.each do |key, actual|
        unless expected_map.key?(key)
          drift[:extra] << actual
          logger.log_warn("EXTRA: #{actual.grantee} on #{actual.column_id}") unless silent
        end
      end
    end
  end
end
