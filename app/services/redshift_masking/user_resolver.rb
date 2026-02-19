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
end
