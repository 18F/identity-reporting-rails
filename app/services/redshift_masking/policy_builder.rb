# frozen_string_literal: true

module RedshiftMasking
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
end
