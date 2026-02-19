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
end
