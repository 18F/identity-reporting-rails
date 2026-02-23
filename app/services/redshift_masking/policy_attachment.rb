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
end
