# frozen_string_literal: true

module RedshiftMasking
  # Represents a masking policy attachment to a database column for a specific grantee
  class PolicyAttachment
    attr_reader :policy_name, :database, :schema, :table, :column, :grantee, :priority

    def initialize(policy_name:, database:, schema:, table:, column:, grantee:, priority:)
      @policy_name = policy_name
      @database = database
      @schema = schema
      @table = table
      @column = column
      @grantee = grantee
      @priority = priority
    end

    def key
      "#{column_id}::#{grantee.upcase}"
    end

    # database.schema.table.column
    def column_id
      "#{database}.#{schema}.#{table}.#{column}"
    end

    def matches?(other)
      policy_name == other.policy_name && priority == other.priority
    end

    def to_h
      {
        policy_name: policy_name,
        database: database,
        schema: schema,
        table: table,
        column: column,
        grantee: grantee,
        priority: priority,
      }
    end
  end
end
