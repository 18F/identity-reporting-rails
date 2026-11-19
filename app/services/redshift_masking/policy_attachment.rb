# frozen_string_literal: true

module RedshiftMasking
  # Represents a masking policy attachment to a database column for a specific grantee
  class PolicyAttachment
    attr_reader :policy_name, :schema, :table, :column, :grantee, :grantor, :priority,
                :grantee_type

    def initialize(policy_name:, schema:, table:, column:, grantee:, priority:, grantor: nil,
                   grantee_type: nil)
      @policy_name = policy_name
      @schema = schema
      @table = table
      @column = column
      @grantee = grantee
      @grantor = grantor
      @priority = priority
      @grantee_type = grantee_type || (grantee.upcase == 'PUBLIC' ? 'public' : 'user')
    end

    def key
      [schema, table, column, grantee_type, grantee.upcase]
    end

    def column_id
      "#{schema}.#{table}.#{column}"
    end

    def matches?(other)
      policy_name == other.policy_name && priority == other.priority &&
        grantee_type == other.grantee_type
    end

    def to_h
      {
        policy_name: policy_name,
        schema: schema,
        table: table,
        column: column,
        grantee: grantee,
        grantee_type: grantee_type,
        grantor: grantor,
        priority: priority,
      }
    end
  end
end
