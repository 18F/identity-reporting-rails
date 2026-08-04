# frozen_string_literal: true

module RedshiftMasking
  # Represents a database column identified by database, schema, table, and column
  class Column
    attr_reader :database, :schema, :table, :column

    def initialize(database:, schema:, table:, column:)
      @database = database
      @schema = schema
      @table = table
      @column = column
    end

    # Identifier scoped to the database: database.schema.table.column
    def id
      "#{database}.#{schema}.#{table}.#{column}"
    end

    # schema.table.column, as written in mask.yaml (the policy-name basis)
    def unqualified_id
      "#{schema}.#{table}.#{column}"
    end

    def to_h
      { database: database, schema: schema, table: table, column: column }
    end

    # Parses a "schema.table.column" identifier scoped to +database+.
    def self.parse(identifier, database:)
      parts = identifier.split('.')
      return nil unless parts.length == 3

      new(database: database, schema: parts[0], table: parts[1], column: parts[2])
    end
  end
end
