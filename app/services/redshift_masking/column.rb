# frozen_string_literal: true

module RedshiftMasking
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
end
