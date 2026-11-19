# frozen_string_literal: true

module RedshiftMasking
  # Maps discovered columns to existing policies. No policy definitions or DDL.
  class DbtConfiguration
    def initialize
      @config = YAML.safe_load(File.read(Rails.root.join('config/dbt_masking.yaml')))
    end

    def schemas
      @config.fetch('schemas')
    end

    def unmask_role
      @config.fetch('unmask_role')
    end

    def statement_timeout_ms
      value = @config.fetch('statement_timeout_ms')
      unless value.is_a?(Integer) && value.positive?
        raise ArgumentError, 'Expected a positive statement timeout'
      end

      value
    end

    def expected_attachments(schema, table, columns, policy_names)
      raise ArgumentError, "No column metadata for #{schema}.#{table}" if columns.empty?

      columns.flat_map do |column|
        type = policy_type(column)
        [attachment(schema, table, column, type, false),
         attachment(schema, table, column, type, true)]
      end.tap do |expected|
        missing = expected.map(&:policy_name).to_set - policy_names
        raise ArgumentError, "Missing dbt policies: #{missing.to_a.sort.join(', ')}" if missing.any?
      end
    end

    private

    def attachment(schema, table, column, type, unmask)
      PolicyAttachment.new(
        schema: schema, table: table, column: column.fetch('column_name'),
        policy_name: @config.fetch('policies').fetch(type).fetch(unmask ? 'unmask' : 'mask'),
        grantee: unmask ? unmask_role : 'PUBLIC',
        grantee_type: unmask ? 'role' : 'public',
        priority: unmask ? unmask_priority : @config.fetch('mask_priority')
      )
    end

    def unmask_priority
      value = IdentityConfig.store.dbt_unmask_priority
      mask_priority = @config.fetch('mask_priority')
      unless value.is_a?(Integer) && value > mask_priority
        raise ArgumentError, 'dbt_unmask_priority must be an integer above the mask priority'
      end

      value
    end

    # Only mappings whose existing dbt sentinel fits are accepted. In particular,
    # do not coerce CHAR, SMALLINT, narrow decimals, or timezone-aware timestamps.
    def policy_type(column)
      type = column.fetch('data_type').downcase.strip
      mapped = case type
               when 'character varying', 'varchar', 'text'
                 'varchar_max' if column.fetch('character_maximum_length').to_i >= 12
               when 'integer', 'int', 'int4' then 'int'
               when 'bigint', 'int8' then 'bigint'
               when 'numeric', 'decimal'
                 'decimal' if column.fetch('numeric_precision').to_i == 18 &&
                              column.fetch('numeric_scale').to_i.zero?
               when 'real', 'double precision', 'float4', 'float8', 'float' then 'decimal'
               when 'boolean', 'bool' then 'boolean'
               when 'date' then 'date'
               when 'timestamp', 'timestamp without time zone' then 'timestamp'
               when 'super' then 'super'
               end
      return mapped if mapped

      raise ArgumentError,
            "Incompatible dbt policy type/size: #{column.fetch('column_name')} (#{type})"
    end
  end
end
