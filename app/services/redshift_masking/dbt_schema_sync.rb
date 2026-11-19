# frozen_string_literal: true

module RedshiftMasking
  # Optimistic discovery followed by a locked, fresh per-table repair.
  class DbtSchemaSync
    def initialize(connection: nil)
      @connection = connection
      @config = DbtConfiguration.new
    end

    def sync(schema:)
      unless @config.schemas.include?(schema)
        raise ArgumentError, 'Expected one configured dbt fraudops schema'
      end

      @connection ||= DataWarehouseApplicationRecord.connection
      @queries = DbtSchemaQueries.new(@connection)
      unless @queries.role_exists?(@config.unmask_role)
        raise ArgumentError, 'Missing pii_viewers role'
      end

      # Preflight the complete schema before applying any corrections.
      candidates = tables(schema).select { |table| drift(schema, table).values.any?(&:any?) }
      candidates.each { |table| repair(schema, table) }
      Rails.logger.info(
        "dbt masking sync completed: schema=#{schema} candidates=#{candidates.size}",
      )
    end

    private

    def tables(schema, table: nil)
      @queries.relations(schema, table: table).filter_map do |relation|
        name = relation.fetch('table_name')
        if [true, 't', 'true'].include?(relation.fetch('materialized_view'))
          raise ArgumentError, "Cannot reconcile materialized view #{schema}.#{name}"
        end
        case relation.fetch('table_type').upcase
        when 'BASE TABLE' then name
        when 'VIEW'
          Rails.logger.warn("Excluded view #{schema}.#{name}; source masking is not verified")
          nil
        else
          raise ArgumentError, "Unsupported relation #{schema}.#{name}"
        end
      end
    end

    def drift(schema, table)
      expected = @config.expected_attachments(
        schema, table, @queries.columns(schema, table), @queries.policy_names
      )
      actual = @queries.attachments(schema, table)
      managed = actual.select do |policy|
        policy.grantee_type == 'public' ||
          (policy.grantee_type == 'role' && policy.grantee == @config.unmask_role)
      end
      # The generic detector has a one-attachment-per-key contract. Never collapse
      # multiple priorities or duplicate managed attachments into an arbitrary row.
      if managed.group_by(&:key).any? { |_, policies| policies.size > 1 }
        raise ArgumentError, "Ambiguous managed attachments on #{schema}.#{table}"
      end

      DriftDetector.new.detect(expected, managed, silent: true).tap do |changes|
        if changes[:extra].any?
          raise ArgumentError, "Unexpected managed attachments on #{schema}.#{table}"
        end
      end
    end

    def repair(schema, table)
      @connection.transaction do
        @connection.execute(
          "SET LOCAL statement_timeout = #{@connection.quote(@config.statement_timeout_ms)}",
        )
        @connection.execute(
          "LOCK TABLE #{@connection.quote_column_name(schema)}." \
          "#{@connection.quote_column_name(table)}",
        )
        unless tables(schema, table: table).include?(table)
          raise ArgumentError, "Repair target disappeared or changed: #{schema}.#{table}"
        end

        corrections = drift(schema, table)
        DriftDetector.new.log_drift(corrections)
        SqlExecutor.new(nil, connection: @connection).apply_corrections(corrections)
      end
    end
  end
end
