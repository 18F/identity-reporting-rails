require 'pg_query'

class PiiAccessDetectionJob < ApplicationJob
  queue_as :default

  def perform
    enabled = IdentityConfig.store.fraud_ops_tracker_enabled
    unless enabled
      log_message(
        :info,
        "fraud_ops_tracker_enabled is #{enabled}, skipping job.",
        false,
      )
      return
    end

    queries_to_process = get_historical_queries
    potential_pii_queries = queries_to_process.select do |query_metadata|
      !authorized_system_users.include?(query_metadata[:user_name])
    end
    # Analyze each query for PII access
    potential_pii_queries.each do |query_metadata|
      query_relations = get_query_relations(query_metadata[:full_query])
      query_relations.each do |query_relation|
        pii_mappings.each do |schema, schema_relations|
          if schema_relations.include?(query_relation)
            log_message(
              :warn,
              'Potential PII access detected',
              false,
              user_name: query_metadata[:user_name],
              query_id: query_metadata[:query_id],
              table_accessed: query_relation,
            )
            end
        end
      end
    end
  end

  private

  def get_query_relations(query)
    node = PgQuery.parse(query)
    # node = PgQuery.parse(query).tree.stmts.first.stmt
    # result = Hash.new { |h, k| h[k] = [] }

    # return result unless node.is_a?(PgQuery::Node)

    # # Helper to recursively walk nodes
    # walker = lambda do |n|
    #     next unless n.is_a?(PgQuery::Node)

    #     # SELECT statements
    #     if n.select_stmt
    #     stmt = n.select_stmt

    #     # Handle FROM clause → find table references
    #     tables = Array(stmt.from_clause).flat_map do |f|
    #         f.range_var ? [f.range_var] : []
    #     end

    #     # Handle target list → find column references
    #     targets = Array(stmt.target_list).map(&:res_target)

    #     targets.each do |t|
    #         if t&.val&.column_ref
    #             column_ref = t.val.column_ref
    #             fields = column_ref.fields.map(&:string).compact
    #             if fields.size == 2
    #                 table_name, col_name = fields
    #                 result[table_name] << col_name
    #             elsif fields.size == 1
    #                 result[table_name] << fields.first
    #             elsif column_ref.a_star
    #                 # Handle SELECT *
    #                 tables.each { |tbl| result[tbl.relname ] << "*" }
    #             end
    #         end
    #     end

    #     # Handle nested subqueries (e.g., in FROM)
    #     Array(stmt.from_clause).each do |f|
    #         walker.call(f.lateral_subquery) if f.respond_to?(:lateral_subquery)
    #     end

    #     # Recursively walk any subnodes
    #     n.class.descriptor.each_field do |field|
    #         value = n[field.name]
    #         if value.is_a?(PgQuery::Node)
    #             walker.call(value)
    #         elsif value.is_a?(Array)
    #             value.each { |v| walker.call(v) if v.is_a?(PgQuery::Node) }
    #         end
    #     end
    # end

    # walker.call(node)
    # remove schema prefix if exists
    node.select_tables.map { |table| table.split('.').last }
  end

  def pii_mappings
    # {
    #     'idp': {
    #         'email_addresses': ['email']
    #         # TODO: how to distinguish email if * is used, maybe if not authorized & part of
    #         # black listed user groups (lg_users, lg_powerusers), check SVV_COLUMN_PRIVILEGES
    #         # & SVV_RELATION_PRIVILEGES to confirm access is blocked, if not, raise alert
    #     },
    #     'fcms': {
    #         'encrypted_idv_events': ['*'],
    #         'fraud_ops_events': ['*']
    #     }
    # }

    # This is assumming that tables are classified as PII at the schema/table level
    # rather than column level
    {
      # schema_name: [table_name1, table_name2...]
      idp_pii: [
        'email_addresses_pii',
      ],
      fraudops: [
        'encrypted_events',
        'fraud_ops_events',
      ],
      system_tables: [
        'stl_query',
        'svl_qlog',
        'stv_recents',
      ],
    }
  end

  # def authorized_user_groups
  #     ['lg_admins']
  # end

  def authorized_system_users
    env_name = Identity::Hostdata.env
    [
      "IAMR:#{env_name}_dbt_connector",
      "IAMR:#{env_name}_fraud_ops_connector",
      "IAMR:#{env_name}_rails_job_connector",
      "IAMR:#{env_name}_db_consumption",
    ]
  end

  # def get_user_memberships(user_group)
  #     current_group_users_statement = <<~SQL
  #         SELECT usename FROM pg_user, pg_group
  #         WHERE pg_user.usesysid = ANY(pg_group.grolist)
  #         and pg_group.groname = '#{user_group}';
  #     SQL
  #     results = connection.execute(current_group_users_statement).to_a
  #     results.map { |row| row['usename'] }
  # end

  def get_historical_queries
    # Fetch queries from the last 15 minutes
    if using_redshift_adapter
      list_agg_function = "LISTAGG(text, ' ') WITHIN GROUP (ORDER BY sequence)"
    else
      list_agg_function = "STRING_AGG(text, ' ' ORDER BY sequence)"
    end
    historical_queries_statement = <<~SQL
      SELECT
      A.user_id,
      B.user_name,
      A.query_id,
      #{list_agg_function} AS full_query
      FROM sys_query_text A
      LEFT JOIN svv_user_info B ON A.user_id = B.user_id
      -- WHERE DATEADD(m, 15, start_time) >= CURRENT_TIMESTAMP
      WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL '15 minutes'
      GROUP BY 1,2,3;
    SQL
    results = connection.execute(
      historical_queries_statement,
    ).to_a
    # Create list of hashes with user_id, query_id, full_query
    results.map do |row|
      {
        user_id: row['user_id'],
        user_name: row['user_name'],
        query_id: row['query_id'],
        full_query: row['full_query'],
      }
    end
  end

  def using_redshift_adapter
    connection.adapter_name.downcase.include?('redshift')
  end

  def connection
    DataWarehouseApplicationRecord.connection
  end
end
