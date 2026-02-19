# frozen_string_literal: true

require 'yaml'

class RedshiftMaskingJob < ApplicationJob
  queue_as :default

  DATA_CONTROLS_PATH = IdentityConfig.local_devops_path(
    :identity_devops,
    'bin/data-warehouse/mask.yaml',
  )
  USERS_YAML_PATH = IdentityConfig.local_devops_path(
    :identity_devops,
    'terraform/master/global/users.yaml',
  )

  def perform(user_filter: nil)
    require Rails.root.join('lib/common')

    unless job_enabled?
      log_message(:info, 'RedshiftMasking job is disabled, skipping', true)
      return
    end

    data_controls = YAML.safe_load(File.read(DATA_CONTROLS_PATH))
    users_yaml = YAML.safe_load(File.read(USERS_YAML_PATH))['users']

    sync_masking_policies(data_controls, users_yaml, user_filter: user_filter)
  end

  private

  def job_enabled?
    IdentityConfig.store.fraud_ops_tracker_enabled
  end

  def sync_masking_policies(data_controls, users_yaml, user_filter: nil)
    log_message(:info, 'starting data controls sync', true)

    redshift_config = RedshiftCommon::Config.new
    aws = RedshiftCommon::AwsClients.new(redshift_config)
    executor = RedshiftCommon::QueryExecutor.new(redshift_config, aws, logger_adapter)

    config = RedshiftMasking::Configuration.new(
      data_controls, users_yaml,
      env_name: redshift_config.env_name
    )
    db_queries = RedshiftMasking::DatabaseQueries.new(executor, logger_adapter)

    users = RedshiftCommon::UserQueries.fetch_users(executor)
    db_user_case_map = users.index_by { |u| u.upcase }
    log_message(:info, "found #{db_user_case_map.size} database users", true)
    db_users_set = Set.new(db_user_case_map.keys)

    if user_filter
      filter_set = Set.new(user_filter.map(&:upcase))
      db_users_set &= filter_set
      log_message(:info, "filtering sync to #{db_users_set.size} user(s)", true)
    end

    columns = extract_columns(config)
    column_types = db_queries.fetch_column_types(columns)

    user_resolver = RedshiftMasking::UserResolver.new(
      config, users_yaml, db_user_case_map,
      logger_adapter
    )
    policy_builder = RedshiftMasking::PolicyBuilder.new(config, user_resolver, logger_adapter)
    drift_detector = RedshiftMasking::DriftDetector.new(logger_adapter)
    sql_executor = RedshiftMasking::SqlExecutor.new(executor, config, logger_adapter)

    sql_executor.create_masking_policies(column_types)

    expected = policy_builder.build_expected_state(column_types, db_users_set)
    actual = db_queries.fetch_existing_policies
    actual = actual.select { |p| filter_set.include?(p.grantee.upcase) } if user_filter

    log_message(
      :info,
      "expected: #{expected.size} attachments, actual: #{actual.size} attachments", true
    )

    drift = drift_detector.detect(expected, actual, silent: user_filter.present?)
    sql_executor.apply_corrections(drift)

    log_message(:info, 'sync completed', true)
  end

  def extract_columns(config)
    config.columns_config.flat_map do |entry|
      entry.keys.filter_map { |id| RedshiftMasking::Column.parse(id) }
    end
  end

  def logger_adapter
    @logger_adapter ||= Object.new.tap do |obj|
      obj.define_singleton_method(:log_info)  { |msg| Rails.logger.info(msg) }
      obj.define_singleton_method(:log_warn)  { |msg| Rails.logger.warn(msg) }
      obj.define_singleton_method(:log_debug) { |msg| Rails.logger.debug(msg) }
    end
  end
end
