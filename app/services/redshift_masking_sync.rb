# frozen_string_literal: true

require 'yaml'

# Service for syncing Redshift masking policies based on configuration files
class RedshiftMaskingSync
  DATA_CONTROLS_PATH = IdentityConfig.local_devops_path(
    :identity_devops,
    'bin/data-warehouse/mask.yaml',
  )
  USERS_YAML_PATH = IdentityConfig.local_devops_path(
    :identity_devops,
    'terraform/master/global/users.yaml',
  )

  def sync(user_filter: nil)
    Rails.logger.info('starting data controls sync')

    data_controls = YAML.safe_load(File.read(DATA_CONTROLS_PATH))
    users_yaml = YAML.safe_load(File.read(USERS_YAML_PATH))['users']

    sync_masking_policies(data_controls, users_yaml, user_filter: user_filter)
  end

  private

  def sync_masking_policies(data_controls, users_yaml, user_filter: nil)
    env_name = Identity::Hostdata.env

    config = RedshiftMasking::Configuration.new(
      data_controls, users_yaml,
      env_name: env_name
    )
    db_queries = RedshiftMasking::DatabaseQueries.new(Rails.logger)

    users = db_queries.fetch_users
    db_user_case_map = users.index_by { |u| u.upcase }
    Rails.logger.info("found #{db_user_case_map.size} database users")
    db_users_set = Set.new(db_user_case_map.keys)

    if user_filter
      filter_set = Set.new(user_filter.map(&:upcase))
      db_users_set &= filter_set
      Rails.logger.info("filtering sync to #{db_users_set.size} user(s)")
    end

    columns = extract_columns(config)
    column_types = db_queries.fetch_column_types(columns)

    user_resolver = RedshiftMasking::UserResolver.new(
      config, users_yaml, db_user_case_map,
      Rails.logger
    )
    policy_builder = RedshiftMasking::PolicyBuilder.new(config, user_resolver)
    drift_detector = RedshiftMasking::DriftDetector.new
    sql_executor = RedshiftMasking::SqlExecutor.new(config)

    sql_executor.create_masking_policies(column_types)

    expected = policy_builder.build_expected_state(column_types, db_users_set)
    actual = db_queries.fetch_existing_policies
    actual = actual.select { |p| filter_set.include?(p.grantee.upcase) } if user_filter

    Rails.logger.info(
      "expected: #{expected.size} attachments, actual: #{actual.size} attachments",
    )

    drift = drift_detector.detect(expected, actual, silent: user_filter.present?)
    sql_executor.apply_corrections(drift)

    Rails.logger.info('sync completed')
  end

  def extract_columns(config)
    config.columns_config.flat_map do |entry|
      entry.keys.filter_map { |id| RedshiftMasking::Column.parse(id) }
    end
  end
end
