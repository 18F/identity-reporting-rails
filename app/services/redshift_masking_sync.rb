# frozen_string_literal: true

require 'yaml'

# Service for syncing Redshift masking policies based on configuration files
class RedshiftMaskingSync
  DATA_CONTROLS_PATH = IdentityConfig.local_devops_path(
    :identity_devops,
    'bin/data-warehouse/mask.yaml',
  )

  def sync(user_filter: nil)
    Rails.logger.info('starting data controls sync')

    data_controls = YAML.safe_load(File.read(DATA_CONTROLS_PATH))
    roles_yaml = YAML.safe_load(
      File.read(Rails.root.join('config/redshift_config.yaml')),
    )['cluster']['user_roles']
    users_yaml = YAML.safe_load(File.read(IdentityConfig.identity_devops_users_yaml_path))['users']

    sync_masking_policies(data_controls, users_yaml, roles_yaml, user_filter: user_filter)
  end

  private

  def sync_masking_policies(data_controls, users_yaml, roles_yaml, user_filter: nil)
    masking_worker_config = Array(roles_yaml).find do |role|
      role && role['role_name'] == 'masking_worker'
    end

    configured_grantors =
      if masking_worker_config &&
         feature_enabled?(masking_worker_config['feature_flag'])
        masking_worker_config.fetch('member_users', [])
      else
        []
      end

    protected_grantors = Set.new(configured_grantors.map(&:upcase))

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

    drift = drift_detector.detect(expected, actual, silent: true)
    exclude_protected_attachments(drift, protected_grantors)
    drift_detector.log_drift(drift) unless user_filter.present?
    sql_executor.apply_corrections(drift)

    Rails.logger.info('sync completed')
  end

  def exclude_protected_attachments(drift, protected_grantors)
    excluded_count = 0

    drift[:extra].reject! do |attachment|
      excluded = protected_grantors.include?(attachment.grantor&.upcase)
      log_preserved_attachment(attachment) if excluded
      excluded_count += 1 if excluded
      excluded
    end

    drift[:mismatched].reject! do |mismatch|
      excluded = protected_grantors.include?(mismatch[:actual].grantor&.upcase)
      log_preserved_attachment(mismatch[:actual]) if excluded
      excluded_count += 1 if excluded
      excluded
    end

    Rails.logger.info(
      "Skipping #{excluded_count} masking correction(s): existing attachments were " \
      "created by configured masking_worker grantors " \
      "[#{protected_grantors.to_a.sort.join(', ')}]. " \
      "These attachments will not be detached or replaced.",
    )
  end

  def log_preserved_attachment(attachment)
    Rails.logger.info(
      "Preserving dbt-managed masking attachment: policy=#{attachment.policy_name} " \
      "column=#{attachment.column_id} grantee=#{attachment.grantee} " \
      "grantor=#{attachment.grantor} reason=protected_masking_worker_grantor",
    )
  end

  def extract_columns(config)
    config.columns_config.flat_map do |entry|
      entry.keys.filter_map { |id| RedshiftMasking::Column.parse(id) }
    end
  end

  def feature_enabled?(feature_flag)
    return true if feature_flag.nil?

    flags_to_check = feature_flag.is_a?(Array) ? feature_flag : [feature_flag]

    flags_to_check.any? do |flag|
      # Check both Terraform config and IdentityConfig.store; flag is enabled if either is true
      config_file.match?(/^\s*(?!#|\/\/)#{flag}\s+=\s+true/m) ||
        (IdentityConfig.store.respond_to?(flag) && IdentityConfig.store.public_send(flag) == true)
    end
  end

  def config_file
    @config_file ||= begin
      terraform_config_path = IdentityConfig.local_devops_path(
        :identity_devops,
        "terraform/data-warehouse/#{env_name}/main.tf",
      )
      File.read(terraform_config_path)
    end
  end
end
