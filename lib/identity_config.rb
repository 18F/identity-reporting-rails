class IdentityConfig
  GIT_SHA = `git rev-parse --short=8 HEAD`.chomp
  GIT_TAG = `git tag --points-at HEAD`.chomp.split("\n").first
  GIT_BRANCH = `git rev-parse --abbrev-ref HEAD`.chomp
  REPO_PATHS = {
    identity_devops: '/etc/login.gov/repos/identity-devops',
    user_sync_identity_devops: '/usersync/identity-devops',
  }

  # Shorthand to allow using old syntax to access configs, minimizes merge conflicts
  # while migrating to newer syntax
  def self.store
    Identity::Hostdata.config
  end

  # rubocop:disable Metrics/BlockLength
  CONFIG_BUILDER = proc do |config|
    #  ______________________________________
    # / Adding something new in here? Please \
    # \ keep methods sorted alphabetically.  /
    #  --------------------------------------
    #                                   /
    #           _.---._    /\\         /
    #        ./'       "--`\//        /
    #      ./              o \       /
    #     /./\  )______   \__ \
    #    ./  / /\ \   | \ \  \ \
    #       / /  \ \  | |\ \  \7
    #        "     "    "  "
    config.add(:domain_name, type: :string)
    config.add(:database_host, type: :string)
    config.add(:database_name, type: :string)
    config.add(:database_password, type: :string)
    config.add(:database_pool_reporting, type: :integer)
    config.add(:database_read_replica_host, type: :string)
    config.add(:database_readonly_password, type: :string)
    config.add(:database_readonly_username, type: :string)
    config.add(:database_socket, type: :string)
    config.add(:database_sslmode, type: :string)
    config.add(:database_statement_timeout, type: :integer)
    config.add(:database_timeout, type: :integer)
    config.add(:database_username, type: :string)
    config.add(:database_worker_jobs_host, type: :string)
    config.add(:database_worker_jobs_name, type: :string)
    config.add(:database_worker_jobs_password, type: :string)
    config.add(:database_worker_jobs_sslmode, type: :string)
    config.add(:database_worker_jobs_username, type: :string)
    config.add(:good_job_max_threads, type: :integer)
    config.add(:good_job_queue_select_limit, type: :integer)
    config.add(:good_job_queues, type: :string)
    config.add(:rack_mini_profiler, type: :boolean)
    config.add(:redshift_database_name, type: :string)
    config.add(:redshift_host, type: :string)
    config.add(:data_freshness_threshold_hours, type: :integer)
    config.add(:unload_line_count_threshold, type: :integer)
    config.add(:s3_report_public_bucket_prefix, type: :string)
    config.add(:report_timeout, type: :integer)
    config.add(:s3_reports_enabled, type: :boolean)
    config.add(:s3_report_bucket_prefix, type: :string)
    config.add(:redshift_sia_v3_enabled, type: :boolean)

    "redshift/#{Identity::Hostdata.env || 'local'}-analytics-superuser".
      then do |redshift_secrets_manager_key|
        config.add(
          :redshift_password,
          secrets_manager_name: redshift_secrets_manager_key,
          type: :string,
        ) { |raw| JSON.parse(raw).fetch('password') }
        config.add(
          :redshift_username,
          secrets_manager_name: redshift_secrets_manager_key,
          type: :string,
        ) { |raw| JSON.parse(raw).fetch('username') }
        config.add(:secret_key_base, type: :string)
      end
  end.freeze
  # rubocop:enable Metrics/BlockLength

  def self.local_devops_path(devops_dir, relative_path)
    root_dir = REPO_PATHS[devops_dir]
    File.join(root_dir, relative_path)
  end
end
