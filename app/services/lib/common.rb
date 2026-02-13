# frozen_string_literal: true

require 'yaml'
require 'aws-sdk-redshiftdataapiservice'
require 'aws-sdk-secretsmanager'
require 'logger'
require 'optparse'
require 'digest'
require 'json'

# Common utilities for Redshift management scripts
module RedshiftCommon

  # Configuration for the Redshift connection
  class Config
    LOGIN_BASE_PATH = '/etc/login.gov'
    ENV_FILE_PATH = File.join(LOGIN_BASE_PATH, 'info', 'env')
    DEVOPS_REPO_BASE_PATH = File.join(LOGIN_BASE_PATH, 'repos', 'identity-devops')
    TERRAFORM_DW_PATH = File.join(DEVOPS_REPO_BASE_PATH, 'terraform', 'data-warehouse')

    attr_reader :env_name, :region, :cluster_suffix, :database, :cluster_identifier, :config_file_path

    def initialize(
      env_file: ENV_FILE_PATH,
      region: 'us-west-2',
      cluster_suffix: 'analytics',
      database: 'analytics'
    )
      @env_name = File.read(env_file).chomp
      @region = region
      @cluster_suffix = cluster_suffix
      @database = database

      @config_file_path = File.join(TERRAFORM_DW_PATH, env_name, 'main.tf')
      @cluster_identifier = "#{env_name}-#{cluster_suffix}"
    end

    def env_type
      %w[prod dm staging].include?(env_name) ? 'prod' : 'sandbox'
    end

    def config_file_contents
      @config_file_contents ||= File.read(config_file_path)
    end

    def feature_enabled?(feature_flag)
      feature_flag.nil? || config_file_contents.match?(%r{^\s*(?!#|//)#{feature_flag}\s+=\s+true}m)
    end
  end

  # Handles AWS credentials and clients
  class AwsClients
    def initialize(config)
      @config = config
    end

    def credentials
      @credentials ||= Aws::InstanceProfileCredentials.new
    end

    def secrets_manager
      @secrets_manager ||= Aws::SecretsManager::Client.new(region: @config.region)
    end

    def redshift_data
      @redshift_data ||= Aws::RedshiftDataAPIService::Client.new(credentials: credentials)
    end

    def secret_arn(secret_id)
      secrets_manager.describe_secret(secret_id: secret_id).arn
    end

    def secret_value(secret_id)
      JSON.parse(secrets_manager.get_secret_value(secret_id: secret_id)['secret_string'])
    end

    def redshift_password_hash(user_name, secret_id)
      password = secret_value(secret_id)['password']
      "'md5#{Digest::MD5.hexdigest(password + user_name)}'"
    end
  end

  # Executes queries against Redshift
  class QueryExecutor
    QUERY_END_STATES = %w[FINISHED ABORTED FAILED].freeze

    def initialize(config, aws_clients, logger)
      @config = config
      @aws = aws_clients
      @logger = logger
      @superuser_secret_arn = nil
    end

    def superuser_secret_arn
      @superuser_secret_arn ||= @aws.secret_arn("redshift/#{@config.env_name}-analytics-superuser")
    end

    def execute(sql, parameters=nil)
      params = {
        cluster_identifier: @config.cluster_identifier,
        database: @config.database,
        secret_arn: superuser_secret_arn,
        sql: sql,
      }
      params[:parameters] = parameters if parameters
      @aws.redshift_data.execute_statement(params)
    end

    def wait_for_completion(query_id)
      loop do
        sleep 1
        state = @aws.redshift_data.describe_statement(id: query_id)
        next unless QUERY_END_STATES.include?(state['status'])

        return true if state['status'] == 'FINISHED'

        raise "Redshift query failed: #{state['error']} | #{state['query_string']}"
      end
    end

    def execute_and_wait(sql, parameters=nil)
      query = execute(sql, parameters)
      wait_for_completion(query['id'])
      query['id']
    end

    def fetch_results(query_id)
      @aws.redshift_data
          .get_statement_result(id: query_id)
          .to_h[:records]
    end

    def fetch_single_column(query_id)
      fetch_results(query_id)
        .flatten
        .map { |record| record[:string_value] }
    end

    # Execute a query and return single-column results as an array
    def query_single_column(sql, parameters=nil)
      query = execute(sql, parameters)
      return [] unless wait_for_completion(query['id'])

      fetch_single_column(query['id'])
    end

    # Execute a query and return results as array of hashes
    def query_records(sql, parameters=nil, &block)
      query = execute(sql, parameters)
      return [] unless wait_for_completion(query['id'])

      records = fetch_results(query['id'])
      block ? records.map(&block) : records
    end
  end

  # SQL quoting utilities
  module SqlQuoting
    # Quote a value for SQL (string literal)
    def self.quote_value(val)
      if val.is_a?(Array)
        "(#{val.map { |v| quote_value(v) }.join(', ')})"
      else
        %('#{val}')
      end
    end

    # Quote an identifier (table name, column name, username)
    def self.quote_identifier(identifier)
      %("#{identifier}")
    end

    # Quote a grantee (PUBLIC is a keyword, shouldn't be quoted)
    def self.quote_grantee(grantee)
      grantee.upcase == 'PUBLIC' ? 'PUBLIC' : quote_identifier(grantee)
    end
  end

  # Data type normalization utilities
  module DataTypeUtils
    DATA_TYPE_MAPPINGS = {
      /^(?:character varying|varchar|text)/i => 'VARCHAR(MAX)',
      /^(?:character|char)$/i => ->(_, len) { "CHAR(#{len || 1})" }, # Lambda to preserve original length
      /^(?:numeric|decimal|integer|int|smallint|bigint|real|double)/i => 'NUMERIC',
      /^date$/i => 'DATE',
      /^timestamp/i => 'TIMESTAMP',
      /^(?:boolean|bool)/i => 'BOOLEAN',
    }.freeze

    # Normalize a Redshift data type to a standardized format
    def self.normalize_data_type(data_type, char_max_length=nil, logger: nil)
      DATA_TYPE_MAPPINGS.each do |pattern, result|
        next unless pattern.match?(data_type)

        return result.respond_to?(:call) ? result.call(data_type, char_max_length) : result
      end

      logger&.warn("RedshiftCommon::DataTypeUtils: unknown data type '#{data_type}', defaulting to VARCHAR(MAX)")
      'VARCHAR(MAX)'
    end
  end

  # IAM role resolution utilities
  module IamRoleUtils
    IAM_ROLE_GROUPS = {
      'dwuser' => %w[dwuser dwusernonprod],
      'dwpoweruser' => %w[dwpoweruser dwpowerusernonprod],
      'dwadmin' => %w[dwadmin dwadminnonprod],
    }.freeze

    # Resolve an IAM role name to the corresponding AWS groups
    def self.resolve_iam_groups(role_name)
      IAM_ROLE_GROUPS.fetch(role_name, [role_name])
    end
  end

  # Database user queries
  module UserQueries
    def self.fetch_users(executor)
      query = 'SELECT usename FROM pg_user'
      executor.query_single_column(query)
    end

    # TODO: use functions below in redshift_sync.rb
    def self.user_exists?(executor, username)
      query = 'SELECT 1 FROM pg_roles WHERE rolname ILIKE :username LIMIT 1'
      parameters = [{ name: 'username', value: username }]
      results = executor.query_single_column(query, parameters)
      results.any?
    end

    def self.group_exists?(executor, group_name)
      query = 'SELECT 1 FROM pg_roles WHERE rolname ILIKE :group_name AND rolcanlogin = false LIMIT 1'
      parameters = [{ name: 'group_name', value: group_name }]
      results = executor.query_single_column(query, parameters)
      results.any?
    end

    def self.group_members(executor, group_name)
      query = 'SELECT usename FROM pg_user, pg_group ' \
              'WHERE pg_user.usesysid = ANY(pg_group.grolist) ' \
              'AND pg_group.groname = :group_name'
      parameters = [{ name: 'group_name', value: group_name }]
      executor.query_single_column(query, parameters)
    end
  end

  # Input validation
  module Validation
    ALLOWED_USERNAME_PATTERN = /\A[A-Za-z.\-:]+\z/

    def self.valid_username?(username)
      ALLOWED_USERNAME_PATTERN.match?(username)
    end

    def self.validate_username!(username)
      raise "Invalid username: #{username}" unless valid_username?(username)
    end
  end

  # Base class for Redshift management scripts
  class BaseScript
    attr_reader :config, :aws, :executor, :logger

    def initialize(logger: nil)
      @logger = logger || create_logger
      @config = Config.new
      @aws = AwsClients.new(@config)
      @executor = QueryExecutor.new(@config, @aws, @logger)
    end

    def run
      raise NotImplementedError, 'Subclasses must implement #run'
    end

    def log_info(message)
      @logger.info("#{script_name}: #{message}")
    end

    def log_warn(message)
      @logger.warn("#{script_name}: #{message}")
    end

    def log_error(message)
      @logger.error("#{script_name}: #{message}")
    end

    protected

    def script_name
      File.basename($PROGRAM_NAME)
    end

    def create_logger
      logger = Logger.new($stdout)
      logger.level = Logger::INFO
      logger
    end

    def load_yaml(file_path)
      log_info("loading YAML from #{file_path}")
      YAML.safe_load(File.read(file_path))
    end

    def parse_args(banner, required_count)
      optparse = OptionParser.new do |opts|
        opts.banner = banner
        yield opts if block_given?
      end

      args = optparse.parse!

      unless args.length == required_count
        $stderr.puts optparse
        exit 1
      end

      args
    end
  end
end
