class IdentityConfig
  GIT_SHA = `git rev-parse --short=8 HEAD`.chomp
  GIT_TAG = `git tag --points-at HEAD`.chomp.split("\n").first
  GIT_BRANCH = `git rev-parse --abbrev-ref HEAD`.chomp

  class << self
    attr_reader :store, :key_types, :unused_keys
  end

  CONVERTERS = {
    # Allows loading a string configuration from a system environment variable
    # ex: To read DATABASE_HOST from system environment for the database_host key
    # database_host: ['env', 'DATABASE_HOST']
    # To use a string value directly, you can specify a string explicitly:
    # database_host: 'localhost'
    string: proc do |value|
      if value.is_a?(Array) && value.length == 2 && value.first == 'env'
        ENV.fetch(value[1])
      elsif value.is_a?(String)
        value
      else
        raise 'invalid system environment configuration value'
      end
    end,
    symbol: proc { |value| value.to_sym },
    comma_separated_string_list: proc do |value|
      value.split(',')
    end,
    integer: proc do |value|
      Integer(value)
    end,
    float: proc do |value|
      Float(value)
    end,
    json: proc do |value, options: {}|
      JSON.parse(value, symbolize_names: options[:symbolize_names])
    end,
    boolean: proc do |value|
      case value
      when 'true', true
        true
      when 'false', false
        false
      else
        raise 'invalid boolean value'
      end
    end,
    date: proc { |value| Date.parse(value) if value },
    timestamp: proc do |value|
      # When the store is built `Time.zone` is not set resulting in a NoMethodError
      # if Time.zone.parse is called
      #
      # rubocop:disable Rails/TimeZone
      Time.parse(value)
      # rubocop:enable Rails/TimeZone
    end,
  }

  attr_reader :key_types

  def initialize(read_env)
    @read_env = read_env
    @written_env = {}
    @key_types = {}
  end

  def add(key, type: :string, allow_nil: false, enum: nil, options: {})
    value = @read_env[key]

    @key_types[key] = type

    converted_value = CONVERTERS.fetch(type).call(value, options: options) if !value.nil?
    raise "#{key} is required but is not present" if converted_value.nil? && !allow_nil
    if enum && !(enum.include?(converted_value) || (converted_value.nil? && allow_nil))
      raise "unexpected #{key}: #{value}, expected one of #{enum}"
    end

    @written_env[key] = converted_value
    @written_env
  end

  attr_reader :written_env

  def self.build_store(config_map)
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

    config = IdentityConfig.new(config_map)
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
    config.add(:secret_key_base, type: :string)

    @key_types = config.key_types
    @unused_keys = config_map.keys - config.written_env.keys
    @store = RedactedStruct.new('IdentityConfig', *config.written_env.keys, keyword_init: true).
      new(**config.written_env)
  end
end
