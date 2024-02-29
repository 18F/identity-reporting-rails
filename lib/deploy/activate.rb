require 'active_support/core_ext/hash/deep_merge'
require 'logger'
require 'identity/hostdata'
require 'yaml'

module Deploy
  class Activate
    attr_reader :logger, :s3_client

    def initialize(
      logger: default_logger,
      s3_client: nil,
      root: nil
    )
      @logger = logger
      @s3_client = s3_client
      @root = root
    end

    def run
    end

    def root
      @root || File.expand_path('../../../', __FILE__)
    end

    private

    def secrets_s3
      @secrets_s3 ||= Identity::Hostdata.secrets_s3(s3_client: s3_client, logger: logger)
    end

    def default_logger
      logger = Logger.new(STDOUT)
      logger.progname = 'deploy/activate'
      logger
    end
  end
end
