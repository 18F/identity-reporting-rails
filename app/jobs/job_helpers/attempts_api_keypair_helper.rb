module JobHelpers
  class AttemptsApiKeypairHelper
    require 'openssl'
    def self.public_key
      private_key.public_key
    end

    def self.private_key
      @private_key ||= OpenSSL::PKey::RSA.new(4096)
    end
  end
end
