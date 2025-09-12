module JobHelpers
  class LogHelper
    def self.log_success(message, **additional_info)
      log_structured(message, success: true, **additional_info)
    end

    def self.log_error(message, **additional_info)
      log_structured(message, success: false, **additional_info)
    end

    def self.log_info(message, **additional_info)
      log_structured(message, **additional_info)
    end

    def self.log_structured(message, **additional_info)
      log_data = {
        job: self.class.name,
        message: message,
      }.merge(additional_info)

      Rails.logger.info(log_data.to_json)
    end
  end
end
