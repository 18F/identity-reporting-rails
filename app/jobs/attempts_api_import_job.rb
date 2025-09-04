require 'net/http'
require 'json'

class AttemptsApiImportJob < ApplicationJob
  queue_as :default

  API_URL = 'https://localhost:3000/attemptsapi'
  # HEADERS = { 'Content-Type' => 'application/json' }
  TABLE_NAME = 'fcms.unextracted_events'

  def perform
    log_info('AttemptsApiImportJob: Job started', true)
    loop do
      begin
        key_pair = create_key_pair
        encrypted_jwt_payload = encrypt_jwt(mock_jwt_payload, key_pair[:public_key])
        import_to_redshift(encrypted_jwt_payload)

        # Call the API request service
        # Attempts::ApiRequestService.new.call
        # mock api call
        # sleep(1)
        # mock api response
        # response = Net::HTTP.get_response(URI(API_URL))
        # if response.is_a?(Net::HTTPSuccess)
        #   events = response.body
        #   log_info('AttemptsApiImportJob: API call succeeded', true, events)
        #   import_to_redshift(events)
        # else
        #   log_info('AttemptsApiImportJob: API call failed', false, response.body)
        # end
      rescue => e
        log_info('AttemptsApiImportJob: Error during API attempt', false, { error: e.message })
      end
      sleep 5
    end
    log_info('AttemptsApiImportJob: Job completed', true)
  end

  # mock api response
  # response = { success: true, data: { message: 'Mock API call successful' } }
  # Rails.logger.info("AttemptsApiImportJob: Mock API call response: #{response.to_json}")
  # if response[:success]
  #   log_info('AttemptsApiImportJob: API call succeeded', true, response[:data])
  # else
  #   log_info('AttemptsApiImportJob: API call failed', false, response[:data])
  # end

  private

  def import_to_redshift(encrypted_payload)
    begin
      # Here you would implement the logic to import the event_json data into Redshift
      log_info('AttemptsApiImportJob: Data imported to Redshift', true, encrypted_payload)
      # message = event_json.to_json.gsub("'", "''") # Escape single quotes for SQL
      import_timestamp = Time.zone.now.utc.strftime('%Y-%m-%d %H:%M:%S')
      sql = <<-SQL
        INSERT INTO #{TABLE_NAME} (encrypted_payload, import_timestamp)
        VALUES ('#{message}', '#{import_timestamp}')
      SQL
      DataWarehouseApplicationRecord.connection.execute(sql)
      log_info('AttemptsApiImportJob: Data import to Redshift succeeded', true, encrypted_payload)
    rescue => e
      log_info('AttemptsApiImportJob: Data import to Redshift failed', false, { error: e.message })
    end
  end

  def log_info(message, success, additional_info = {})
    Rails.logger.info(
      {
        job: self.class.name,
        success: success,
        message: message,
      }.merge(additional_info).to_json,
    )
  end

  def create_key_pair
    rsa_key = OpenSSL::PKey::RSA.new(2048)
    { private_key: rsa_key.to_pem, public_key: rsa_key.public_key.to_pem }
  end

  def encrypt_jwt(payload, public_key)
    jwk = JWT::JWK.new(public_key)
    JWE.encrypt(
      payload.to_json,
      public_key,
      typ: 'secevent+jwe',
      zip: 'DEF',
      alg: 'RSA-OAEP',
      enc: 'A256GCM',
      kid: jwk.kid,
    )
  end

  def mock_jwt_payload
    current_time = Time.current

    {
      'jti' => SecureRandom.uuid,
      'iat' => current_time.to_i,
      'iss' => 'http://localhost:3000/',
      'aud' => 'urn:gov:gsa:openidconnect:sp:sinatra',
      'events' => {
        'https://schemas.login.gov/secevent/attempts-api/event-type/mfa-login-auth-submitted' => {
          'subject' => {
            'subject_type' => 'session',
            'session_id' => SecureRandom.uuid,
          },
          'occurred_at' => current_time.to_f,
          'user_agent' => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
          'unique_session_id' => SecureRandom.alphanumeric(20),
          'user_uuid' => SecureRandom.uuid,
          'device_id' => SecureRandom.hex(64),
          'user_ip_address' => '::1',
          'application_url' => 'http://localhost:9292/auth/result',
          'language' => 'en',
          'client_port' => nil,
          'aws_region' => 'us-west-2',
          'google_analytics_cookies' => {},
          'mfa_device_type' => 'otp',
          'reauthentication' => false,
          'success' => true,
          'agency_uuid' => SecureRandom.uuid,
          'user_id' => 3,
        },
      },
    }.with_indifferent_access
  end
end
