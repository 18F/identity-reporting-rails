require 'net/http'
require 'json'
require 'jwt'
require 'jwe'

class AttemptsApiImportJob < ApplicationJob
  queue_as :default

  # API_URL = 'https://localhost:3000/attemptsapi'
  # HEADERS = { 'Content-Type' => 'application/json' }
  TABLE_NAME = 'fcms.unextracted_events'
  PRIVATE_KEY = JobHelpers::AttemptsApiKeypairHelper.private_key
  PUBLIC_KEY = JobHelpers::AttemptsApiKeypairHelper.public_key

  def perform
    log_info('AttemptsApiImportJob: Job started', true)

    begin
      response_data = fetch_api_data
      # response_data[:sets].each do |event|
      log_info('AttemptsApiImportJob: Processing event', true, { response_data: response_data })
      import_to_redshift(response_data[:sets])
      # end
      # decrypted_response = decrypt_jwt(response_data, PRIVATE_KEY)
    rescue => e
      log_info('AttemptsApiImportJob: Error during API attempt', false, { error: e.message })
      raise e
    end

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
    # rescue => e
    #   log_info('AttemptsApiImportJob: Error during API attempt', false, { error: e.message })
    # end
    # sleep 5
    # end
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
  #

  private

  def fetch_api_data
    # loop through 3 mock events and encrypt them
    # return as array of encrypted events
    event_key = 999.times.map { |i| "event-#{SecureRandom.hex(4)}-#{i}" }

    sets = event_key.map do |event_type|
      { event_type => encrypt_mock_jwt_payload(
        mock_jwt_payload(event_type: event_type),
        PUBLIC_KEY,
      ) }
    end

    {
      sets: sets,
    }.with_indifferent_access
  end

  def import_to_redshift(encrypted_payloads)
    binding.pry
    # Here you would implement the logic to import the event_json data into Redshift
    # hash_payload = { payload: encrypted_payload }
    # log_info('AttemptsApiImportJob: Data imported to Redshift', true, hash_payload)
    # message = event_json.to_json.gsub("'", "''") # Escape single quotes for SQL
    import_timestamp = Time.zone.now.utc.strftime('%Y-%m-%d %H:%M:%S')
    values_sql = encrypted_payloads.map do |payload_hash|
      key_hash = payload_hash.keys.first
      encrypted_payload = payload_hash.values.first
      "('#{key_hash.gsub(
        "'",
        "''",
      )}', '#{encrypted_payload.gsub("'", "''")}', '#{import_timestamp}')"
    end.join(",\n")

    sql = <<-SQL
      INSERT INTO #{TABLE_NAME} (key_hash, message, import_timestamp)
      VALUES #{values_sql}
    SQL

    begin
      DataWarehouseApplicationRecord.connection.execute(sql)
      log_info('AttemptsApiImportJob: Data import to Redshift succeeded', true, hash_payload)
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

  # todo: move to the other decrypting job
  def decrypt_jwt(encrypted_jwt, public_key)
    decrypted_jwt = JWE.decrypt(encrypted_jwt, public_key)
    JSON.parse(decrypted_jwt)
  end

  def encrypt_mock_jwt_payload(payload, private_key)
    jwk = JWT::JWK.new(private_key)
    JWE.encrypt(
      payload.to_json,
      private_key,
      typ: 'secevent+jwe',
      zip: 'DEF',
      alg: 'RSA-OAEP',
      enc: 'A256GCM',
      kid: jwk.kid,
    )
  end

  def mock_jwt_payload(event_type:)
    current_time = Time.current

    {
      'jti' => SecureRandom.uuid,
      'iat' => current_time.to_i,
      'iss' => 'http://localhost:3000/',
      'aud' => 'urn:gov:gsa:openidconnect:sp:sinatra',
      'events' => {
        "https://schemas.login.gov/secevent/attempts-api/event-type/#{event_type}" => {
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
          'user_id' => rand(1..1000),
        },
      },
    }.with_indifferent_access
  end
end
