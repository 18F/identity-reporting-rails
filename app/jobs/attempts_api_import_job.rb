require 'net/http'
require 'json'
require 'jwt'
require 'jwe'

class AttemptsApiImportJob < ApplicationJob
  queue_as :default

  PRIVATE_KEY = JobHelpers::AttemptsApiKeypairHelper.private_key
  PUBLIC_KEY = JobHelpers::AttemptsApiKeypairHelper.public_key

  def perform
    log_info('AttemptsApiImportJob: Job started', true)

    begin
      response_data = fetch_api_data

      log_info("AttemptsApiImportJob: Processing #{response_data[:sets].size} events", true)

      import_to_redshift(response_data[:sets])

      # todo: this is for testing purposes. Job will be handled in job_configurations.rb
      FcmsPiiDecryptJob.perform_now(PRIVATE_KEY.to_pem)
    rescue => e
      log_info('AttemptsApiImportJob: Error during API attempt', false, { error: e.message })
      raise
    end

    log_info('AttemptsApiImportJob: Job completed', true)
  end

  private

  def fetch_api_data
    sets = 999.times.map do |i|
      event_type = "event-#{SecureRandom.hex(4)}-#{i}"
      encrypted_payload = encrypt_mock_jwt_payload(
        mock_jwt_payload(event_type: event_type),
        PUBLIC_KEY,
      )

      { event_type => encrypted_payload }
    end

    { sets: sets }.with_indifferent_access
  end

  def import_to_redshift(encrypted_payloads)
    return if encrypted_payloads.empty?

    values = encrypted_payloads.flat_map do |payload_hash|
      key_hash, encrypted_payload = payload_hash.first
      [key_hash, encrypted_payload]
    end

    placeholders = (['(?, ?, CURRENT_TIMESTAMP)'] * encrypted_payloads.size).join(', ')

    sql = <<~SQL
      INSERT INTO fcms.unextracted_events (key_hash, message, import_timestamp)
      VALUES #{placeholders}
    SQL

    DataWarehouseApplicationRecord.connection.execute(
      ActiveRecord::Base.sanitize_sql_array(
        [sql,
         *values],
      ),
    )

    log_info(
      'AttemptsApiImportJob: Data import to Redshift succeeded', true,
      { row_count: encrypted_payloads.size }
    )
  rescue ActiveRecord::StatementInvalid => e
    log_info('AttemptsApiImportJob: Data import to Redshift failed', false, { error: e.message })
    raise
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

  def encrypt_mock_jwt_payload(payload, public_key)
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
          'user_agent' => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36', # rubocop:disable Layout/LineLength
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
