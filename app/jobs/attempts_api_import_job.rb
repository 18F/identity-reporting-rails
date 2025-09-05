require 'net/http'
require 'json'
require 'jwt'
require 'jwe'

class AttemptsApiImportJob < ApplicationJob
  queue_as :default

  SCHEMA_NAME = 'fcms'
  TABLE_NAME = 'unextracted_events'
  PRIVATE_KEY = JobHelpers::AttemptsApiKeypairHelper.private_key
  PUBLIC_KEY = JobHelpers::AttemptsApiKeypairHelper.public_key

  def perform
    log_info('AttemptsApiImportJob: Job started', true)

    @schema_name = SCHEMA_NAME
    @table_name = TABLE_NAME

    begin
      response_data = fetch_api_data
      log_info('AttemptsApiImportJob: Processing event', true, { response_data: response_data })
      import_to_redshift(response_data[:sets])
    rescue => e
      log_info('AttemptsApiImportJob: Error during API attempt', false, { error: e.message })
      raise e
    end
    log_info('AttemptsApiImportJob: Job completed', true)
  end

  private

  def fetch_api_data
    # loop through 999 mock events and encrypt them
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
    import_timestamp = Time.zone.now.utc.strftime('%Y-%m-%d %H:%M:%S')
    columns = %w[key_hash message import_timestamp]
    insert_columns = columns.join(', ')
    values = encrypted_payloads.map do |payload_hash|
      [
        payload_hash.keys.first,
        payload_hash.values.first,
        import_timestamp,
      ]
    end
    values_sql = values.map do |row|
      "(#{row.map do |v|
        ActiveRecord::Base.connection.quote(v)
      end.join(', ')})"
    end.join(",\n")

    build_params = {
      schema_name: @schema_name,
      table_name: @table_name,
      insert_columns: insert_columns,
      values_sql: values_sql,
    }

    insert_query = format(<<~SQL.squish, build_params)
      INSERT INTO %{schema_name}.%{table_name} (%{insert_columns})
      VALUES
      %{values_sql}
      ;
    SQL

    begin
      DataWarehouseApplicationRecord.connection.execute(insert_query)
      log_info(
        'AttemptsApiImportJob: Data imported to Redshift', true,
        { row_count: encrypted_payloads.size }
      )
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
