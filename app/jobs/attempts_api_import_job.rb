require 'net/http'
require 'json'
require 'jwt'

PUBLIC_KEY = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAuUMLV31VkBf+UkFG/gyL\nPbjUpsGYF6OoXUu9IsF3/fUNMdELO0yTQP/79iPHr0HlTcDVK3NhMostHY0FK7b7\nrg/SeSLoxIBLQth+NuYK8b8VVyOZv3S4TU0XJQu5ihi0LlyjOsNNmtnEjhK7G56N\nNIfPW65eghhF2rMNhHJqDgTMYPj2jwyx1Iz/t25BlL5mexVPixom8Suht22np7FQ\nJUlOsdesTww8yRlik8uODkb1QCuBFR7pYAErtL9RE3fsj28QRxRr1+bXeQa3FtHf\nY/8rYuTJVkL630hvUBXDTEv26FhkG+cI3jQJ0HLiYfacO5v4+btaCkIp2z4LjL8X\n6wIDAQAB\n-----END PUBLIC KEY-----\n"
PRIVATE_KEY = "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEAuUMLV31VkBf+UkFG/gyLPbjUpsGYF6OoXUu9IsF3/fUNMdEL\nO0yTQP/79iPHr0HlTcDVK3NhMostHY0FK7b7rg/SeSLoxIBLQth+NuYK8b8VVyOZ\nv3S4TU0XJQu5ihi0LlyjOsNNmtnEjhK7G56NNIfPW65eghhF2rMNhHJqDgTMYPj2\njwyx1Iz/t25BlL5mexVPixom8Suht22np7FQJUlOsdesTww8yRlik8uODkb1QCuB\nFR7pYAErtL9RE3fsj28QRxRr1+bXeQa3FtHfY/8rYuTJVkL630hvUBXDTEv26Fhk\nG+cI3jQJ0HLiYfacO5v4+btaCkIp2z4LjL8X6wIDAQABAoIBADO9TKqXf1Dp0oYo\nYupKmIyZVRJpWM+pTjkYEO9m9tr/GDtEdY2PGkT5+OVKIteIYMDxTNeAtrGF+wiY\nvMvzo6B+HLrmC/ntFpfJnJ46au4O/qfCcorszxgPopofBydRNOkJyDB+IOtRTDqd\ncpIJGsiD8V5aeVx96OxmOy01Qvc812uEHbcGwiKT1KMiStvoc40t86mvXZFy+pxi\nALZbS9uCaacr3zIAqHe/yTUkiuiVENhtEwbm9oO7yLrpvYar/h+WTIe7zaWRcy1e\nwei8CNG3kLwInMpcB6zYT5mDAiny0xarP/V82sg+OWL4xm1hh0LN+ptV0C5neFYr\n5KBtBLkCgYEA85T59aw5Yyal7iwjeBK14fKfePkWSodN+FcYUGz4HfTaMcQVmyOg\nkgllDSobDdvfCiMLq2p4Z1LAr9jw2LNGaVhoYMEbhpfrN3qG8vgj9RT/5gubeWL1\npQytuuGmppptLwD5Ru8u0j0twaNd/PuR30DTlJ4V5FrD7+/Cvos6HPcCgYEAwrTs\npkqrrjfh9dnNCDjLTh2bt3Dds5a2vE8w484zK/fBfiuvJt1nhTPgA9Q3XE4ZoOEJ\nzJNVqJqDoLtkgBAOT4ooWaTg3GQflqrfgG5SI+jcPd0X0Mhy+SJQrjlGmNBnTifu\nBacQhaq7NMJ8ctiYnsN2MzCoubM/FF7QOzlqY60CgYBTqM41F8LaEBMbe2NvQRXh\nFcC5/usuC8y2x8sdDGAngcpTH2LAVvs2TS282MJT/zlatPC0HixeaGivvNXzx5ce\nZXPsD0cR8imic13YI2vOKDk/3Kq4hUmTN4iP0CK5w/5OD4qOV2YAZWzKvf5w/kJ6\nqDxDJgyk0pvLU9DSOuVpvwKBgQCgFhms8CAP8ip69US6ydd+tqFdRhNCoVxFn+bW\npqc0M1SH5GryTX/b6Tb6bvXFkwFHYT+pUEpRghlPgkOzd4AMrc9XRVUMX9YJDx0M\nf6hRlhffVXVLWEQPysMDPFxrMI5/mBz/0Mio3iNl0bJ4ytVRU+xPUmanqFTo7rvO\ntfpfUQKBgFa2toGR4ByxUjfkvSKDOCpGuiJHqjs+7LPntYvEtN6+3ffTyhaw1uRR\n8XJ/ekWsmBSYX72CZofqSpsLaBEIeQ1bKaH96mmd7CLXvyPDFVs7jLPfg8xo+EzZ\nMltml2puTb9hCbsBmbtRnOhI0UD06YkcrYHEtjvTA1dbqJmH6mMz\n-----END RSA PRIVATE KEY-----\n"

class AttemptsApiImportJob < ApplicationJob
  queue_as :default

  # API_URL = 'https://localhost:3000/attemptsapi'
  # HEADERS = { 'Content-Type' => 'application/json' }
  TABLE_NAME = 'fcms.unextracted_events'

  def perform
    log_info('AttemptsApiImportJob: Job started', true)
    loop do
      # begin
      public_key = IdentityConfig::Hostdata.config.api_public_key
      encrypted_jwt_payload = encrypt_jwt(mock_jwt_payload, public_key)
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
      # rescue => e
      #   log_info('AttemptsApiImportJob: Error during API attempt', false, { error: e.message })
      # end
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
