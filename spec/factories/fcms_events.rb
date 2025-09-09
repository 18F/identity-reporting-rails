FactoryBot.define do
  factory :fcms_event do
    jti { SecureRandom.uuid }
    message { Faker::Book.title }
    import_timestamp { Time.zone.now }
  end
end
