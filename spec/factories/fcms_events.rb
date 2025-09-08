FactoryBot.define do
  factory :fcmsevent do
    jti { SecureRandom.uuid }
    message { Faker::Book.title }
    import_timestamp { Time.zone.now }
  end
end
