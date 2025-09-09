FactoryBot.define do
  factory :fcms_encrypted_event do
    message { { text: Faker::Lorem.sentence } }
    import_timestamp { Time.zone.now }
    processsed_timestamp { Time.zone.now }
  end
end
