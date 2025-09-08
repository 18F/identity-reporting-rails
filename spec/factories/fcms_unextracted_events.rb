FactoryBot.define do
  factory :fcms_unextracted_event do
    import_timestamp { Time.zone.now }
    message { { text: Faker::Lorem.sentence } }
    key_hash { Faker::Internet.uuid }
  end
end
