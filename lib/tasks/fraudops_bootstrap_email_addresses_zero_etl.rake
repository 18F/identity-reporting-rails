# lib/tasks/fraudops_bootstrap_email_addresses_zero_etl.rake

# frozen_string_literal: true

namespace :fraudops do
  desc 'Create and seed fraudops.frd_email_addresses_zero_etl from fraudops.frd_email_addresses'
  task bootstrap_email_addresses_zero_etl: :environment do
    FraudOps::EmailAddressesZeroEtlBootstrap.new.bootstrap
  end
end
