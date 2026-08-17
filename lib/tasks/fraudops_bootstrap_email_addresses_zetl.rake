# lib/tasks/fraudops_bootstrap_email_addresses_zetl.rake

# frozen_string_literal: true

namespace :fraudops do
  desc 'Create and seed fraudops.frd_email_addresses_zero_etl from fraudops.frd_email_addresses'
  task bootstrap_email_addresses_zetl: :environment do
    FraudOps::EmailAddressesZeroEtlBootstrap.new.bootstrap
  end
end
