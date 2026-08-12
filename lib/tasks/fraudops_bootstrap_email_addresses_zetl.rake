# lib/tasks/fraudops_bootstrap_email_addresses_zetl.rake

# frozen_string_literal: true

namespace :fraudops do
  desc 'Create and seed fraudops.frd_email_addresses_zetl from fraudops.frd_email_addresses'
  task bootstrap_email_addresses_zetl: :environment do
    FraudOps::EmailAddressesZetlBootstrap.new.bootstrap
  end
end
