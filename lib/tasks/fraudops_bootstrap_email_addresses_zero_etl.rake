# lib/tasks/fraudops_bootstrap_email_addresses_zero_etl.rake

# frozen_string_literal: true

namespace :fraudops do
  desc 'Seed an empty fraudops.frd_email_addresses_zetl from fraudops.frd_email_addresses'
  task :bootstrap_email_addresses_zero_etl, [:zetl_cutoff_datetime] => :environment do |_task, args|
    zetl_cutoff_datetime = args[:zetl_cutoff_datetime]

    if zetl_cutoff_datetime.blank?
      abort 'zetl_cutoff_datetime is required, e.g. ' \
            'rake "fraudops:bootstrap_email_addresses_zero_etl[2026-08-27T00:00:00Z]"'
    end

    FraudOps::EmailAddressesZeroEtlBootstrap.new(
      zetl_cutoff_datetime: zetl_cutoff_datetime,
    ).bootstrap
  end
end
