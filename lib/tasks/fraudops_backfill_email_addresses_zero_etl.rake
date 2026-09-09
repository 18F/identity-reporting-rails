# lib/tasks/fraudops_backfill_email_addresses_zero_etl.rake

# frozen_string_literal: true

namespace :fraudops do
  desc 'Backfill fraudops.frd_email_addresses_zetl from the Zero-ETL email_addresses replica'
  task :backfill_email_addresses_zero_etl, [:zetl_cutoff_datetime] => :environment do |_task, args|
    zetl_cutoff_datetime = args[:zetl_cutoff_datetime]

    if zetl_cutoff_datetime.blank?
      abort 'zetl_cutoff_datetime is required, e.g. ' \
            'rake "fraudops:backfill_email_addresses_zero_etl[2026-08-27T00:00:00Z]"'
    end

    FraudOps::EmailAddressesZeroEtlBackfill.new(
      zetl_cutoff_datetime: zetl_cutoff_datetime,
    ).backfill
  end
end
