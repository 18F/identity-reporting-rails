# lib/tasks/sync_idp_curated_views.rake

# frozen_string_literal: true

require 'zetl_binding_view_sync'

namespace :db do
  desc 'Refresh idp_curated_views late-binding views from analytics_zetl (minus excluded columns)'
  task sync_idp_curated_views: :environment do
    unless ENV['RAILS_ENV']
      puts 'RAILS_ENV environment variable is not set'
      exit 1
    end

    unless IdentityConfig.store.zero_etl_enabled
      puts 'Skipping: zero_etl_enabled is false'
      next
    end

    results = ZetlBindingViewSync.new.sync

    puts "idp_curated_views binding view sync completed successfully " \
         "(#{results[:created]} synced, #{results[:skipped]} skipped, " \
         "#{results[:stale]} stale, #{results[:failed]} failed)"
  end
end
