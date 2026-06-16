# lib/tasks/redshift_rotate_password.rake

# frozen_string_literal: true

namespace :redshift do
  desc 'Rotate Redshift login passwords for system users (optionally pass usernames)'
  task :rotate_password, [:usernames] => :environment do |_task, args|
    # Accepts a comma- or space-separated list of usernames, e.g.
    #   rake "redshift:rotate_password[pii_reader rails_worker]"
    # With no argument, rotates every system user that has a secret_id.
    usernames = args[:usernames].to_s.split(/[\s,]+/).reject(&:empty?)

    RedshiftSync.new.rotate_password(usernames: usernames)
  end
end
