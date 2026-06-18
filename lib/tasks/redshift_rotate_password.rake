# lib/tasks/redshift_rotate_password.rake

# frozen_string_literal: true

namespace :redshift do
  desc 'Rotate Redshift login passwords for system users (pass usernames, or "all")'
  task :rotate_password, [:usernames] => :environment do |_task, args|
    #   rake "redshift:rotate_password[pii_reader rails_worker] or [all]"
    RedshiftPasswordRotator.new.rotate(args[:usernames])
  end
end
