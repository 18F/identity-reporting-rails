# rubocop:disable Rails/ApplicationRecord
class AnalyticsZetlApplicationRecord < ActiveRecord::Base
  self.abstract_class = true

  connects_to database: { writing: :analytics_zetl, reading: :analytics_zetl }
end
# rubocop:enable Rails/ApplicationRecord
