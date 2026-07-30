# frozen_string_literal: true

# RedshiftMaskingSync; only the target database section and the ActiveRecord
class RedshiftMaskingZetlSync < RedshiftMaskingSync
  private

  def database_name
    'analytics_zetl'
  end

  def connection_class
    AnalyticsZetlApplicationRecord
  end
end
