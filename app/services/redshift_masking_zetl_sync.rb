# frozen_string_literal: true

# Syncs Redshift masking policies against the analytics_zetl database.
# Shares all sync logic (and the mask.yaml data-controls file) with
# RedshiftMaskingSync; only the target database section and the ActiveRecord
# connection differ.
class RedshiftMaskingZetlSync < RedshiftMaskingSync
  private

  def database_name
    'analytics_zetl'
  end

  def connection_class
    AnalyticsZetlApplicationRecord
  end
end
