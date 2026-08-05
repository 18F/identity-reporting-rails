class AnalyticsZetlApplicationRecord < DataWarehouseApplicationRecord
  self.abstract_class = true

  # Skipped in test. There's no real analytics_zetl database in test,
  # and specs cover the generated SQL via mocked connections.
  unless Rails.env.test?
    zetl_config = DataWarehouseApplicationRecord.connection_db_config.configuration_hash.
      merge(database: 'analytics_zetl', pool: 1)

    connects_to database: { writing: zetl_config, reading: zetl_config }
  end
end
