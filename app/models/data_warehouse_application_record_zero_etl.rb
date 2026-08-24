# rubocop:disable Rails/ApplicationRecord
class DataWarehouseApplicationRecordZeroEtl < ActiveRecord::Base
  self.abstract_class = true

  connects_to database: { writing: :data_warehouse_zero_etl, reading: :data_warehouse_zero_etl }
end
# rubocop:enable Rails/ApplicationRecord
