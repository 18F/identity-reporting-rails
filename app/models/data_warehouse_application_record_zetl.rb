# rubocop:disable Rails/ApplicationRecord
class DataWarehouseApplicationRecordZetl < ActiveRecord::Base
  self.abstract_class = true

  connects_to database: { writing: :data_warehouse_zetl, reading: :data_warehouse_zetl }
end
# rubocop:enable Rails/ApplicationRecord
