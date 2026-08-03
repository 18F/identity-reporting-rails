# frozen_string_literal: true

# Syncs masking policies against the analytics_zetl database.
class RedshiftMaskingZetlSync < RedshiftMaskingSync
  ZETL_DATABASE_NAME = 'analytics_zetl'

  # Connection bound to the analytics_zetl database on the data_warehouse
  class ZetlConnection < DataWarehouseApplicationRecord
    self.abstract_class = true

    class << self
      def connection
        unless @connection_established
          establish_connection(
            ActiveRecord::Base.configurations.
              configs_for(env_name: Rails.env, name: 'data_warehouse').
              configuration_hash.
              merge(database: ZETL_DATABASE_NAME),
          )
          @connection_established = true
        end
        super
      end
    end
  end

  private

  def database_name
    ZETL_DATABASE_NAME
  end

  def connection_class
    ZetlConnection
  end
end
