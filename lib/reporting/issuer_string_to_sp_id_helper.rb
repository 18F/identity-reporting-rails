# frozen_string_literal: true

module Reporting
  module IssuerStringToSpIdHelper
    # Returns a hash mapping issuer strings to service provider data
    # @param direction [Symbol] :issuer_to_id (default) or :id_to_issuer
    # @return [Hash] mapping based on direction parameter
    def get_issuer_sp_mapping(direction: :issuer_to_id)
      raw_data = fetch_issuer_mapping_data
      formatted_data = format_issuer_mapping(raw_data)

      case direction
      when :issuer_to_id
        formatted_data
      when :id_to_issuer
        invert_mapping(formatted_data)
      else
        raise ArgumentError, "Invalid direction: #{direction}. Use :issuer_to_id or :id_to_issuer"
      end
    end

    # Returns the service provider ID for a given issuer string
    # @param issuer_string [String] the issuer string to look up
    # @return [Integer, nil] the service provider ID or nil if not found
    def get_sp_id_for_issuer(issuer_string)
      mapping = get_issuer_sp_mapping
      mapping.dig(issuer_string, :id)
    end

    # Returns the issuer string for a given service provider ID
    # @param sp_id [Integer] the service provider ID to look up
    # @return [String, nil] the issuer string or nil if not found
    def get_issuer_for_sp_id(sp_id)
      mapping = get_issuer_sp_mapping(direction: :id_to_issuer)
      mapping.dig(sp_id, :issuer)
    end

    private

    def fetch_issuer_mapping_data
      DataWarehouseApplicationRecord.connection.execute(issuer_mapping_query).to_a
    rescue StandardError => e
      Rails.logger.error "Failed to fetch service provider issuer map data: #{e.message}"
      raise e
    end

    def issuer_mapping_query
      <<~SQL
        SELECT issuer, id
        FROM idp.service_providers
        WHERE issuer IS NOT NULL
          AND TRIM(issuer) <> ''
          AND id IS NOT NULL
        ORDER BY issuer;
      SQL
    end

    def format_issuer_mapping(raw_data)
      if raw_data.empty?
        Rails.logger.warn 'No service providers found in idp.service_providers'
        return {}
      end

      result = {}
      raw_data.each do |row|
        issuer = row['issuer']
        id = row['id']

        if result.key?(issuer)
          Rails.logger.error "Duplicate issuer found in idp.service_providers: #{issuer}. "\
                             "Keeping first id."
          next
        end

        begin
          result[issuer] = { id: Integer(id) }
        rescue ArgumentError, TypeError
          Rails.logger.error "Invalid id value for issuer #{issuer}: #{id.inspect}. Skipping row."
        end
      end

      result
    end

    def invert_mapping(issuer_to_id_mapping)
      id_to_issuer = {}
      issuer_to_id_mapping.each do |issuer, data|
        id = data[:id]
        if id_to_issuer.key?(id)
          Rails.logger.error "Duplicate service provider ID found: #{id}. "\
                             "Keeping first issuer: #{id_to_issuer[id][:issuer]}"
          next
        end
        id_to_issuer[id] = { issuer: issuer }
      end
      id_to_issuer
    end
  end
end
