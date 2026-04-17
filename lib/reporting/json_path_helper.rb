module Reporting
  module JsonPathHelper
    def extract_json_path(column, path, type: 'VARCHAR', keep_parenthesis: true)
      if connection.adapter_name.downcase.include?('redshift')
        Rails.logger.info 'Detected redshift'
        "#{column}.#{path}"
      else
        Rails.logger.info 'Detected PostgreSQL'
        # PostgreSQL JSON operators - cast to JSONB first
        parts = path.split('.')
        quoted_parts = parts.map { |part| "'#{part}'" }
        to_string = types_to_extract_as_text.include?(type) || type.include?('VARCHAR')

        if to_string
          if parts.length == 1
            final_key = "(#{column}::jsonb->>'#{path}')"
          else
            final_key = "(#{column}::jsonb->#{quoted_parts[0..-2].join('->')}" \
                        "->>#{quoted_parts[-1]})"
          end
        elsif parts.length == 1
          # For non-text types, cast the final result
          final_key = "(#{column}::jsonb->>'#{path}')::#{type.downcase}"
        else
          final_key = "(#{column}::jsonb->#{quoted_parts[0..-2].join('->')}" \
                      "->>#{quoted_parts[-1]})::#{type.downcase}"
        end

        keep_parenthesis ? final_key : final_key[1..-2]
      end
    end

    private

    def connection
      DataWarehouseApplicationRecord.connection
    end

    def types_to_extract_as_text
      ['TIMESTAMP']
    end
  end
end
