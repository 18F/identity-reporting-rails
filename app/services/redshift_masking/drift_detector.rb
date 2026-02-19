# frozen_string_literal: true

module RedshiftMasking
  # Detects differences between expected and actual masking policy attachments
  class DriftDetector
    attr_reader :logger

    def initialize(logger)
      @logger = logger
    end

    def detect(expected_list, actual_list, silent: false)
      logger.log_info('detecting drift in masking policies')

      expected_map = expected_list.index_by(&:key)
      actual_map = actual_list.index_by(&:key)

      drift = { missing: [], extra: [], mismatched: [] }

      find_missing_and_mismatched(expected_map, actual_map, drift, silent: silent)
      find_extra(expected_map, actual_map, drift, silent: silent)

      drift
    end

    private

    def find_missing_and_mismatched(expected_map, actual_map, drift, silent: false)
      expected_map.each do |key, expected|
        actual = actual_map[key]
        if actual.nil?
          drift[:missing] << expected
          unless silent
            logger.log_warn("MISSING: #{expected.grantee} on #{expected.column_id}")
          end
        elsif !expected.matches?(actual)
          drift[:mismatched] << { expected: expected, actual: actual }
          unless silent
            logger.log_warn(
              "MISMATCH: #{expected.grantee} on #{expected.column_id} " \
              "(Expected #{expected.policy_name} Priority #{expected.priority})",
            )
          end
        end
      end
    end

    def find_extra(expected_map, actual_map, drift, silent: false)
      actual_map.each do |key, actual|
        unless expected_map.key?(key)
          drift[:extra] << actual
          logger.log_warn("EXTRA: #{actual.grantee} on #{actual.column_id}") unless silent
        end
      end
    end
  end
end
