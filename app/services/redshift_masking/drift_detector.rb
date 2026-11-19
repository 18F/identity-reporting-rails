# frozen_string_literal: true

module RedshiftMasking
  # Detects differences between expected and actual masking policy attachments
  class DriftDetector
    def detect(expected_list, actual_list, silent: false)
      Rails.logger.info('detecting drift in masking policies')

      expected_map = expected_list.index_by(&:key)
      actual_map = actual_list.index_by(&:key)

      drift = { missing: [], extra: [], mismatched: [] }

      find_missing_and_mismatched(expected_map, actual_map, drift)
      find_extra(expected_map, actual_map, drift)
      log_drift(drift) unless silent

      drift
    end

    def log_drift(drift)
      drift[:missing].each do |policy|
        Rails.logger.warn("MISSING: #{policy.grantee} on #{policy.column_id}")
      end
      drift[:mismatched].each do |mismatch|
        expected = mismatch[:expected]
        Rails.logger.warn(
          "MISMATCH: #{expected.grantee} on #{expected.column_id} " \
          "(Expected #{expected.policy_name} Priority #{expected.priority})",
        )
      end
      drift[:extra].each do |policy|
        Rails.logger.warn("EXTRA: #{policy.grantee} on #{policy.column_id}")
      end
    end

    private

    def find_missing_and_mismatched(expected_map, actual_map, drift)
      expected_map.each do |key, expected|
        actual = actual_map[key]
        if actual.nil?
          drift[:missing] << expected
        elsif !expected.matches?(actual)
          drift[:mismatched] << { expected: expected, actual: actual }
        end
      end
    end

    def find_extra(expected_map, actual_map, drift)
      actual_map.each do |key, actual|
        unless expected_map.key?(key)
          drift[:extra] << actual
        end
      end
    end
  end
end
