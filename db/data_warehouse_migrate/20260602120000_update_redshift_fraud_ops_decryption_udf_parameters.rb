class UpdateRedshiftFraudOpsDecryptionUdfParameters < ActiveRecord::Migration[7.2]
  # Intentional no-op. This migration originally used DROP FUNCTION IF EXISTS which
  # is not supported by Redshift and caused a syntax error on deploy.
  # The actual work (drop legacy single-arg overload + create two-arg overload) is
  # handled by 20260609120000_fix_fraud_ops_decrypt_udf_drop_legacy_signature.rb
  def change
  end
end
