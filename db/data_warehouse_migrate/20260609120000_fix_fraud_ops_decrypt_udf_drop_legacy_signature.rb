class FixFraudOpsDecryptUdfDropLegacySignature < ActiveRecord::Migration[7.2]
  # The previous migration 20260602120000_update_redshift_fraud_ops_decryption_udf_parameters
  # failed because Redshift does not support DROP FUNCTION IF EXISTS.
  # This migration safely drops the legacy single-argument overload if it still exists,
  # and ensures the two-argument overload is present.
  def change
    return unless connection.adapter_name.downcase.include?('redshift')

    reversible do |dir|
      env_name = Identity::Hostdata.env
      account_id = Identity::Hostdata.aws_account_id
      lambda_name = "#{env_name}-redshift-idp-decryption-udf"
      redshift_iam_role_name = "arn:aws:iam::#{account_id}:role/#{env_name}-redshift-iam-role"

      dir.up do
        single_arg_exists = DataWarehouseApplicationRecord.connection.execute(<<~SQL).to_a.any?
          SELECT 1 FROM SVV_REDSHIFT_FUNCTIONS
          WHERE schema_name = 'fraudops'
            AND function_name = 'decrypt_udf'
            AND argument_type = 'character varying';
        SQL
        execute('DROP FUNCTION fraudops.decrypt_udf(varchar);') if single_arg_exists

        execute <<~SQL
          CREATE OR REPLACE EXTERNAL FUNCTION fraudops.decrypt_udf (encrypted_value varchar, id bigint)
          RETURNS varchar(2048) STABLE
          LAMBDA '#{lambda_name}'
          IAM_ROLE '#{redshift_iam_role_name}';
        SQL
      end

      dir.down do
        two_arg_exists = DataWarehouseApplicationRecord.connection.execute(<<~SQL).to_a.any?
          SELECT 1 FROM SVV_REDSHIFT_FUNCTIONS
          WHERE schema_name = 'fraudops'
            AND function_name = 'decrypt_udf'
            AND argument_type = 'character varying, bigint';
        SQL
        execute('DROP FUNCTION fraudops.decrypt_udf(varchar, bigint);') if two_arg_exists

        execute <<~SQL
          CREATE OR REPLACE EXTERNAL FUNCTION fraudops.decrypt_udf (encrypted_value varchar)
          RETURNS varchar(2048) STABLE
          LAMBDA '#{lambda_name}'
          IAM_ROLE '#{redshift_iam_role_name}';
        SQL
      end
    end
  end
end
