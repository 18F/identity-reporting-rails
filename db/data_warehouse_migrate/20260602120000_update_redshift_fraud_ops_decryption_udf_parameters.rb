class UpdateRedshiftFraudOpsDecryptionUdfParameters < ActiveRecord::Migration[7.2]
  # This migration updates the decryption UDF signature to accept an additional
  # id argument while preserving the existing return type length.
  def change
    if connection.adapter_name.downcase.include?('redshift')
      reversible do |dir|
        env_name = Identity::Hostdata.env
        account_id = Identity::Hostdata.aws_account_id
        lambda_name = "#{env_name}-redshift-idp-decryption-udf"
        redshift_iam_role_name = "arn:aws:iam::#{account_id}:role/#{env_name}-redshift-iam-role"

        dir.up do
          execute <<~SQL
            CREATE OR REPLACE EXTERNAL FUNCTION fraudops.decrypt_udf (encrypted_value varchar, id bigint)
            RETURNS varchar(2048) STABLE
            LAMBDA '#{lambda_name}'
            IAM_ROLE '#{redshift_iam_role_name}';
          SQL
        end

        dir.down do
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
end
