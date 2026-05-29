class UpdateRedshiftFraudOpsDecryptionUdfReturnLength < ActiveRecord::Migration[7.2]
  # This migration was created to update the return length of the decryption udf to 2048 characters to support longer decrypted values. The previous length of 256 characters was not sufficient for some use cases and caused truncation of decrypted values. This migration will ensure that the decryption udf can handle longer decrypted values without truncation issues.

  def change
    if connection.adapter_name.downcase.include?('redshift')
      reversible do |dir|
        env_name = Identity::Hostdata.env
        account_id = Identity::Hostdata.aws_account_id
        lambda_name = "#{env_name}-redshift-idp-decryption-udf"
        redshift_iam_role_name = "arn:aws:iam::#{account_id}:role/#{env_name}-redshift-iam-role"

        dir.up do
          execute <<~SQL
            CREATE OR REPLACE EXTERNAL FUNCTION fraudops.decrypt_udf (encrypted_value varchar)
            RETURNS varchar(2048) STABLE
            LAMBDA '#{lambda_name}'
            IAM_ROLE '#{redshift_iam_role_name}';
          SQL
        end

        dir.down do
          execute <<~SQL
            CREATE OR REPLACE EXTERNAL FUNCTION fraudops.decrypt_udf (encrypted_value varchar)
            RETURNS varchar STABLE
            LAMBDA '#{lambda_name}'
            IAM_ROLE '#{redshift_iam_role_name}';
          SQL
        end
      end
    end
  end
end