class CreateRedshiftDecryptionUdf < ActiveRecord::Migration[7.2]
  def change
    if connection.adapter_name.downcase.include?('redshift')
      reversible do |dir|
        env_name = Identity::Hostdata.env
        account_id = Identity::Hostdata.aws_account_id
        lambda_name = "#{env_name}-redshift-idp-decryption-udf"
        redshift_iam_role_name = "arn:aws:iam::#{account_id}:role/#{env_name}-redshift-iam-role"
        dir.up do
          execute <<-SQL
          CREATE OR REPLACE EXTERNAL FUNCTION decrypt_udf (encrypted_value varchar)
          RETURNS varchar STABLE
          LAMBDA '#{lambda_name}'
          IAM_ROLE '#{redshift_iam_role_name}';
          SQL
        end

        dir.down do
          execute <<-SQL
          DROP FUNCTION decrypt_udf(varchar);
          SQL
        end
      end
    end
  end
end
