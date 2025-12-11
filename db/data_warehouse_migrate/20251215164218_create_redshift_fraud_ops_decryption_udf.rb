class CreateRedshiftFraudOpsDecryptionUdf < ActiveRecord::Migration[7.2]
  # This migration was created to fix a bug introduced with migration 20250820200739_create_redshift_decryption_udf
  # where the udf function was being created before the existence of the fraudops schema which caused
  # issues with new envs but would run successfully with existing envs where the fraudops schema already exists.
    def change
    if connection.adapter_name.downcase.include?('redshift')
      reversible do |dir|
        env_name = Identity::Hostdata.env
        account_id = Identity::Hostdata.aws_account_id
        lambda_name = "#{env_name}-redshift-idp-decryption-udf"
        redshift_iam_role_name = "arn:aws:iam::#{account_id}:role/#{env_name}-redshift-iam-role"
        dir.up do
          # First drop the public schema function, for envs where it was created
          query = <<~SQL
            SELECT 1 AS exists
            FROM SVV_REDSHIFT_FUNCTIONS 
            WHERE function_name = 'decrypt_udf' and schema_name = 'public';
          SQL
          exists = DataWarehouseApplicationRecord.connection.execute(query).to_a.any?
          unless not exists
            execute <<-SQL
            DROP FUNCTION public.decrypt_udf(varchar);
            SQL
          end
          
          # Second recreate the fraudops schema function
          execute <<-SQL
          CREATE OR REPLACE EXTERNAL FUNCTION fraudops.decrypt_udf (encrypted_value varchar)
          RETURNS varchar STABLE
          LAMBDA '#{lambda_name}'
          IAM_ROLE '#{redshift_iam_role_name}';
          SQL
        end

        dir.down do
          execute <<-SQL
          DROP FUNCTION fraudops.decrypt_udf(varchar);
          SQL
        end
      end
    end
  end
end
