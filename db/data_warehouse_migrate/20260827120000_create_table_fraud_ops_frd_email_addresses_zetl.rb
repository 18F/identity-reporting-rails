class CreateTableFraudOpsFrdEmailAddressesZetl < ActiveRecord::Migration[8.0]
  def change
    reversible do |dir|
      dir.up { create_table_sql }
      dir.down { drop_table_sql }
    end
  end

  private

  def using_redshift_adapter?
    connection.adapter_name.downcase.include?('redshift')
  end

  def create_table_sql
    if using_redshift_adapter?
      execute <<-SQL
        CREATE TABLE IF NOT EXISTS fraudops.frd_email_addresses_zetl (
          id bigint NOT NULL ENCODE raw,
          encrypted_email character varying(2048) ENCODE lzo COLLATE case_sensitive,
          user_id bigint ENCODE az64,
          email character varying(2048) ENCODE lzo COLLATE case_sensitive,
          dw_created_at timestamp without time zone DEFAULT GETDATE() ENCODE az64,
          dw_updated_at timestamp without time zone DEFAULT GETDATE() ENCODE az64,
          PRIMARY KEY (id)
        ) DISTSTYLE KEY DISTKEY(id) SORTKEY(id);
      SQL
    else
      execute <<-SQL
        CREATE TABLE IF NOT EXISTS fraudops.frd_email_addresses_zetl (
          id bigint NOT NULL,
          encrypted_email character varying(2048),
          user_id bigint,
          email character varying(2048),
          dw_created_at timestamp without time zone DEFAULT now(),
          dw_updated_at timestamp without time zone DEFAULT now(),
          PRIMARY KEY (id)
        );
      SQL
    end
  end

  def drop_table_sql
    execute 'DROP TABLE IF EXISTS fraudops.frd_email_addresses_zetl'
  end
end
