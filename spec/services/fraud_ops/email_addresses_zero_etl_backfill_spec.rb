require 'rails_helper'

RSpec.describe FraudOps::EmailAddressesZeroEtlBackfill do
  let(:service) { described_class.new(zetl_cutoff_datetime: cutoff) }
  let(:cutoff) { '2026-01-01T00:00:00Z' }
  let(:mock_connection) { instance_double(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter) }
  let(:target_table_exists) { true }

  # Every SQL string handed to the connection's #execute, in order.
  let(:executed_sql) { [] }

  before do
    allow(DataWarehouseApplicationRecord).to receive(:connection).and_return(mock_connection)
    allow(DataWarehouseApplicationRecord).to receive(:transaction).and_yield

    allow(mock_connection).to receive(:table_exists?).and_return(target_table_exists)
    allow(mock_connection).to receive(:quote) { |value| "'#{value}'" }
    allow(mock_connection).to receive(:execute) { |sql| executed_sql << sql }
    allow(Rails.logger).to receive(:info)
  end

  describe '#backfill' do
    context 'when the target table does not exist' do
      let(:target_table_exists) { false }

      it 'logs the skip and does no work' do
        expect(Rails.logger).to receive(:info).with(
          a_string_matching(/fraudops\.frd_email_addresses_zetl does not exist/),
        )

        expect(service.backfill).to be(false)
        expect(executed_sql).to be_empty
      end

      it 'never provisions the table itself' do
        service.backfill

        expect(executed_sql).not_to include(a_string_matching(/CREATE TABLE/i))
      end
    end

    context 'when the target table exists' do
      let(:insert) do
        service.backfill
        executed_sql.find { |sql| sql.include?('INSERT INTO') }
      end

      it 'seeds the target with an insert-only anti-join keyed on id' do
        expect(insert).to match(/INSERT INTO fraudops\.frd_email_addresses_zetl \(id, /)
        expect(insert).to match(/NOT EXISTS \( SELECT 1 FROM fraudops\.frd_email_addresses_zetl/)
        expect(insert).to match(/AS target WHERE target\.id = source\.id \)/)
      end

      it 'does not update rows that already exist in the target' do
        expect(insert).not_to match(/MERGE/)
        expect(insert).not_to match(/UPDATE/)
      end

      it 'inserts only rows created before the cutoff' do
        expect(insert).to include("WHERE source.created_at < '#{cutoff}'")
      end

      it 'decrypts the email through the decryption UDF' do
        expect(insert).to match(/fraudops\.decrypt_udf\(source\.encrypted_email, source\.id\)/)
      end

      # Regression: the placeholder was once %{SCHEMA_NAME} while build_params supplied
      # :schema_name, so #format raised KeyError on every call. Any unresolved or
      # misnamed placeholder shows up as a literal %{...} or blows up before this runs.
      it 'resolves every format placeholder in the statement' do
        expect(insert).not_to include('%{')
      end

      it 'wraps the seed in a single transaction' do
        service.backfill

        expect(DataWarehouseApplicationRecord).to have_received(:transaction).once
      end

      it 'never creates the table' do
        service.backfill

        expect(executed_sql).not_to include(a_string_matching(/CREATE TABLE/i))
      end

      # The seed now runs as the connection's own user rather than switching to
      # pii_reader, so that user needs INSERT on the target and EXECUTE on the UDF.
      it 'does not switch the session user' do
        service.backfill

        expect(executed_sql).not_to include(a_string_matching(/SESSION AUTHORIZATION/i))
      end

      it 'logs what it seeded and reports that it backfilled' do
        expect(Rails.logger).to receive(:info).with(
          "Seeded fraudops.frd_email_addresses_zetl from #{described_class::SOURCE_TABLE}",
        )

        expect(service.backfill).to be(true)
      end

      it 'never logs row values' do
        logged = []
        allow(Rails.logger).to receive(:info) { |msg| logged << msg }

        service.backfill

        expect(logged).not_to be_empty
        logged.each { |message| expect(message).not_to include('@') }
      end
    end

    # The source moved from the warehouse table fraudops.frd_email_addresses to the
    # Zero-ETL replica of the IdP table, which is reached by a three-part name and
    # carries the IdP's own columns. These examples pin both halves of that contract.
    describe 'the Zero-ETL source' do
      let(:insert) do
        service.backfill
        executed_sql.find { |sql| sql.include?('INSERT INTO') }
      end

      # The database half comes from redshift_database_zero_etl_name, which is ''
      # locally, so this pins the shape of the FROM clause rather than the deployed
      # database name.
      it 'reads from public.email_addresses in the configured Zero-ETL database' do
        expect(described_class::SOURCE_TABLE).to end_with('.public.email_addresses')
        expect(insert).to include("FROM #{described_class::SOURCE_TABLE} AS source")
      end

      # Regression: the three-part name was passed through #qualified, producing
      # fraudops.<db>.public.email_addresses, which no database will parse.
      it 'does not prefix the already-qualified source with the fraudops schema' do
        expect(insert).not_to include("fraudops.#{described_class::SOURCE_TABLE}")
      end

      # Regression: dw_created_at/dw_updated_at exist only on the target table. The
      # replica has the IdP's created_at/updated_at, so reading dw_* from the source
      # is a missing-column error.
      it 'never reads dw_ audit columns from the source' do
        expect(insert).not_to match(/source\.dw_/)
      end

      it 'stamps the target dw_ columns at load time' do
        select_list = insert[/SELECT(.*?)FROM /m, 1]

        expect(insert).to include(
          '(id, encrypted_email, user_id, email, dw_created_at, dw_updated_at)',
        )
        expect(select_list.scan('CURRENT_TIMESTAMP').length).to eq(2)
      end

      # The whole point of the three-part name: one connection, one transaction.
      it 'never opens a connection to the Zero-ETL database' do
        expect(DataWarehouseApplicationRecordZeroEtl).not_to receive(:connection)

        service.backfill
      end
    end
  end

  # The examples above assert on generated SQL strings, which cannot show that the
  # statement is executable or that it actually copies rows. This block runs the real
  # code path against the test database.
  #
  # SOURCE_TABLE is stubbed to a local two-part name because PostgreSQL has no
  # cross-database references and the database half of the name is '' in test. The
  # three-part shape is covered above; what runs here is the insert-only anti-join,
  # the cutoff filter and the UDF call, against a source table deliberately built
  # with the replica's columns (no dw_*) so a regression fails as a missing column.
  describe 'executed against PostgreSQL' do
    let(:connection) { DataWarehouseApplicationRecord.connection }
    let(:source) { 'fraudops.email_addresses' }
    let(:target) { 'fraudops.frd_email_addresses_zetl' }
    let(:drop_udf_sql) { 'DROP FUNCTION IF EXISTS fraudops.decrypt_udf(varchar, bigint)' }

    before do
      allow(DataWarehouseApplicationRecord).to receive(:connection).and_call_original
      allow(DataWarehouseApplicationRecord).to receive(:transaction).and_call_original
      stub_const("#{described_class}::SOURCE_TABLE", source)

      connection.execute('CREATE SCHEMA IF NOT EXISTS fraudops')
      [target, source].each { |table| connection.execute("DROP TABLE IF EXISTS #{table} CASCADE") }

      # Mirrors the Zero-ETL replica of the IdP table: no dw_ columns.
      connection.execute(<<~SQL)
        CREATE TABLE #{source} (
          id bigint NOT NULL PRIMARY KEY,
          encrypted_email varchar(2048),
          user_id bigint,
          created_at timestamp,
          updated_at timestamp
        )
      SQL
      connection.execute(<<~SQL)
        CREATE TABLE #{target} (
          id bigint NOT NULL PRIMARY KEY,
          encrypted_email varchar(2048),
          user_id bigint,
          email varchar(2048),
          dw_created_at timestamp DEFAULT now(),
          dw_updated_at timestamp DEFAULT now()
        )
      SQL

      # Both rows predate the cutoff so they are copied; a later example adds a
      # post-cutoff row to prove the filter excludes it.
      connection.execute(<<~SQL)
        INSERT INTO #{source} (id, encrypted_email, user_id, created_at, updated_at)
        VALUES (1, 'enc1', 11, '2020-01-01', '2020-01-01'),
               (2, 'enc2', 22, '2020-02-01', '2020-02-01')
      SQL

      # On Redshift this is a Lambda-backed EXTERNAL FUNCTION, which the data
      # warehouse migrations skip on PostgreSQL. Stand in a SQL function with the
      # deployed two-argument signature so the call arity is exercised, not faked.
      # Dropped rather than replaced first: CREATE OR REPLACE cannot rename input
      # parameters, so a leftover definition would make this fail.
      connection.execute(drop_udf_sql)
      connection.execute(<<~SQL)
        CREATE FUNCTION fraudops.decrypt_udf(encrypted_value varchar, id bigint)
        RETURNS varchar AS $$ SELECT 'decrypted-' || encrypted_value $$ LANGUAGE sql STABLE
      SQL
    end

    after do
      [target, source].each { |table| connection.execute("DROP TABLE IF EXISTS #{table} CASCADE") }
      connection.execute(drop_udf_sql)
    end

    it 'seeds the empty target with the decrypted source rows and reports success' do
      expect(service.backfill).to be(true)

      rows = connection.select_all("SELECT id, user_id, email FROM #{target} ORDER BY id").to_a

      expect(rows).to eq(
        [
          { 'id' => 1, 'user_id' => 11, 'email' => 'decrypted-enc1' },
          { 'id' => 2, 'user_id' => 22, 'email' => 'decrypted-enc2' },
        ],
      )
    end

    it 'carries the encrypted email over alongside the decrypted one' do
      service.backfill

      expect(connection.select_value("SELECT encrypted_email FROM #{target} WHERE id = 1")).
        to eq('enc1')
    end

    it 'stamps dw_created_at and dw_updated_at on the rows it inserts' do
      service.backfill

      row = connection.select_one("SELECT dw_created_at, dw_updated_at FROM #{target} WHERE id = 1")

      expect(row['dw_created_at']).to be_present
      expect(row['dw_updated_at']).to be_present
    end

    it 'copies only rows created before the cutoff' do
      connection.execute(<<~SQL)
        INSERT INTO #{source} (id, encrypted_email, user_id, created_at, updated_at)
        VALUES (3, 'enc3', 33, '2030-01-01', '2030-01-01')
      SQL

      expect(service.backfill).to be(true)

      expect(connection.select_values("SELECT id FROM #{target} ORDER BY id")).to eq([1, 2])
    end

    it 'leaves rows the target already has untouched' do
      connection.execute(<<~SQL)
        INSERT INTO #{target} (id, encrypted_email, user_id, email)
        VALUES (1, 'existing', 111, 'existing-email')
      SQL

      expect(service.backfill).to be(true)

      rows = connection.select_all("SELECT id, user_id, email FROM #{target} ORDER BY id").to_a

      expect(rows).to eq(
        [
          { 'id' => 1, 'user_id' => 111, 'email' => 'existing-email' },
          { 'id' => 2, 'user_id' => 22, 'email' => 'decrypted-enc2' },
        ],
      )
    end

    it 'preserves target rows that have no counterpart in the source' do
      connection.execute("INSERT INTO #{target} (id, user_id, email) VALUES (99, 999, 'kept')")

      expect(service.backfill).to be(true)

      expect(connection.select_values("SELECT id FROM #{target} ORDER BY id")).to eq([1, 2, 99])
    end

    it 'is idempotent across repeated runs' do
      service.backfill

      expect { service.backfill }.
        not_to change { connection.select_value("SELECT count(*) FROM #{target}") }
    end

    it 'does nothing when the target does not exist' do
      connection.execute("DROP TABLE IF EXISTS #{target} CASCADE")

      expect(service.backfill).to be(false)
    end
  end
end
