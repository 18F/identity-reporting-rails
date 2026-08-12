require 'rails_helper'

RSpec.describe FraudOps::EmailAddressesZetlSync do
  let(:service) { described_class.new }
  let(:mock_connection) { instance_double(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter) }
  let(:target_table_exists) { true }

  # Every SQL string handed to the connection, in order.
  let(:executed_sql) { [] }

  before do
    allow(DataWarehouseApplicationRecord).to receive(:connection).and_return(mock_connection)
    allow(DataWarehouseApplicationRecord).to receive(:transaction).and_yield

    # Stubbed only so the "does not branch on the adapter" example can assert the
    # service never reaches for it.
    allow(mock_connection).to receive(:adapter_name).and_return('PostgreSQL')
    allow(mock_connection).to receive(:table_exists?).and_return(target_table_exists)
    allow(mock_connection).to receive(:quote) { |value| "'#{value}'" }
    allow(mock_connection).to receive(:execute) { |sql| executed_sql << sql }
    allow(Rails.logger).to receive(:info)
  end

  describe '#sync' do
    context 'when the target table does not exist' do
      let(:target_table_exists) { false }

      it 'executes no SQL' do
        service.sync

        expect(executed_sql).to be_empty
      end

      it 'logs the skip and names the rake task that provisions the table' do
        expect(Rails.logger).to receive(:info).with(
          a_string_matching(/fraudops\.frd_email_addresses_zetl does not exist/).
            and(a_string_matching(/rake fraudops:bootstrap_email_addresses_zetl/)),
        )

        service.sync
      end

      it 'reports that it skipped' do
        expect(service.sync).to eq(skipped: true)
      end

      it 'does not provision the table itself' do
        service.sync

        expect(executed_sql).not_to include(a_string_matching(/CREATE TABLE .*_zetl \(LIKE/))
        expect(executed_sql).not_to include(a_string_matching(/INSERT INTO .*_zetl SELECT \*/))
      end
    end

    context 'when the target table already exists' do
      let(:target_table_exists) { true }

      it 'performs the merge' do
        service.sync

        expect(executed_sql).to include(a_string_matching(/frd_email_addresses_zetl_staging/))
      end

      it 'reports that it did not skip' do
        expect(service.sync[:skipped]).to be(false)
      end
    end

    describe 'staging load' do
      let(:staging_load_sql) do
        service.sync
        executed_sql.find do |sql|
          sql.include?('INSERT INTO fraudops.frd_email_addresses_zetl_staging')
        end
      end

      it 'drops any leftover staging table before creating it' do
        service.sync

        drop_index = executed_sql.index { |sql| sql.match?(/DROP TABLE IF EXISTS .*_staging/) }
        create_index = executed_sql.index { |sql| sql.match?(/CREATE TABLE .*_staging \(LIKE/) }

        expect(drop_index).to be < create_index
      end

      it 'decrypts the email through the decryption UDF' do
        expect(staging_load_sql).to match(
          /fraudops\.decrypt_udf\(curated\.encrypted_email, curated\.id\)/,
        )
      end

      it 'reads from the curated view' do
        expect(staging_load_sql).to match(/FROM idp_curated_views\.email_addresses curated/)
      end

      it 'filters on the curated view updated_at' do
        expect(staging_load_sql).to match(/WHERE curated\.updated_at >= /)
      end

      it 'drops the staging table after the merge' do
        service.sync

        expect(executed_sql.last).to match(/DROP TABLE IF EXISTS .*_staging/)
      end
    end

    describe 'lookback window', freeze_time: true do
      let(:staging_load_cutoff) do
        service.sync
        executed_sql.find { |sql| sql.match?(/INSERT INTO .*_staging/) }
      end

      it 'reads the window from config so each environment can override it' do
        allow(IdentityConfig.store).
          to receive(:zero_etl_email_addresses_lookback_minutes).and_return(60)

        expect(staging_load_cutoff).to include(60.minutes.ago.utc.to_s)
      end

      it 'falls back to the default when config is unset' do
        allow(IdentityConfig.store).
          to receive(:zero_etl_email_addresses_lookback_minutes).and_return(nil)

        expect(described_class::DEFAULT_LOOKBACK_MINUTES).to eq(15)
        expect(staging_load_cutoff).to include(15.minutes.ago.utc.to_s)
      end

      it 'returns the cutoff and lookback it used' do
        allow(IdentityConfig.store).
          to receive(:zero_etl_email_addresses_lookback_minutes).and_return(60)

        expect(service.sync).to include(
          cutoff: 60.minutes.ago.utc.iso8601,
          lookback_minutes: 60,
        )
      end
    end

    describe 'merge' do
      it 'uses the MERGE construct keyed on id' do
        service.sync

        merge_sql = executed_sql.find { |sql| sql.include?('MERGE INTO') }

        expect(merge_sql).to match(/MERGE INTO fraudops\.frd_email_addresses_zetl/)
        expect(merge_sql).to match(/ON fraudops\.frd_email_addresses_zetl\.id = source\.id/)
        expect(merge_sql).to match(/WHEN MATCHED THEN UPDATE SET/)
        expect(merge_sql).to match(/WHEN NOT MATCHED THEN INSERT/)
      end

      it 'preserves dw_created_at on update' do
        service.sync

        merge_sql = executed_sql.find { |sql| sql.include?('MERGE INTO') }
        matched_clause = merge_sql[/WHEN MATCHED THEN UPDATE SET(.*?)WHEN NOT MATCHED/m, 1]

        expect(matched_clause).not_to include('dw_created_at')
        expect(matched_clause).to include('dw_updated_at')
      end

      # Redshift requires the fully-qualified target with no alias on the left of
      # ON. That form is also valid on PostgreSQL 15+, so one statement serves
      # every environment and the service never branches on the adapter.
      it 'does not branch on the adapter' do
        service.sync

        expect(mock_connection).not_to have_received(:adapter_name)
      end
    end
  end

  # The examples above assert on generated SQL strings, which cannot show that a
  # statement is actually executable. This one runs the real MERGE against the
  # test database so the production statement is exercised, not just matched.
  describe 'the generated MERGE, executed against PostgreSQL' do
    let(:connection) { DataWarehouseApplicationRecord.connection }
    let(:target) { 'fraudops.frd_email_addresses_zetl' }
    let(:staging) { 'fraudops.frd_email_addresses_zetl_staging' }

    before do
      allow(DataWarehouseApplicationRecord).to receive(:connection).and_call_original

      connection.execute('CREATE SCHEMA IF NOT EXISTS fraudops')
      [target, staging].each do |table|
        connection.execute("DROP TABLE IF EXISTS #{table} CASCADE")
        connection.execute(<<~SQL)
          CREATE TABLE #{table} (
            id bigint NOT NULL,
            encrypted_email varchar(2048),
            user_id bigint,
            email varchar(2048),
            dw_created_at timestamp,
            dw_updated_at timestamp
          )
        SQL
      end
      connection.execute("ALTER TABLE #{target} ADD PRIMARY KEY (id)")

      # id 2 already exists and should be updated; id 3 is new and should insert.
      connection.execute(<<~SQL)
        INSERT INTO #{target}
        VALUES (2, 'old-enc', 22, 'stale', '2020-01-01', '2020-01-01')
      SQL
      connection.execute(<<~SQL)
        INSERT INTO #{staging} VALUES
          (2, 'new-enc', 222, 'fresh', NULL, NULL),
          (3, 'enc3', 33, 'brand-new', NULL, NULL)
      SQL
    end

    after do
      [target, staging].each do |table|
        connection.execute("DROP TABLE IF EXISTS #{table} CASCADE")
      end
    end

    it 'updates matched rows and inserts unmatched ones' do
      connection.execute(described_class.new.send(:merge_staging_into_target_query))

      rows = connection.select_all("SELECT id, user_id, email FROM #{target} ORDER BY id").to_a

      expect(rows).to eq(
        [
          { 'id' => 2, 'user_id' => 222, 'email' => 'fresh' },
          { 'id' => 3, 'user_id' => 33, 'email' => 'brand-new' },
        ],
      )
    end

    it 'does not overwrite dw_created_at on a matched row' do
      connection.execute(described_class.new.send(:merge_staging_into_target_query))

      created_at = connection.select_value("SELECT dw_created_at FROM #{target} WHERE id = 2")

      expect(created_at.to_date.to_s).to eq('2020-01-01')
    end
  end
end
