require 'rails_helper'

RSpec.describe FraudOps::EmailAddressesZeroEtlBootstrap do
  let(:service) { described_class.new }
  let(:mock_connection) { instance_double(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter) }
  let(:target_table_exists) { false }

  # Every SQL string handed to the connection, in order.
  let(:executed_sql) { [] }

  before do
    allow(DataWarehouseApplicationRecord).to receive(:connection).and_return(mock_connection)
    allow(DataWarehouseApplicationRecord).to receive(:transaction).and_yield

    allow(mock_connection).to receive(:table_exists?).and_return(target_table_exists)
    allow(mock_connection).to receive(:execute) { |sql| executed_sql << sql }
    allow(Rails.logger).to receive(:info)
  end

  describe '#bootstrap' do
    context 'when the target table already exists' do
      let(:target_table_exists) { true }

      it 'logs and exits without touching the table' do
        expect(Rails.logger).to receive(:info).with(
          a_string_matching(/fraudops\.frd_email_addresses_zetl already exists/),
        )

        expect(service.bootstrap).to be(false)
        expect(executed_sql).to be_empty
      end
    end

    context 'when the target table does not exist' do
      it 'creates the table as a copy of the legacy table' do
        service.bootstrap

        expect(executed_sql).to include(
          a_string_matching(/CREATE TABLE fraudops\.frd_email_addresses_zetl \(LIKE/).
            and(a_string_matching(/\(LIKE fraudops\.frd_email_addresses INCLUDING/)),
        )
      end

      it 'adds the primary key that LIKE does not copy' do
        service.bootstrap

        expect(executed_sql).to include(
          a_string_matching(
            /ALTER TABLE fraudops\.frd_email_addresses_zetl ADD PRIMARY KEY \(id\)/,
          ),
        )
      end

      it 'seeds the new table with the existing records' do
        service.bootstrap

        expect(executed_sql).to include(
          a_string_matching(/INSERT INTO fraudops\.frd_email_addresses_zetl SELECT \*/).
            and(a_string_matching(/FROM fraudops\.frd_email_addresses\z/)),
        )
      end

      it 'creates the table before seeding it' do
        service.bootstrap

        create_index = executed_sql.index { |sql| sql.match?(/CREATE TABLE .*_zetl \(LIKE/) }
        seed_index = executed_sql.index { |sql| sql.match?(/INSERT INTO .*_zetl SELECT \*/) }

        expect(create_index).to be < seed_index
      end

      it 'logs what it created and reports that it bootstrapped' do
        expect(Rails.logger).to receive(:info).with(
          'Created fraudops.frd_email_addresses_zetl from fraudops.frd_email_addresses',
        )

        expect(service.bootstrap).to be(true)
      end

      it 'never logs row values' do
        logged = []
        allow(Rails.logger).to receive(:info) { |msg| logged << msg }

        service.bootstrap

        expect(logged).not_to be_empty
        logged.each { |message| expect(message).not_to include('@') }
      end
    end
  end

  # The examples above assert on generated SQL strings, which cannot show that a
  # statement is executable or that LIKE actually carries every column over.
  # This block runs the real DDL against the test database.
  describe 'executed against PostgreSQL' do
    let(:connection) { DataWarehouseApplicationRecord.connection }
    let(:source) { 'fraudops.frd_email_addresses' }
    let(:target) { 'fraudops.frd_email_addresses_zetl' }

    def column_names(table)
      schema, name = table.split('.')
      connection.select_values(<<~SQL)
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = '#{schema}' AND table_name = '#{name}'
        ORDER BY ordinal_position
      SQL
    end

    before do
      allow(DataWarehouseApplicationRecord).to receive(:connection).and_call_original
      allow(DataWarehouseApplicationRecord).to receive(:transaction).and_call_original

      connection.execute('CREATE SCHEMA IF NOT EXISTS fraudops')
      [target, source].each { |table| connection.execute("DROP TABLE IF EXISTS #{table} CASCADE") }
      connection.execute(<<~SQL)
        CREATE TABLE #{source} (
          id bigint NOT NULL,
          encrypted_email varchar(2048),
          user_id bigint,
          email varchar(2048),
          dw_created_at timestamp DEFAULT now(),
          dw_updated_at timestamp DEFAULT now(),
          PRIMARY KEY (id)
        )
      SQL
      connection.execute(<<~SQL)
        INSERT INTO #{source} (id, encrypted_email, user_id, email)
        VALUES (1, 'enc1', 11, 'one'), (2, 'enc2', 22, 'two')
      SQL
    end

    after do
      [target, source].each { |table| connection.execute("DROP TABLE IF EXISTS #{table} CASCADE") }
    end

    it 'gives the new table every column of the source, in the same order' do
      service.bootstrap

      expect(column_names(target)).to eq(column_names(source))
    end

    it 'seeds the new table with the source rows' do
      service.bootstrap

      rows = connection.select_all("SELECT id, user_id, email FROM #{target} ORDER BY id").to_a

      expect(rows).to eq(
        [
          { 'id' => 1, 'user_id' => 11, 'email' => 'one' },
          { 'id' => 2, 'user_id' => 22, 'email' => 'two' },
        ],
      )
    end

    # PostgreSQL's LIKE only copies the primary key under INCLUDING INDEXES, so
    # this passes only because of the explicit ALTER TABLE. requires_new wraps
    # the failing insert in a savepoint, otherwise it would abort the enclosing
    # transaction and the after hook could not drop the tables.
    it 'puts an enforced primary key on the merge key' do
      service.bootstrap

      expect(connection.primary_key(target)).to eq('id')

      expect do
        connection.transaction(requires_new: true) do
          connection.execute("INSERT INTO #{target} (id) VALUES (1)")
        end
      end.to raise_error(ActiveRecord::RecordNotUnique)
    end

    it 'is idempotent' do
      expect(service.bootstrap).to be(true)
      expect(service.bootstrap).to be(false)

      expect(connection.select_value("SELECT COUNT(*) FROM #{target}")).to eq(2)
    end
  end
end
