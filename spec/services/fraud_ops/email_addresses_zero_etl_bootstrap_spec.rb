require 'rails_helper'

RSpec.describe FraudOps::EmailAddressesZeroEtlBootstrap do
  let(:service) { described_class.new }
  let(:mock_connection) { instance_double(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter) }
  let(:target_table_exists) { true }
  let(:target_has_rows) { false }

  # Every SQL string handed to the connection's #execute, in order.
  let(:executed_sql) { [] }

  before do
    allow(DataWarehouseApplicationRecord).to receive(:connection).and_return(mock_connection)
    allow(DataWarehouseApplicationRecord).to receive(:transaction).and_yield

    allow(mock_connection).to receive(:table_exists?).and_return(target_table_exists)
    allow(mock_connection).to receive(:select_value).and_return(target_has_rows ? 1 : nil)
    allow(mock_connection).to receive(:execute) { |sql| executed_sql << sql }
    allow(Rails.logger).to receive(:info)
  end

  describe '#bootstrap' do
    context 'when the target table does not exist' do
      let(:target_table_exists) { false }

      it 'logs the skip and does no work' do
        expect(Rails.logger).to receive(:info).with(
          a_string_matching(/fraudops\.frd_email_addresses_zetl does not exist/),
        )

        expect(service.bootstrap).to be(false)
        expect(executed_sql).to be_empty
      end

      it 'never provisions the table itself' do
        service.bootstrap

        expect(executed_sql).not_to include(a_string_matching(/CREATE TABLE/i))
      end
    end

    context 'when the target table already has rows' do
      let(:target_table_exists) { true }
      let(:target_has_rows) { true }

      it 'logs the skip and does not seed' do
        expect(Rails.logger).to receive(:info).with(
          a_string_matching(/fraudops\.frd_email_addresses_zetl already has rows/),
        )

        expect(service.bootstrap).to be(false)
        expect(executed_sql).not_to include(a_string_matching(/INSERT INTO/i))
      end
    end

    context 'when the target table exists and is empty' do
      let(:target_table_exists) { true }
      let(:target_has_rows) { false }

      it 'seeds the target from the legacy table' do
        service.bootstrap

        expect(executed_sql).to include(
          a_string_matching(/INSERT INTO fraudops\.frd_email_addresses_zetl SELECT \*/).
            and(a_string_matching(/FROM fraudops\.frd_email_addresses\z/)),
        )
      end

      it 'never creates the table' do
        service.bootstrap

        expect(executed_sql).not_to include(a_string_matching(/CREATE TABLE/i))
      end

      it 'runs the seed as the pii_reader database user and restores the session after' do
        service.bootstrap

        index_of = ->(needle) { executed_sql.index { |sql| sql.include?(needle) } }
        set_index = index_of.call('SET SESSION AUTHORIZATION pii_reader')
        insert_index = executed_sql.index { |sql| sql.match?(/INSERT INTO .*_zetl SELECT \*/) }
        reset_index = index_of.call('RESET SESSION AUTHORIZATION')

        expect([set_index, insert_index, reset_index]).to all(be_present)
        expect(set_index).to be < insert_index
        expect(insert_index).to be < reset_index
      end

      it 'restores the session user even when the seed fails' do
        allow(mock_connection).to receive(:execute) do |sql|
          executed_sql << sql
          raise ActiveRecord::StatementInvalid, 'boom' if sql.include?('INSERT INTO')
        end

        expect { service.bootstrap }.to raise_error(ActiveRecord::StatementInvalid)
        expect(executed_sql.last).to match(/RESET SESSION AUTHORIZATION/)
      end

      it 'logs what it seeded and reports that it bootstrapped' do
        expect(Rails.logger).to receive(:info).with(
          'Seeded fraudops.frd_email_addresses_zetl from fraudops.frd_email_addresses',
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
  # statement is executable or that the seed actually copies rows. This block
  # runs the real code path against the test database.
  describe 'executed against PostgreSQL' do
    let(:connection) { DataWarehouseApplicationRecord.connection }
    let(:source) { 'fraudops.frd_email_addresses' }
    let(:target) { 'fraudops.frd_email_addresses_zetl' }
    let(:seed_user) { described_class::INSERT_DB_USER }

    def columns_sql
      <<~SQL
        id bigint NOT NULL,
        encrypted_email varchar(2048),
        user_id bigint,
        email varchar(2048),
        dw_created_at timestamp DEFAULT now(),
        dw_updated_at timestamp DEFAULT now(),
        PRIMARY KEY (id)
      SQL
    end

    before do
      allow(DataWarehouseApplicationRecord).to receive(:connection).and_call_original
      allow(DataWarehouseApplicationRecord).to receive(:transaction).and_call_original

      connection.execute('CREATE SCHEMA IF NOT EXISTS fraudops')
      [target, source].each { |table| connection.execute("DROP TABLE IF EXISTS #{table} CASCADE") }
      connection.execute("CREATE TABLE #{source} (#{columns_sql})")
      connection.execute("CREATE TABLE #{target} (#{columns_sql})")
      connection.execute(<<~SQL)
        INSERT INTO #{source} (id, encrypted_email, user_id, email)
        VALUES (1, 'enc1', 11, 'one'), (2, 'enc2', 22, 'two')
      SQL

      # The seed runs `SET SESSION AUTHORIZATION pii_reader`, so the role must
      # exist and be able to read the source and write the target. Roles are
      # cluster-global, so create it only if missing and leave it in place.
      if connection.select_value("SELECT 1 FROM pg_roles WHERE rolname = '#{seed_user}'").nil?
        connection.execute("CREATE ROLE #{seed_user}")
      end
      connection.execute("GRANT USAGE ON SCHEMA fraudops TO #{seed_user}")
      connection.execute("GRANT SELECT ON #{source} TO #{seed_user}")
      connection.execute("GRANT INSERT ON #{target} TO #{seed_user}")
    end

    after do
      [target, source].each { |table| connection.execute("DROP TABLE IF EXISTS #{table} CASCADE") }
    end

    it 'seeds the empty target with the source rows and reports success' do
      expect(service.bootstrap).to be(true)

      rows = connection.select_all("SELECT id, user_id, email FROM #{target} ORDER BY id").to_a

      expect(rows).to eq(
        [
          { 'id' => 1, 'user_id' => 11, 'email' => 'one' },
          { 'id' => 2, 'user_id' => 22, 'email' => 'two' },
        ],
      )
    end

    it 'restores the connection to its original user after seeding' do
      original_user = connection.select_value('SELECT current_user')

      service.bootstrap

      expect(connection.select_value('SELECT current_user')).to eq(original_user)
    end

    it 'does nothing when the target already has rows' do
      connection.execute("INSERT INTO #{target} (id, user_id, email) VALUES (99, 999, 'kept')")

      expect(service.bootstrap).to be(false)

      rows = connection.select_all("SELECT id, user_id, email FROM #{target} ORDER BY id").to_a
      expect(rows).to eq([{ 'id' => 99, 'user_id' => 999, 'email' => 'kept' }])
    end

    it 'does nothing when the target does not exist' do
      connection.execute("DROP TABLE IF EXISTS #{target} CASCADE")

      expect(service.bootstrap).to be(false)
    end
  end
end
