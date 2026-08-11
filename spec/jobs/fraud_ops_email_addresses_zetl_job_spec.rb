require 'rails_helper'

RSpec.describe FraudOpsEmailAddressesZetlJob, type: :job do
  let(:job) { described_class.new }
  let(:mock_connection) { instance_double(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter) }
  let(:target_table_exists) { true }
  let(:adapter_name) { 'PostgreSQL' }

  # Every SQL string handed to the connection, in order.
  let(:executed_sql) { [] }

  before do
    allow(DataWarehouseApplicationRecord).to receive(:connection).and_return(mock_connection)
    allow(DataWarehouseApplicationRecord).to receive(:transaction).and_yield
    allow(IdentityConfig.store).to receive(:zero_etl_enabled).and_return(true)

    allow(mock_connection).to receive(:adapter_name).and_return(adapter_name)
    allow(mock_connection).to receive(:table_exists?).and_return(target_table_exists)
    allow(mock_connection).to receive(:quote) { |value| "'#{value}'" }
    allow(mock_connection).to receive(:execute) { |sql| executed_sql << sql }

    allow(Rails.logger).to receive(:info)
    allow(Rails.logger).to receive(:error)
  end

  describe '#perform' do
    context 'when zero_etl_enabled is false' do
      before { allow(IdentityConfig.store).to receive(:zero_etl_enabled).and_return(false) }

      it 'logs that it skipped and executes no SQL' do
        expect(Rails.logger).to receive(:info).with(
          a_string_matching(/zero_etl_enabled is false/),
        )
        expect(mock_connection).not_to receive(:execute)

        job.perform
      end

      it 'does not check for or bootstrap the target table' do
        expect(mock_connection).not_to receive(:table_exists?)

        job.perform
      end
    end

    context 'when the target table does not exist' do
      let(:target_table_exists) { false }

      it 'creates the table as a copy of the legacy table' do
        job.perform

        expect(executed_sql).to include(
          a_string_matching(
            /CREATE TABLE fraudops\.frd_email_addresses_zetl \(LIKE fraudops\.frd_email_addresses/,
          ),
        )
      end

      it 'adds the primary key that LIKE does not copy' do
        job.perform

        expect(executed_sql).to include(
          a_string_matching(
            /ALTER TABLE fraudops\.frd_email_addresses_zetl ADD PRIMARY KEY \(id\)/,
          ),
        )
      end

      it 'seeds the new table with the existing records' do
        job.perform

        expect(executed_sql).to include(
          a_string_matching(/INSERT INTO fraudops\.frd_email_addresses_zetl SELECT \*/).
            and(a_string_matching(/FROM fraudops\.frd_email_addresses\z/)),
        )
      end

      it 'creates the table before seeding it' do
        job.perform

        create_index = executed_sql.index { |sql| sql.match?(/CREATE TABLE .*_zetl \(LIKE/) }
        seed_index = executed_sql.index { |sql| sql.match?(/INSERT INTO .*_zetl SELECT \*/) }

        expect(create_index).to be < seed_index
      end
    end

    context 'when the target table already exists' do
      let(:target_table_exists) { true }

      it 'does not create or seed the table' do
        job.perform

        expect(executed_sql).not_to include(a_string_matching(/CREATE TABLE .*_zetl \(LIKE/))
        expect(executed_sql).not_to include(a_string_matching(/INSERT INTO .*_zetl SELECT \*/))
      end

      it 'still performs the merge' do
        job.perform

        expect(executed_sql).to include(a_string_matching(/frd_email_addresses_zetl_staging/))
      end
    end

    describe 'staging load' do
      let(:staging_load_sql) do
        job.perform
        executed_sql.find do |sql|
          sql.include?('INSERT INTO fraudops.frd_email_addresses_zetl_staging')
        end
      end

      it 'drops any leftover staging table before creating it' do
        job.perform

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
        job.perform

        expect(executed_sql.last).to match(/DROP TABLE IF EXISTS .*_staging/)
      end
    end

    describe 'lookback window', freeze_time: true do
      it 'defaults to a 15 minute lookback' do
        job.perform

        staging_load = executed_sql.find { |sql| sql.match?(/INSERT INTO .*_staging/) }
        expect(staging_load).to include(15.minutes.ago.utc.to_s)
      end

      it 'honors a custom lookback_minutes argument' do
        job.perform(lookback_minutes: 60)

        staging_load = executed_sql.find { |sql| sql.match?(/INSERT INTO .*_staging/) }
        expect(staging_load).to include(60.minutes.ago.utc.to_s)
      end
    end

    describe 'merge dialect' do
      context 'on Redshift' do
        let(:adapter_name) { 'Redshift' }

        it 'uses the MERGE construct keyed on id' do
          job.perform

          merge_sql = executed_sql.find { |sql| sql.include?('MERGE INTO') }

          expect(merge_sql).to match(/MERGE INTO fraudops\.frd_email_addresses_zetl/)
          expect(merge_sql).to match(
            /ON fraudops\.frd_email_addresses_zetl\.id = source\.id/,
          )
          expect(merge_sql).to match(/WHEN MATCHED THEN UPDATE SET/)
          expect(merge_sql).to match(/WHEN NOT MATCHED THEN INSERT/)
        end

        it 'preserves dw_created_at on update' do
          job.perform

          merge_sql = executed_sql.find { |sql| sql.include?('MERGE INTO') }
          matched_clause = merge_sql[/WHEN MATCHED THEN UPDATE SET(.*?)WHEN NOT MATCHED/m, 1]

          expect(matched_clause).not_to include('dw_created_at')
          expect(matched_clause).to include('dw_updated_at')
        end
      end

      context 'on PostgreSQL' do
        let(:adapter_name) { 'PostgreSQL' }

        it 'uses the CTE-based upsert instead of MERGE' do
          job.perform

          expect(executed_sql).not_to include(a_string_matching(/MERGE INTO/))
          expect(executed_sql).to include(a_string_matching(/WITH updated AS \( UPDATE/))
          expect(executed_sql).to include(a_string_matching(/WHERE NOT EXISTS/))
        end
      end
    end

    context 'when a query fails' do
      before do
        allow(mock_connection).to receive(:execute).
          and_raise(ActiveRecord::StatementInvalid, 'relation does not exist')
      end

      it 'logs the error and re-raises so GoodJob marks the job failed' do
        expect(Rails.logger).to receive(:error).with(a_string_matching(/Job failed/))

        expect { job.perform }.to raise_error(ActiveRecord::StatementInvalid)
      end
    end

    describe 'PII safety' do
      it 'never logs the generated SQL or any row values' do
        logged = []
        allow(Rails.logger).to receive(:info) { |msg| logged << msg }

        job.perform

        expect(logged).not_to be_empty
        logged.each do |message|
          expect(message).not_to include('decrypt_udf')
          expect(message).not_to include('encrypted_email')
          expect(message).not_to include('@')
        end
      end
    end
  end
end
