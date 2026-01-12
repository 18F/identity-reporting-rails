require 'rails_helper'

RSpec.describe RedshiftSystemTableSyncJob, type: :job do
  let(:job) { RedshiftSystemTableSyncJob.new }
  let(:source_schema) { 'test_pg_catalog' }
  let(:target_schema) { 'system_tables' }
  let(:source_table) { 'stl_query' }
  let(:target_table) { 'stl_query' }
  let(:source_table2) { 'stl_some_table' }
  let(:target_table2) { 'stl_some_table' }
  let(:source_table_with_schema) { "#{source_schema}.#{source_table}" }
  let(:target_table_with_schema) { "#{target_schema}.#{target_table}" }
  let(:source_table_with_schema2) { "#{source_schema}.#{source_table2}" }
  let(:target_table_with_schema2) { "#{target_schema}.#{target_table2}" }
  let(:timestamp_column) { 'endtime' }
  let(:column_keys) { ['userid', 'query'] }
  let(:last_sync_time) { Time.zone.now - 6.days }
  let!(:file_path) { Rails.root.join('spec', 'fixtures', 'redshift_system_tables.yml') }
  let!(:file_path2) { Rails.root.join('spec', 'fixtures', 'redshift_system_tables2.yml') }
  let(:table) do
    {
      'source_table' => source_table,
      'target_table' => source_table,
      'source_schema' => source_schema,
      'target_schema' => target_schema,
      'column_keys' => column_keys,
      'timestamp_column' => timestamp_column,
    }
  end
  let(:table2) do
    {
      'source_table' => source_table2,
      'target_table' => target_table2,
      'source_schema' => source_schema,
      'target_schema' => target_schema,
      'column_keys' => column_keys,
      'timestamp_column' => timestamp_column,
    }
  end

  before do
    job.send(:setup_instance_variables, table)
    allow(job).to receive(:config_file_path).and_return(file_path)
  end

  describe '#upsert_data' do
    context 'when using Redshift as the adapter' do
      before do
        connection = DataWarehouseApplicationRecord.connection
        allow(connection).to receive(:adapter_name).and_return('redshift')
        allow(job).to receive(:perform_merge_upsert)
      end

      it 'calls #perform_merge_upsert' do
        expect(job).to receive(:perform_merge_upsert)
        job.send(:upsert_data)
      end
    end
  end

  describe '#perform_merge_upsert' do
    before do
      allow(DataWarehouseApplicationRecord.connection).to receive(:execute)
      allow(Rails.logger).to receive(:info)
    end

    it 'executes a MERGE statement with proper conditions' do
      job.send(:create_target_table)

      expected_query = <<-QUERY.squish
        MERGE INTO system_tables.stl_query
        USING(
          SELECT * FROM (
            SELECT userid, query, CAST(label AS VARCHAR(MAX)) AS label, xid, pid, CAST(database AS VARCHAR(MAX)) AS database,
            CAST(querytxt AS VARCHAR(MAX)) AS querytxt, starttime, endtime, aborted, insert_pristine, concurency_scalling_status,
            ROW_NUMBER() OVER (PARTITION BY stl_query.userid, stl_query.query) AS row_num
            FROM stl_query
          )
          WHERE row_num = 1
        ) AS source ON stl_query.userid = source.userid AND stl_query.query = source.query
        WHEN MATCHED
        THEN UPDATE SET userid = source.userid, query = source.query, label = source.label, xid = source.xid, pid = source.pid,
        database = source.database, querytxt = source.querytxt, starttime = source.starttime, endtime = source.endtime,
        aborted = source.aborted, insert_pristine = source.insert_pristine, concurency_scalling_status = source.concurency_scalling_status
        WHEN NOT MATCHED THEN INSERT (userid, query, label, xid, pid, database, querytxt, starttime, endtime, aborted, insert_pristine, concurency_scalling_status)
        VALUES (source.userid, source.query, source.label, source.xid, source.pid, source.database, source.querytxt, source.starttime, source.endtime, source.aborted,
        source.insert_pristine, source.concurency_scalling_status);
      QUERY

      expect(DataWarehouseApplicationRecord.connection).to receive(:execute).with(expected_query)
      job.send(:perform_merge_upsert)
    end
  end

  describe '#table_definitions' do
    it 'return table definitions from the config file' do
      table_definitions = job.send(:table_definitions)
      expect(table_definitions).to match_array(
        [{ 'source_table' => 'stl_query',
           'target_table' => 'stl_query',
           'source_schema' => 'test_pg_catalog',
           'target_schema' => 'system_tables',
           'column_keys' => ['userid', 'query'],
           'timestamp_column' => 'endtime' }],
      )
    end
  end

  describe '#target_table_exists?' do
    it 'returns true if the source table exists' do
      source = DataWarehouseApplicationRecord.connection.table_exists?(source_table_with_schema)
      expect(source).to be true
    end

    it 'returns false if the target table not exists' do
      target = DataWarehouseApplicationRecord.connection.table_exists?(target_table_with_schema)
      expect(target).to be false
    end
  end

  describe '#create_target_table' do
    it 'creates target tables, and log message' do
      allow(DataWarehouseApplicationRecord.connection).to receive(:table_exists?).
        with(target_table_with_schema).and_return(false)
      allow(DataWarehouseApplicationRecord.connection).to receive(:execute)

      allow(Rails.logger).to receive(:info).and_call_original
      expected_msgs = [
        {
          job: 'RedshiftSystemTableSyncJob',
          success: true,
          message: 'Created target table stl_query',
          target_table: 'stl_query',
        }.to_json,
        {
          job: 'RedshiftSystemTableSyncJob',
          success: true,
          message: 'Schema system_tables created',
        }.to_json,
        {
          job: 'RedshiftSystemTableSyncJob',
          success: true,
          message: 'Columns fetched for stl_query',
        }.to_json,
      ]

      expected_msgs.each do |msg|
        expect(Rails.logger).to receive(:info).with(msg)
      end

      expect(DataWarehouseApplicationRecord.connection).to receive(:create_table).
        with(target_table_with_schema, hash_including(id: false))

      job.send(:create_target_table)
    end
  end

  describe '#sync_target_and_source_table_schemas' do
    context 'when there are missing columns from target table' do
      before do
        # Create source table with no new columns that are missing from target
        DataWarehouseApplicationRecord.connection.create_table(
          source_table_with_schema2,
          id: false,
        ) do |t|
          t.column 'userid', 'integer'
          t.column 'query', 'integer'
          t.column 'starttime', 'timestamp without time zone'
          t.column 'endtime', 'timestamp without time zone'
        end
        job.send(:setup_instance_variables, table2)
        job.send(:create_target_table)
      end

      it 'adds missing columns to target table, and log message' do
        # Add a new columns to source table to simulate missing column scenario
        DataWarehouseApplicationRecord.connection.add_column(
          source_table_with_schema2,
          'new_column1',
          'integer',
        )
        DataWarehouseApplicationRecord.connection.add_column(
          source_table_with_schema2,
          'new_column2',
          'varchar',
          limit: 130,
        )
        allow(job).to receive(:missing_system_table_columns).and_return(
          ['new_column1',
           'new_column2'],
        )
        allow(job).to receive(:get_source_table_ddl).and_return(
          'CREATE TABLE test_pg_catalog.stl_some_table (
            userid integer,
            query integer,
            starttime timestamp without time zone,
            endtime timestamp without time zone,
            new_column1 integer,
            new_column2 varchar(130)
          );',
        )
        allow(Rails.logger).to receive(:info).and_call_original
        msg = {
          job: 'RedshiftSystemTableSyncJob',
          success: true,
          message: "Synchronized schema for #{target_table2}",
          added_columns: ['new_column1', 'new_column2'],
        }
        expect(Rails.logger).to receive(:info).with(msg.to_json)

        job.send(:sync_target_and_source_table_schemas)
      end
    end
  end

  describe '#create_schema_if_not_exists' do
    it 'creates target schema, and log message' do
      allow(Rails.logger).to receive(:info).and_call_original
      msg = {
        job: 'RedshiftSystemTableSyncJob',
        success: true,
        message: 'Schema system_tables created',
      }
      expect(Rails.logger).to receive(:info).with(msg.to_json)

      job.send(:create_schema_if_not_exists)
    end

    context 'when schema already exists' do
      let(:target_schema) { 'pg_catalog' }

      it 'return target schema if already exists, and log message' do
        allow(Rails.logger).to receive(:info).and_call_original
        msg = {
          job: 'RedshiftSystemTableSyncJob',
          success: true,
          message: 'Schema pg_catalog already created',
        }
        expect(Rails.logger).to receive(:info).with(msg.to_json)

        job.send(:create_schema_if_not_exists)
      end
    end
  end

  describe '#fetch_source_columns' do
    it 'returns column information' do
      columns = job.send(:fetch_source_columns)

      expect(columns).to match_array(
        [
          { 'column' => 'userid', 'type' => 'integer' },
          { 'column' => 'query', 'type' => 'integer' },
          { 'column' => 'label', 'type' => 'character varying' },
          { 'column' => 'xid', 'type' => 'bigint' },
          { 'column' => 'pid', 'type' => 'integer' },
          { 'column' => 'database', 'type' => 'character varying' },
          { 'column' => 'querytxt', 'type' => 'character varying' },
          { 'column' => 'starttime', 'type' => 'timestamp without time zone' },
          { 'column' => 'endtime', 'type' => 'timestamp without time zone' },
          { 'column' => 'aborted', 'type' => 'integer' },
          { 'column' => 'insert_pristine', 'type' => 'integer' },
          { 'column' => 'concurency_scalling_status', 'type' => 'integer' },
        ],
      )
    end
  end

  describe '#update_sync_time' do
    it 'updates the sync time in SystemTablesSyncMetadata' do
      job.send(:update_sync_time)

      sync_metadata = SystemTablesSyncMetadata.find_by(table_name: target_table)
      expect(sync_metadata).not_to be_nil
      expect(sync_metadata.last_sync_time).to be_within(1.second).of(Time.zone.now)
      expect(sync_metadata.table_name).to eq target_table
    end
  end

  describe 'redshift_data_type' do
    context 'when datatype is :json or :jsonb' do
      it 'returns :super' do
        expect(job.send(:redshift_data_type, 'json')).to eq('super')
        expect(job.send(:redshift_data_type, 'jsonb')).to eq('super')
      end
    end

    context 'when datatype is :text' do
      it 'returns VARCHAR(MAX)' do
        expect(job.send(:redshift_data_type, 'text')).to eq('VARCHAR(MAX)')
      end
    end

    context 'when datatype is :character' do
      it 'returns VARCHAR(MAX)' do
        expect(job.send(:redshift_data_type, 'character')).to eq('VARCHAR(MAX)')
      end
    end

    context 'when datatype is :char' do
      it 'returns VARCHAR(MAX)' do
        expect(job.send(:redshift_data_type, 'char')).to eq('VARCHAR(MAX)')
      end
    end

    context 'when datatypes have a length' do
      it 'returns VARCHAR(length)' do
        expect(job.send(:redshift_data_type, 'character(10)')).to eq('VARCHAR(10)')
      end
    end

    context 'when datatype is not :json or :jsonb or :text or :character or :char' do
      it 'returns the input datatype symbol' do
        expect(job.send(:redshift_data_type, 'integer')).to eq('integer')
        expect(job.send(:redshift_data_type, 'string')).to eq('string')
      end
    end
  end
end
