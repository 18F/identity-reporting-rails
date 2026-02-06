require 'rails_helper'

RSpec.describe PiiRetentionEnforcementJob, type: :job do
  let(:job) { described_class.new }
  let(:mock_connection) { instance_double(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter) }

  let(:config) do
    {
      retention_days: 366,
      schemas: {
        fraudops: {
          excluded_tables: ['excluded_table'],
          timestamp_columns: {
            'custom_table' => 'custom_timestamp',
          },
        },
      },
    }
  end

  before do
    allow(job).to receive(:connection).and_return(mock_connection)
    allow(job).to receive(:config).and_return(config)
    allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(true)
    allow(DataWarehouseApplicationRecord).to receive(:connection).and_return(mock_connection)
    allow(Rails.logger).to receive(:info)
    allow(Rails.logger).to receive(:warn)
    allow(Rails.logger).to receive(:error)
  end

  describe '#perform' do
    context 'when job is disabled' do
      before do
        allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(false)
      end

      it 'skips job execution and logs info' do
        expect(Rails.logger).to receive(:info).with(
          a_string_matching(/Skipped.*fraud_ops_tracker_enabled/),
        )
        job.perform
      end

      it 'does not process any schemas' do
        expect(job).not_to receive(:process_schema)
        job.perform
      end
    end

    context 'when job is enabled' do
      let(:tables_result) do
        instance_double(ActiveRecord::Result, rows: [['encrypted_events'], ['decrypted_events']])
      end
      let(:columns_result) do
        instance_double(ActiveRecord::Result, rows: [['updated_at'], ['created_at'], ['message']])
      end
      let(:delete_result) { instance_double(PG::Result, cmd_tuples: 100) }

      before do
        allow(mock_connection).to receive(:exec_query).and_return(
          tables_result, columns_result, columns_result
        )
        allow(mock_connection).to receive(:execute).and_return(delete_result)
        allow(mock_connection).to receive(:quote_table_name) { |name| "\"#{name}\"" }
        allow(mock_connection).to receive(:quote_column_name) { |name| "\"#{name}\"" }
      end

      it 'logs job started message' do
        expect(Rails.logger).to receive(:info).with(a_string_matching(/Job started/))
        job.perform
      end

      it 'logs job completed message with total deleted count' do
        expect(Rails.logger).to receive(:info).with(
          a_string_matching(/Job completed successfully.*total_deleted/),
        )
        job.perform
      end

      it 'processes all configured schemas' do
        expect(job).to receive(:process_schema).with('fraudops', anything)
        job.perform
      end
    end

    context 'when errors occur during processing' do
      let(:tables_result) do
        instance_double(ActiveRecord::Result, rows: [['table1']])
      end

      before do
        allow(mock_connection).to receive(:exec_query).and_return(tables_result)
        allow(job).to receive(:process_table).and_raise(StandardError, 'Database error')
      end

      it 'logs error and raises exception at the end' do
        expect(Rails.logger).to receive(:error).at_least(:once)
        expect { job.perform }.to raise_error(StandardError, /PII retention enforcement failed/)
      end
    end
  end

  describe '#process_schema' do
    let(:tables_result) do
      instance_double(ActiveRecord::Result, rows: [['table1'], ['excluded_table'], ['table2']])
    end
    let(:columns_result) do
      instance_double(ActiveRecord::Result, rows: [['updated_at']])
    end
    let(:delete_result) { instance_double(PG::Result, cmd_tuples: 50) }

    before do
      allow(mock_connection).to receive(:exec_query).and_return(
        tables_result, columns_result, columns_result
      )
      allow(mock_connection).to receive(:execute).and_return(delete_result)
      allow(mock_connection).to receive(:quote_table_name) { |name| "\"#{name}\"" }
      allow(mock_connection).to receive(:quote_column_name) { |name| "\"#{name}\"" }
    end

    it 'fetches tables in the schema' do
      expect(job).to receive(:fetch_tables_in_schema).with('fraudops').and_call_original
      job.send(:process_schema, 'fraudops', config[:schemas][:fraudops])
    end

    it 'skips excluded tables' do
      expect(job).not_to receive(:process_table).with('fraudops', 'excluded_table', anything)
      job.send(:process_schema, 'fraudops', config[:schemas][:fraudops])
    end

    it 'processes non-excluded tables' do
      expect(job).to receive(:process_table).with('fraudops', 'table1', anything).and_call_original
      expect(job).to receive(:process_table).with('fraudops', 'table2', anything).and_call_original
      job.send(:process_schema, 'fraudops', config[:schemas][:fraudops])
    end

    context 'when included_tables is "*"' do
      let(:schema_config) do
        {
          included_tables: '*',
          excluded_tables: [],
        }
      end

      it 'processes all tables in schema' do
        expect(job).to receive(:process_table).with('fraudops', 'table1', anything)
        expect(job).to receive(:process_table).with('fraudops', 'excluded_table', anything)
        expect(job).to receive(:process_table).with('fraudops', 'table2', anything)
        job.send(:process_schema, 'fraudops', schema_config)
      end
    end

    context 'when included_tables is an array of specific tables' do
      let(:schema_config) do
        {
          included_tables: ['table1'],
          excluded_tables: [],
        }
      end

      it 'processes only included tables' do
        expect(job).to receive(:process_table).with('fraudops', 'table1', anything)
        expect(job).not_to receive(:process_table).with('fraudops', 'table2', anything)
        expect(job).not_to receive(:process_table).with('fraudops', 'excluded_table', anything)
        job.send(:process_schema, 'fraudops', schema_config)
      end
    end

    context 'when both included_tables and excluded_tables are specified' do
      let(:schema_config) do
        {
          included_tables: ['table1', 'excluded_table', 'table2'],
          excluded_tables: ['excluded_table'],
        }
      end

      it 'processes only included tables minus excluded tables' do
        expect(job).to receive(:process_table).with('fraudops', 'table1', anything)
        expect(job).not_to receive(:process_table).with('fraudops', 'excluded_table', anything)
        expect(job).to receive(:process_table).with('fraudops', 'table2', anything)
        job.send(:process_schema, 'fraudops', schema_config)
      end
    end

    context 'when included_tables is an empty array' do
      let(:schema_config) do
        {
          included_tables: [],
          excluded_tables: [],
        }
      end

      it 'processes all tables (treats empty array as wildcard)' do
        expect(job).to receive(:process_table).with('fraudops', 'table1', anything)
        expect(job).to receive(:process_table).with('fraudops', 'excluded_table', anything)
        expect(job).to receive(:process_table).with('fraudops', 'table2', anything)
        job.send(:process_schema, 'fraudops', schema_config)
      end
    end

    context 'when included_tables is nil (not specified)' do
      let(:schema_config) do
        {
          excluded_tables: [],
        }
      end

      it 'processes all tables (backward compatible with existing configs)' do
        expect(job).to receive(:process_table).with('fraudops', 'table1', anything)
        expect(job).to receive(:process_table).with('fraudops', 'excluded_table', anything)
        expect(job).to receive(:process_table).with('fraudops', 'table2', anything)
        job.send(:process_schema, 'fraudops', schema_config)
      end
    end

    context 'when included_tables contains non-existent table' do
      let(:schema_config) do
        {
          included_tables: ['table1', 'nonexistent_table'],
          excluded_tables: [],
        }
      end

      it 'processes only existing tables from included list' do
        expect(job).to receive(:process_table).with('fraudops', 'table1', anything)
        expect(job).not_to receive(:process_table).with('fraudops', 'nonexistent_table', anything)
        job.send(:process_schema, 'fraudops', schema_config)
      end
    end
  end

  describe '#filter_included_tables' do
    let(:tables) { ['table1', 'table2', 'table3'] }

    context 'when included_tables is "*"' do
      it 'returns all tables' do
        result = job.send(:filter_included_tables, tables, '*')
        expect(result).to eq(tables)
      end
    end

    context 'when included_tables is ["*"]' do
      it 'returns all tables' do
        result = job.send(:filter_included_tables, tables, ['*'])
        expect(result).to eq(tables)
      end
    end

    context 'when included_tables is nil' do
      it 'returns all tables' do
        result = job.send(:filter_included_tables, tables, nil)
        expect(result).to eq(tables)
      end
    end

    context 'when included_tables is empty array' do
      it 'returns all tables' do
        result = job.send(:filter_included_tables, tables, [])
        expect(result).to eq(tables)
      end
    end

    context 'when included_tables is a specific array' do
      it 'returns only specified tables' do
        result = job.send(:filter_included_tables, tables, ['table1', 'table3'])
        expect(result).to eq(['table1', 'table3'])
      end
    end

    context 'when included_tables contains non-existent tables' do
      it 'returns only existing tables from the list' do
        result = job.send(:filter_included_tables, tables, ['table1', 'nonexistent'])
        expect(result).to eq(['table1'])
      end
    end
  end

  describe '#resolve_timestamp_column' do
    let(:timestamp_columns) { { 'custom_table' => 'custom_timestamp' } }

    context 'when table has updated_at column' do
      let(:columns_result) do
        instance_double(ActiveRecord::Result, rows: [['id'], ['updated_at'], ['created_at']])
      end

      before do
        allow(mock_connection).to receive(:exec_query).and_return(columns_result)
      end

      it 'returns updated_at' do
        result = job.send(:resolve_timestamp_column, 'fraudops', 'events', timestamp_columns)
        expect(result).to eq('updated_at')
      end
    end

    context 'when table has only created_at column' do
      let(:columns_result) do
        instance_double(ActiveRecord::Result, rows: [['id'], ['created_at'], ['message']])
      end

      before do
        allow(mock_connection).to receive(:exec_query).and_return(columns_result)
      end

      it 'returns created_at' do
        result = job.send(:resolve_timestamp_column, 'fraudops', 'events', timestamp_columns)
        expect(result).to eq('created_at')
      end
    end

    context 'when table has neither updated_at nor created_at' do
      let(:columns_result) do
        instance_double(ActiveRecord::Result, rows: [['id'], ['message']])
      end

      before do
        allow(mock_connection).to receive(:exec_query).and_return(columns_result)
      end

      it 'returns YAML configured column if present' do
        result = job.send(:resolve_timestamp_column, 'fraudops', 'custom_table', timestamp_columns)
        expect(result).to eq('custom_timestamp')
      end

      it 'returns nil if no YAML config for table' do
        result = job.send(:resolve_timestamp_column, 'fraudops', 'unknown_table', timestamp_columns)
        expect(result).to be_nil
      end
    end
  end

  describe '#process_table' do
    let(:timestamp_columns) { {} }

    context 'when timestamp column cannot be resolved' do
      before do
        allow(job).to receive(:resolve_timestamp_column).and_return(nil)
      end

      it 'logs warning and skips table' do
        expect(Rails.logger).to receive(:warn).with(a_string_matching(/No timestamp column found/))
        expect(job).not_to receive(:delete_expired_records)
        job.send(:process_table, 'fraudops', 'bad_table', timestamp_columns)
      end
    end

    context 'when timestamp column is resolved' do
      let(:delete_result) { instance_double(PG::Result, cmd_tuples: 75) }

      before do
        allow(job).to receive(:resolve_timestamp_column).and_return('updated_at')
        allow(mock_connection).to receive(:quote_table_name) { |name| "\"#{name}\"" }
        allow(mock_connection).to receive(:quote_column_name) { |name| "\"#{name}\"" }
        allow(mock_connection).to receive(:execute).and_return(delete_result)
      end

      it 'deletes expired records' do
        expect(job).to receive(:delete_expired_records).with(
          'fraudops', 'events', 'updated_at'
        ).and_call_original
        job.send(:process_table, 'fraudops', 'events', timestamp_columns)
      end

      it 'logs completion with deleted count' do
        expect(Rails.logger).to receive(:info).with(
          a_string_matching(/Retention enforcement completed.*deleted_count.*75/),
        )
        job.send(:process_table, 'fraudops', 'events', timestamp_columns)
      end
    end

    context 'when deletion fails' do
      before do
        allow(job).to receive(:resolve_timestamp_column).and_return('updated_at')
        allow(job).to receive(:delete_expired_records).and_raise(StandardError, 'Connection lost')
      end

      it 'logs error and does not raise' do
        expect(Rails.logger).to receive(:error).with(
          a_string_matching(/Error processing table.*Connection lost/),
        )
        expect do
          job.send(:process_table, 'fraudops', 'events', timestamp_columns)
        end.not_to raise_error
      end
    end
  end

  describe '#fetch_tables_in_schema' do
    let(:tables_result) do
      instance_double(ActiveRecord::Result, rows: [['table1'], ['table2'], ['table3']])
    end

    before do
      allow(mock_connection).to receive(:exec_query).and_return(tables_result)
    end

    it 'queries information_schema for tables' do
      expect(mock_connection).to receive(:exec_query).with(
        a_string_matching(/information_schema\.tables.*table_schema.*BASE TABLE/),
      )
      job.send(:fetch_tables_in_schema, 'fraudops')
    end

    it 'returns array of table names' do
      result = job.send(:fetch_tables_in_schema, 'fraudops')
      expect(result).to eq(['table1', 'table2', 'table3'])
    end
  end

  describe '#fetch_columns_for_table' do
    let(:columns_result) do
      instance_double(ActiveRecord::Result, rows: [['id'], ['name'], ['updated_at']])
    end

    before do
      allow(mock_connection).to receive(:exec_query).and_return(columns_result)
    end

    it 'queries information_schema for columns' do
      expect(mock_connection).to receive(:exec_query).with(
        a_string_matching(/information_schema\.columns.*table_schema.*table_name/),
      )
      job.send(:fetch_columns_for_table, 'fraudops', 'events')
    end

    it 'returns array of column names' do
      result = job.send(:fetch_columns_for_table, 'fraudops', 'events')
      expect(result).to eq(['id', 'name', 'updated_at'])
    end
  end

  describe '#delete_expired_records' do
    let(:delete_result) { instance_double(PG::Result, cmd_tuples: 150) }

    before do
      allow(mock_connection).to receive(:quote_table_name) { |name| "\"#{name}\"" }
      allow(mock_connection).to receive(:quote_column_name) { |name| "\"#{name}\"" }
      allow(mock_connection).to receive(:execute).and_return(delete_result)
    end

    it 'executes DELETE query with correct retention period' do
      expected_pattern = %r{DELETE\ FROM\ "fraudops"\."events"
                           \s+WHERE\ "updated_at"\ <\ CURRENT_DATE\ -\ 366}x

      expect(mock_connection).to receive(:execute).with(a_string_matching(expected_pattern))
      job.send(:delete_expired_records, 'fraudops', 'events', 'updated_at')
    end

    it 'returns the number of deleted records' do
      result = job.send(:delete_expired_records, 'fraudops', 'events', 'updated_at')
      expect(result).to eq(150)
    end
  end

  describe '#extract_deleted_count' do
    context 'when result responds to cmd_tuples' do
      let(:result) { instance_double(PG::Result, cmd_tuples: 42) }

      it 'returns cmd_tuples value' do
        expect(job.send(:extract_deleted_count, result)).to eq(42)
      end
    end

    context 'when result is an Integer' do
      it 'returns the integer value' do
        expect(job.send(:extract_deleted_count, 99)).to eq(99)
      end
    end

    context 'when result has rows' do
      let(:result) { instance_double(ActiveRecord::Result, rows: [[25]]) }

      before do
        allow(result).to receive(:respond_to?).with(:cmd_tuples).and_return(false)
        allow(result).to receive(:is_a?).with(Integer).and_return(false)
        allow(result).to receive(:respond_to?).with(:rows).and_return(true)
      end

      it 'returns first row first column as integer' do
        expect(job.send(:extract_deleted_count, result)).to eq(25)
      end
    end

    context 'when result format is unknown' do
      let(:result) { Object.new }

      before do
        allow(result).to receive(:respond_to?).and_return(false)
        allow(result).to receive(:is_a?).and_return(false)
      end

      it 'returns 0' do
        expect(job.send(:extract_deleted_count, result)).to eq(0)
      end
    end
  end

  describe '#config' do
    let(:job_without_mocked_config) { described_class.new }
    let(:config_path) { Rails.root.join('config', 'pii_retention.yml') }

    it 'loads config from YAML file' do
      expect(YAML).to receive(:safe_load_file).with(
        config_path, symbolize_names: true
      ).and_return(config)
      job_without_mocked_config.send(:config)
    end
  end

  describe '#retention_days' do
    it 'returns configured retention days' do
      expect(job.send(:retention_days)).to eq(366)
    end

    context 'when retention_days is not configured' do
      before do
        allow(job).to receive(:config).and_return({ schemas: {} })
      end

      it 'returns default of 366' do
        expect(job.send(:retention_days)).to eq(366)
      end
    end
  end

  describe '#job_enabled?' do
    it 'returns value from IdentityConfig.store.fraud_ops_tracker_enabled' do
      allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(true)
      expect(job.send(:job_enabled?)).to be(true)

      allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(false)
      expect(job.send(:job_enabled?)).to be(false)
    end
  end

  describe '#log_format' do
    it 'returns JSON formatted log with job name and message' do
      result = job.send(:log_format, 'Test message', extra: 'data')
      parsed = JSON.parse(result)

      expect(parsed['job']).to eq('PiiRetentionEnforcementJob')
      expect(parsed['message']).to eq('Test message')
      expect(parsed['extra']).to eq('data')
    end
  end
end
