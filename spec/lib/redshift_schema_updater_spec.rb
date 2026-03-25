require 'rails_helper'
require 'redshift_schema_updater'

RSpec.describe RedshiftSchemaUpdater do
  let!(:redshift_schema_updater) { RedshiftSchemaUpdater.new('idp') }
  let!(:file_path) { Rails.root.join('spec', 'fixtures', 'includes_columns.yml') }
  let!(:file_path2) { Rails.root.join('spec', 'fixtures', 'includes_columns2.yml') }
  let!(:combined_columns_file_path) { Rails.root.join('spec', 'fixtures', 'combined_columns.yml') }
  let!(:pii_columns_file_path) { Rails.root.join('spec', 'fixtures', 'table_with_pii_columns.yml') }
  let!(:pii_columns_file_path2) do
    Rails.root.join('spec', 'fixtures', 'table_with_pii_columns2.yml')
  end
  let!(:users_table) { 'idp.new_users' }
  let!(:users_table_pii) { 'fraudops.decrypted_new_users' }
  let!(:events_table) { 'idp.events' }
  let!(:primary_key) { 'id' }
  let!(:expected_columns) do
    ['id', 'dw_created_at', 'dw_updated_at', 'name', 'email', 'created_at', 'updated_at']
  end
  let!(:expected_combined_columns) do
    ['id', 'name', 'email', 'created_at', 'updated_at', 'redshift_only_field', 'encrypted_ssn',
     'dw_created_at', 'dw_updated_at']
  end
  let!(:expected_pii_columns) do
    ['id', 'ssn', 'mailing_address', 'dw_created_at', 'dw_updated_at']
  end
  let(:expected_encrypted_columns) do
    ['id', 'name', 'encrypted_ssn', 'encrypted_mailing_address', 'dw_created_at', 'dw_updated_at']
  end
  let!(:yaml_data) do
    [
      {
        'table' => 'new_users',
        'schema' => 'public',
        'primary_key' => 'id',
        'include_columns' => [
          { 'name' => 'id', 'datatype' => 'integer', 'not_null' => true },
          { 'name' => 'name', 'datatype' => 'string', 'not_null' => true },
          { 'name' => 'email', 'datatype' => 'string' },
          { 'name' => 'created_at', 'datatype' => 'datetime' },
          { 'name' => 'updated_at', 'datatype' => 'datetime' },
        ],
      },
      {
        'table' => 'events',
        'schema' => 'public',
        'primary_key' => 'id',
        'foreign_keys' => [
          {
            'column' => 'new_user_id',
            'references' => {
              'table' => 'new_users',
              'column' => 'id',
            },
          },
          {
            'column' => 'name',
            'references' => {
              'table' => 'new_users',
              'column' => 'name',
            },
          },
        ],
        'include_columns' => [
          { 'name' => 'id', 'datatype' => 'integer', 'not_null' => true },
          { 'name' => 'name', 'datatype' => 'string' },
          { 'name' => 'new_user_id', 'datatype' => 'integer' },
          { 'name' => 'event_type', 'datatype' => 'integer' },
          { 'name' => 'created_at', 'datatype' => 'datetime' },
          { 'name' => 'updated_at', 'datatype' => 'datetime' },
        ],
      },
    ]
  end

  describe '.update_schema_from_yaml' do
    context 'when table does not exist' do
      it 'creates new table' do
        expect(redshift_schema_updater.table_exists?(users_table)).to eq(false)
        expect(redshift_schema_updater.table_exists?(events_table)).to eq(false)

        redshift_schema_updater.update_schema_from_yaml(file_path)

        expect(redshift_schema_updater.table_exists?(users_table)).to eq(true)
        expect(redshift_schema_updater.table_exists?(events_table)).to eq(true)

        # validate primary and foreign keys column are set as NOT NULL
        users_columns = DataWarehouseApplicationRecord.connection.columns(users_table)
        expect(users_columns.map(&:name)).to include('id')
        # validate id is set as NOT NULL
        id_column = users_columns.find { |col| col.name == 'id' }
        expect(id_column.null).to eq(false)

        primary_key_query = <<~SQL
          SELECT kcu.column_name
          FROM information_schema.table_constraints tco
          JOIN information_schema.key_column_usage kcu
          ON kcu.constraint_name = tco.constraint_name
          WHERE tco.table_name = 'new_users' AND tco.constraint_type = 'PRIMARY KEY';
        SQL
        primary_key_result = DataWarehouseApplicationRecord.
          connection.exec_query(primary_key_query).to_a
        expect(primary_key_result.map { |row| row['column_name'] }).to include('id')
        foreign_key_query = <<~SQL
          SELECT kcu.column_name, ccu.table_name, ccu.column_name as referenced_column_name
          FROM information_schema.table_constraints tco
          JOIN information_schema.key_column_usage kcu
          ON kcu.constraint_name = tco.constraint_name
          JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tco.constraint_name
          WHERE tco.table_name = 'events' AND tco.constraint_type = 'FOREIGN KEY';
        SQL
        foreign_key_result = DataWarehouseApplicationRecord.
          connection.exec_query(foreign_key_query).to_a
        expect(foreign_key_result.map { |row| row['column_name'] }).to include('new_user_id')
        expect(foreign_key_result.map { |row| row['table_name'] }).to include('new_users')
        expect(foreign_key_result.map { |row| row['referenced_column_name'] }).to include('id')
        expect(foreign_key_result.map { |row| row['column_name'] }).to include('name')
        expect(foreign_key_result.map { |row| row['table_name'] }).to include('new_users')
        expect(foreign_key_result.map { |row| row['referenced_column_name'] }).to include('name')
      end
    end

    context 'when table already exists' do
      let(:existing_columns) { [{ 'name' => 'id', 'datatype' => 'integer' }] }
      let(:foreign_keys) { [] }

      before do
        redshift_schema_updater.
          create_table(users_table, existing_columns, primary_key, foreign_keys)
      end

      it 'adds new columns' do
        expect(redshift_schema_updater.table_exists?(users_table)).to eq(true)
        existing_columns = DataWarehouseApplicationRecord.connection.columns(users_table)
        expect(existing_columns.map(&:name)).to eq(['id', 'dw_created_at', 'dw_updated_at'])

        redshift_schema_updater.update_schema_from_yaml(file_path)

        new_columns = DataWarehouseApplicationRecord.connection.columns(users_table).map(&:name)
        expect(new_columns).to eq(expected_columns)
      end
    end

    context 'when table already exists with extra columns' do
      let(:existing_columns) do
        [{ 'name' => 'id', 'datatype' => 'integer' }, { 'name' => 'phone', 'datatype' => 'string' }]
      end
      let(:foreign_keys) { [] }
      let(:primary_key) { nil }

      before do
        redshift_schema_updater.
          create_table(users_table, existing_columns, primary_key, foreign_keys)
      end

      it 'updates columns and removes columns not exist in YAML file' do
        expect(redshift_schema_updater.table_exists?(users_table)).to eq(true)
        existing_columns = DataWarehouseApplicationRecord.connection.columns(users_table)
        expect(existing_columns.map(&:name)).to eq(
          ['id', 'phone', 'dw_created_at',
           'dw_updated_at'],
        )

        redshift_schema_updater.update_schema_from_yaml(file_path)

        new_columns = DataWarehouseApplicationRecord.connection.columns(users_table).map(&:name)
        expect(new_columns).to eq(expected_columns)
      end
    end

    context 'when an existing column changes data type,' do
      let(:existing_columns) do
        [{ 'name' => 'id', 'datatype' => 'integer' },
         { 'name' => 'some_numeric_column', 'datatype' => 'decimal' }]
      end
      let(:foreign_keys) { [] }

      before do
        DataWarehouseApplicationRecord.establish_connection(:data_warehouse)
        redshift_schema_updater.
          create_table(users_table, existing_columns, primary_key, foreign_keys)
        DataWarehouseApplicationRecord.connection.execute(
          DataWarehouseApplicationRecord.sanitize_sql(
            "INSERT INTO #{users_table} (id, some_numeric_column) VALUES (1, 999.0)",
          ), allow_retry: true
        )
      end

      it 'updates columns, drops columns with old data type, replaces with new data type' do
        expect(redshift_schema_updater.table_exists?(users_table)).to eq(true)
        column_obs = DataWarehouseApplicationRecord.connection.columns(users_table)
        type = column_obs.find { |col| col.name == 'some_numeric_column' }.type
        expect(type).to eq(:decimal)

        redshift_schema_updater.update_schema_from_yaml(file_path2)

        column_obs = DataWarehouseApplicationRecord.connection.columns(users_table)
        type = column_obs.find { |col| col.name == 'some_numeric_column' }.type
        expect(type).to eq(:integer)

        results = DataWarehouseApplicationRecord.connection.execute(
          DataWarehouseApplicationRecord.sanitize_sql(
            "SELECT id, some_numeric_column FROM #{users_table};",
          ),
        )
        expect(results.values).to eq([[1, 999]])
      end
    end

    context 'when a string column has their limit value updated in yaml file,' do
      let(:existing_columns) do
        [{ 'name' => 'id', 'datatype' => 'integer' },
         { 'name' => 'string_with_limit', 'datatype' => 'string', 'limit' => 100 }]
      end
      let(:foreign_keys) { [] }

      before do
        DataWarehouseApplicationRecord.establish_connection(:data_warehouse)
        redshift_schema_updater.
          create_table(users_table, existing_columns, primary_key, foreign_keys)
      end

      it 'updates column with new limit' do
        columns_objs = DataWarehouseApplicationRecord.connection.columns(users_table)
        string_col = columns_objs.find { |col| col.name == 'string_with_limit' }
        expect(string_col.limit).to eq(100)

        redshift_schema_updater.update_schema_from_yaml(file_path2)

        columns_objs = DataWarehouseApplicationRecord.connection.columns(users_table)
        string_col = columns_objs.find { |col| col.name == 'string_with_limit' }
        expect(string_col.limit).to eq(300)
      end
    end

    context 'when a string column has a limit value added in yaml file for the first time,' do
      let(:existing_columns) do
        [{ 'name' => 'id', 'datatype' => 'integer' },
         { 'name' => 'string_with_limit', 'datatype' => 'string' }]
      end
      let(:foreign_keys) { [] }
      before do
        DataWarehouseApplicationRecord.establish_connection(:data_warehouse)
        redshift_schema_updater.
          create_table(users_table, existing_columns, primary_key, foreign_keys)
      end

      it 'updates column with new limit' do
        columns_objs = DataWarehouseApplicationRecord.connection.columns(users_table)
        string_col = columns_objs.find { |col| col.name == 'string_with_limit' }
        expect(string_col.limit).to eq(nil)

        redshift_schema_updater.update_schema_from_yaml(file_path2)

        columns_objs = DataWarehouseApplicationRecord.connection.columns(users_table)
        string_col = columns_objs.find { |col| col.name == 'string_with_limit' }
        expect(string_col.limit).to eq(300)
      end
    end

    context 'when table already exist skip primary and foreign key' do
      let(:existing_columns) do
        [{ 'name' => 'id', 'datatype' => 'integer', 'not_null' => true }]
      end
      let(:foreign_keys) { [] }
      before do
        allow(redshift_schema_updater).to receive(:log_info)
        allow(redshift_schema_updater).to receive(:log_error)
        allow(redshift_schema_updater).to receive(:log_warning)
        DataWarehouseApplicationRecord.establish_connection(:data_warehouse)
        redshift_schema_updater.
          create_table(users_table, existing_columns, primary_key, foreign_keys)
      end

      it 'updates columns and skips primary and foreign key' do
        expect(redshift_schema_updater.table_exists?(users_table)).to eq(true)
        existing_columns = DataWarehouseApplicationRecord.connection.columns(users_table)
        expect(existing_columns.map(&:name)).to eq(['id', 'dw_created_at', 'dw_updated_at'])

        redshift_schema_updater.update_schema_from_yaml(file_path)

        new_columns = DataWarehouseApplicationRecord.connection.columns(users_table).map(&:name)

        expect(new_columns).to eq(expected_columns)
        # validate primary and foreign keys validation is skipped logs
        allow(Rails.logger).to receive(:info).and_call_original

        msg = 'Foreign keys and Primary_keys are not processed'
        expect(redshift_schema_updater).to have_received(:log_info).with(msg)
      end
    end
  end

  describe '.update_schema_from_yaml with add_columns configured' do
    context 'when using both include_columns and add_columns in same table' do
      it 'creates new table with columns from both configurations' do
        expect(redshift_schema_updater.table_exists?(users_table)).to eq(false)
        expect(redshift_schema_updater.table_exists?(events_table)).to eq(false)

        redshift_schema_updater.update_schema_from_yaml(combined_columns_file_path)

        expect(redshift_schema_updater.table_exists?(users_table)).to eq(true)
        expect(redshift_schema_updater.table_exists?(events_table)).to eq(true)

        users_columns = DataWarehouseApplicationRecord.connection.columns(users_table)
        expect(users_columns.map(&:name)).to match_array(expected_combined_columns)

        redshift_only_field = users_columns.find { |col| col.name == 'redshift_only_field' }
        expect(redshift_only_field).not_to be_nil
        expect(redshift_only_field.limit).to eq(256)

        id_column = users_columns.find { |col| col.name == 'id' }
        expect(id_column.null).to eq(false)

        primary_key_query = <<~SQL
          SELECT kcu.column_name
          FROM information_schema.table_constraints tco
          JOIN information_schema.key_column_usage kcu
          ON kcu.constraint_name = tco.constraint_name
          WHERE tco.table_name = 'new_users' AND tco.constraint_type = 'PRIMARY KEY';
        SQL
        primary_key_result = DataWarehouseApplicationRecord.
          connection.exec_query(primary_key_query).to_a
        expect(primary_key_result.map { |row| row['column_name'] }).to include('id')
      end

      context 'when table already exists' do
        let(:existing_columns) { [{ 'name' => 'id', 'datatype' => 'integer' }] }
        let(:foreign_keys) { [] }

        before do
          redshift_schema_updater.
            create_table(users_table, existing_columns, primary_key, foreign_keys)
        end

        it 'adds new columns from both include_columns and add_columns' do
          expect(redshift_schema_updater.table_exists?(users_table)).to eq(true)
          existing_columns = DataWarehouseApplicationRecord.connection.columns(users_table)
          expect(existing_columns.map(&:name)).to eq(['id', 'dw_created_at', 'dw_updated_at'])

          redshift_schema_updater.update_schema_from_yaml(combined_columns_file_path)

          new_columns = DataWarehouseApplicationRecord.connection.columns(users_table).map(&:name)
          expect(new_columns).to match_array(expected_combined_columns)

          redshift_only_field = DataWarehouseApplicationRecord.connection.columns(users_table).
            find do |col|
            col.name == 'redshift_only_field'
          end
          expect(redshift_only_field).not_to be_nil
        end
      end

      it 'handles events table with both column types correctly' do
        redshift_schema_updater.update_schema_from_yaml(combined_columns_file_path)

        events_columns = DataWarehouseApplicationRecord.connection.columns(events_table)
        expected_events_columns = [
          'id',
          'name',
          'new_user_id',
          'event_type',
          'created_at',
          'updated_at',
          'analytics_score',
          'dw_created_at',
          'dw_updated_at',
        ]
        expect(events_columns.map(&:name)).to match_array(expected_events_columns)

        analytics_score_field = events_columns.find { |col| col.name == 'analytics_score' }
        expect(analytics_score_field).not_to be_nil
        expect(analytics_score_field.type).to eq(:float)
      end
    end
  end

  describe '.update_schema_from_yaml' do
    context 'when dw_fraudops_email_enabled is true' do
      before do
        allow(IdentityConfig.store).to receive(:dw_fraudops_email_enabled).and_return(true)
      end

      context 'pii_table_reference is configured' do
        it 'creates a new table along with a secondary PII table' do
          expect(redshift_schema_updater.table_exists?(users_table)).to eq(false)
          expect(redshift_schema_updater.table_exists?(users_table_pii)).to eq(false)

          redshift_schema_updater.update_schema_from_yaml(pii_columns_file_path)

          # Validate users_table
          expect(redshift_schema_updater.table_exists?(users_table)).to eq(true)
          users_columns = DataWarehouseApplicationRecord.connection.columns(users_table)
          expect(users_columns.map(&:name)).to match_array(expected_encrypted_columns)

          id_column = users_columns.find { |col| col.name == 'id' }
          expect(id_column.null).to eq(false)

          primary_key_query = <<~SQL
            SELECT kcu.column_name
            FROM information_schema.table_constraints tco
            JOIN information_schema.key_column_usage kcu
            ON kcu.constraint_name = tco.constraint_name
            WHERE tco.table_name = 'new_users' AND tco.constraint_type = 'PRIMARY KEY';
          SQL
          primary_key_result = DataWarehouseApplicationRecord.
            connection.exec_query(primary_key_query).to_a
          expect(primary_key_result.map { |row| row['column_name'] }).to include('id')

          # Validate decrypted_new_users PII table
          expect(redshift_schema_updater.table_exists?(users_table_pii)).to eq(true)

          pii_columns = DataWarehouseApplicationRecord.connection.columns(users_table_pii)
          expect(pii_columns.map(&:name)).to match_array(expected_pii_columns)

          id_column = pii_columns.find { |col| col.name == 'id' }
          expect(id_column.null).to eq(false)

          primary_key_query = <<~SQL
            SELECT kcu.column_name
            FROM information_schema.table_constraints tco
            JOIN information_schema.key_column_usage kcu
            ON kcu.constraint_name = tco.constraint_name
            WHERE tco.table_name = 'decrypted_new_users' AND tco.constraint_type = 'PRIMARY KEY';
          SQL
          primary_key_result = DataWarehouseApplicationRecord.
            connection.exec_query(primary_key_query).to_a
          expect(primary_key_result.map { |row| row['column_name'] }).to include('id')

          foreign_key_query = <<~SQL
            SELECT kcu.column_name, ccu.table_name, ccu.column_name as referenced_column_name
            FROM information_schema.table_constraints tco
            JOIN information_schema.key_column_usage kcu
            ON kcu.constraint_name = tco.constraint_name
            JOIN information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tco.constraint_name
            WHERE tco.table_name = 'decrypted_new_users' 
              AND tco.table_schema = 'fraudops' 
              AND tco.constraint_type = 'FOREIGN KEY';
          SQL
          foreign_key_result = DataWarehouseApplicationRecord.
            connection.exec_query(foreign_key_query).to_a
          expect(foreign_key_result.map { |row| row['column_name'] }).to include('id')
          expect(foreign_key_result.map { |row| row['table_name'] }).to include('new_users')
          expect(foreign_key_result.map { |row| row['referenced_column_name'] }).to include('id')
        end

        context 'PII table already exists' do
          let(:existing_columns) do
            [
              { 'name' => 'id', 'datatype' => 'integer' },
              { 'name' => 'ssn', 'datatype' => 'string' },
            ]
          end
          let(:foreign_keys) { [] }

          before do
            redshift_schema_updater.
              create_table(users_table_pii, existing_columns, primary_key, foreign_keys)
          end

          it 'adds new column to PII table' do
            expect(redshift_schema_updater.table_exists?(users_table_pii)).to eq(true)
            existing_columns = DataWarehouseApplicationRecord.connection.columns(users_table_pii)
            expect(existing_columns.map(&:name)).to eq(expected_pii_columns - ['mailing_address'])

            redshift_schema_updater.update_schema_from_yaml(pii_columns_file_path)

            new_columns = DataWarehouseApplicationRecord.
              connection.columns(users_table_pii).map(&:name)
            expect(new_columns).to match_array(expected_pii_columns)
          end
        end
      end

      context 'pii_table_reference and add_columns is configured' do
        it 'creates a new table along with a secondary PII table including add_columns' do
          expect(redshift_schema_updater.table_exists?(users_table)).to eq(false)
          expect(redshift_schema_updater.table_exists?(users_table_pii)).to eq(false)

          redshift_schema_updater.update_schema_from_yaml(pii_columns_file_path2)

          # Validate users_table
          expect(redshift_schema_updater.table_exists?(users_table)).to eq(true)
          users_columns = DataWarehouseApplicationRecord.connection.columns(users_table)
          expect(users_columns.map(&:name)).to match_array(
            expected_encrypted_columns + ['add_column_1', 'add_column_2'],
          )

          # Validate decrypted_new_users PII table
          expect(redshift_schema_updater.table_exists?(users_table_pii)).to eq(true)

          pii_columns = DataWarehouseApplicationRecord.connection.columns(users_table_pii)
          expect(pii_columns.map(&:name)).to match_array(expected_pii_columns)
        end
      end
    end

    context 'when dw_fraudops_email_enabled is false' do
      before do
        allow(IdentityConfig.store).to receive(:dw_fraudops_email_enabled).and_return(false)
      end

      it 'pii_table_reference configuration is ignored' do
        expect(redshift_schema_updater.table_exists?(users_table)).to eq(false)
        expect(redshift_schema_updater.table_exists?(users_table_pii)).to eq(false)

        redshift_schema_updater.update_schema_from_yaml(combined_columns_file_path)

        expect(redshift_schema_updater.table_exists?(users_table)).to eq(true)
        expect(redshift_schema_updater.table_exists?(users_table_pii)).to eq(false)
      end
    end
  end

  describe '.load_yaml' do
    context 'when YAML file exists' do
      it 'loads YAML file' do
        expect(redshift_schema_updater.send(:load_yaml, file_path)).to eq(yaml_data)
      end
    end

    context 'when YAML file does not exist' do
      let!(:file_path) { 'path/to/nonexistent/file.yml' }
      before do
        allow(Rails.logger).to receive(:error)
      end

      it 'logs an error' do
        expect(Rails.logger).to receive(:error).with(/Error loading include columns YML file:/)
        redshift_schema_updater.send(:load_yaml, file_path)
      end

      it 'returns nil' do
        expect(redshift_schema_updater.send(:load_yaml, file_path)).to be_nil
      end
    end
  end

  describe 'redshift_data_type' do
    context 'when datatype is json, jsonb, or array' do
      it 'returns super' do
        expect(redshift_schema_updater.redshift_data_type('json')).to eq('super')
        expect(redshift_schema_updater.redshift_data_type('jsonb')).to eq('super')
        expect(redshift_schema_updater.redshift_data_type('array')).to eq('super')
      end
    end

    context 'when datatype is not a semi-structured Redshift type' do
      it 'returns the input datatype symbol' do
        expect(redshift_schema_updater.redshift_data_type('integer')).to eq('integer')
        expect(redshift_schema_updater.redshift_data_type('string')).to eq('string')
      end
    end
  end

  describe '#update_existing_table logging' do
    let(:table_name) { 'idp.test_table' }
    let(:existing_columns) do
      [
        { 'name' => 'id', 'datatype' => 'integer' },
        { 'name' => 'name', 'datatype' => 'string' },
        { 'name' => 'old_column', 'datatype' => 'string' },
      ]
    end
    let(:new_columns) do
      [
        { 'name' => 'id', 'datatype' => 'integer' },
        { 'name' => 'name', 'datatype' => 'string', 'limit' => 200 },
        { 'name' => 'email', 'datatype' => 'string' },
      ]
    end
    let(:foreign_keys) { [] }
    let(:primary_key) { 'id' }

    before do
      allow(redshift_schema_updater).to receive(:log_info)
      allow(redshift_schema_updater).to receive(:log_error)
      allow(redshift_schema_updater).to receive(:log_warning)

      redshift_schema_updater.create_table(table_name, existing_columns, primary_key, foreign_keys)
    end

    context 'when updating table successfully' do
      it 'logs startup information with table name and columns' do
        redshift_schema_updater.send(:update_existing_table, table_name, new_columns)

        expect(redshift_schema_updater).to have_received(:log_info).
          with("Starting update for existing table: #{table_name}")
        expect(redshift_schema_updater).to have_received(:log_info).
          with(match(/Current columns in table:/))
        expect(redshift_schema_updater).to have_received(:log_info).
          with(match(/Columns from YAML config:/))
      end

      it 'logs per-column processing information' do
        redshift_schema_updater.send(:update_existing_table, table_name, new_columns)

        new_columns.each do |column|
          expect(redshift_schema_updater).to have_received(:log_info).
            with(match(/Processing column: #{column['name']}/))
        end
      end

      it 'logs when adding new columns' do
        redshift_schema_updater.send(:update_existing_table, table_name, new_columns)

        expect(redshift_schema_updater).to have_received(:log_info).
          with('Action: Adding new column')
        expect(redshift_schema_updater).to have_received(:log_info).
          with("Column 'email' added successfully")
      end

      it 'logs when removing columns not in YAML config' do
        redshift_schema_updater.send(:update_existing_table, table_name, new_columns)

        expect(redshift_schema_updater).to have_received(:log_info).
          with(match(/Marking column for removal: old_column/))
        expect(redshift_schema_updater).to have_received(:log_info).
          with("Column 'old_column' removed successfully")
      end

      it 'logs when updating VARCHAR length' do
        redshift_schema_updater.send(:update_existing_table, table_name, new_columns)

        expect(redshift_schema_updater).to have_received(:log_info).
          with(match(/Action: Updating VARCHAR length/))
        expect(redshift_schema_updater).to have_received(:log_info).
          with('VARCHAR length updated successfully')
      end

      it 'logs when column matches config and no update needed' do
        columns_no_changes = [{ 'name' => 'id', 'datatype' => 'integer' }]

        redshift_schema_updater.send(:update_existing_table, table_name, columns_no_changes)

        expect(redshift_schema_updater).to have_received(:log_info).
          with('Action: No updates needed (column matches config)')
      end

      it 'logs success summary with column counts' do
        redshift_schema_updater.send(:update_existing_table, table_name, new_columns)

        expect(redshift_schema_updater).to have_received(:log_info).
          with(match(/Successfully updated table '#{table_name}'/))
        expect(redshift_schema_updater).to have_received(:log_info).
          with(match(/Configured columns: \d+/m))
        expect(redshift_schema_updater).to have_received(:log_info).
          with(match(/Removed: \d+/m))
      end

      it 'logs that foreign and primary keys are not processed' do
        redshift_schema_updater.send(:update_existing_table, table_name, new_columns)

        expect(redshift_schema_updater).to have_received(:log_info).
          with('Foreign keys and Primary_keys are not processed')
      end
    end

    context 'when data type needs to be updated' do
      let(:columns_with_type_change) do
        [
          { 'name' => 'id', 'datatype' => 'integer' },
          { 'name' => 'name', 'datatype' => 'integer' },
        ]
      end

      before do
        # Mock the actual operations to test only the logging
        allow(redshift_schema_updater).to receive(:rename_column).and_return(true)
        allow(redshift_schema_updater).to receive(:add_column).and_call_original
        allow(redshift_schema_updater).to receive(:backfill_column).and_return(true)
        allow(redshift_schema_updater).to receive(:remove_column).and_call_original
      end

      it 'logs data type comparison and update action' do
        redshift_schema_updater.send(:update_existing_table, table_name, columns_with_type_change)

        expect(redshift_schema_updater).to have_received(:log_info).
          with(match(/Current types:/m)).at_least(:once)
        expect(redshift_schema_updater).to have_received(:log_info).
          with(match(/Mapped type:/m)).at_least(:once)
        expect(redshift_schema_updater).to have_received(:log_info).
          with(match(/Action: Updating data type to integer/))
        expect(redshift_schema_updater).to have_received(:log_info).
          with('Data type updated successfully')
      end
    end

    context 'when timestamp columns need to be added' do
      let(:table_without_timestamps) { 'idp.test_table_no_ts' }
      let(:columns_minimal) { [{ 'name' => 'id', 'datatype' => 'integer' }] }

      before do
        DataWarehouseApplicationRecord.connection.execute(
          "CREATE TABLE #{table_without_timestamps} (id INTEGER)",
        )
      end

      it 'logs adding timestamp columns' do
        redshift_schema_updater.send(
          :update_existing_table, table_without_timestamps,
          columns_minimal
        )

        expect(redshift_schema_updater).to have_received(:log_info).
          with(match(/Adding timestamp column: dw_created_at/))
        expect(redshift_schema_updater).to have_received(:log_info).
          with("Timestamp column 'dw_created_at' added successfully")
        expect(redshift_schema_updater).to have_received(:log_info).
          with(match(/Adding timestamp column: dw_updated_at/))
        expect(redshift_schema_updater).to have_received(:log_info).
          with("Timestamp column 'dw_updated_at' added successfully")
      end
    end

    context 'when an error occurs during update' do
      let(:columns_invalid) do
        [{ 'name' => 'invalid_col', 'datatype' => 'invalid_type' }]
      end

      before do
        allow(redshift_schema_updater).to receive(:add_column).
          and_raise(StandardError.new('Test error'))
      end

      it 'logs detailed error information with exception class and message' do
        expect do
          redshift_schema_updater.send(:update_existing_table, table_name, columns_invalid)
        end.to raise_error(StandardError)

        expect(redshift_schema_updater).to have_received(:log_error).
          with("FAILED: Error updating existing table '#{table_name}'")
        expect(redshift_schema_updater).to have_received(:log_error).
          with('Exception: StandardError')
        expect(redshift_schema_updater).to have_received(:log_error).
          with('Message: Test error')
      end

      it 're-raises the exception after logging' do
        expect do
          redshift_schema_updater.send(:update_existing_table, table_name, columns_invalid)
        end.to raise_error(StandardError, 'Test error')
      end
    end

    context 'when processing string columns with limits' do
      let(:columns_with_limits) do
        [
          { 'name' => 'id', 'datatype' => 'integer' },
          { 'name' => 'name', 'datatype' => 'string', 'limit' => 150 },
        ]
      end

      it 'logs current and configured limits for string columns' do
        redshift_schema_updater.send(:update_existing_table, table_name, columns_with_limits)

        expect(redshift_schema_updater).to have_received(:log_info).
          with(match(/Current limit:/m))
        expect(redshift_schema_updater).to have_received(:log_info).
          with(match(/Configured limit:/m))
      end
    end
  end
end
