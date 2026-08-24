require 'rails_helper'
require 'rake'

RSpec.describe 'frd_events:backfill_columns', type: :task do
  before(:all) do
    Rake.application.rake_require('tasks/backfill_frd_events_columns')
    Rake::Task.define_task(:environment)
  end

  let(:task) { Rake::Task['frd_events:backfill_columns'] }
  let(:connection) { DataWarehouseApplicationRecord.connection }
  let(:message) do
    {
      events: {
        'https://schemas.login.gov/secevent/attempts-api/event-type/idv-phone-submitted' => {
          success: true,
          device_id: 'dev-xyz',
          user_ip_address: '104.56.43.23',
          agency_uuid: 'agency-bbb',
          unique_session_id: 'sess-ccc',
        },
      },
    }
  end

  def insert_row(event_key, message_hash)
    connection.execute(
      ActiveRecord::Base.send(
        :sanitize_sql_array,
        ['INSERT INTO fraudops.frd_events (event_key, message, dw_created_at) ' \
         'VALUES (?, ?::jsonb, CURRENT_TIMESTAMP)', event_key, JSON.generate(message_hash)],
      ),
    )
  end

  after { task.reenable }

  before do
    connection.execute('DELETE FROM fraudops.frd_events')
    insert_row('evt-1', message)
  end

  it 'backfills the six flattened columns from message' do
    task.invoke
    row = connection.exec_query(
      "SELECT event_type, success, device_id, user_ip_address, agency_uuid, " \
      "unique_session_id FROM fraudops.frd_events WHERE event_key = 'evt-1'",
    ).first
    expect(row['event_type']).to eq('idv-phone-submitted')
    expect(row['success']).to eq(true)
    expect(row['device_id']).to eq('dev-xyz')
    expect(row['user_ip_address']).to eq('104.56.43.23')
    expect(row['agency_uuid']).to eq('agency-bbb')
    expect(row['unique_session_id']).to eq('sess-ccc')
  end

  it 'is idempotent — a second run leaves already-populated rows unchanged' do
    task.invoke
    task.reenable
    expect { task.invoke }.not_to raise_error
    count = connection.exec_query(
      "SELECT COUNT(*) AS c FROM fraudops.frd_events WHERE event_type = 'idv-phone-submitted'",
    ).first['c']
    expect(count).to eq(1)
  end

  it 'terminates on rows whose message yields no event object (no /event-type/ key)' do
    # These rows extract to event_type = nil, so the UPDATE leaves event_type NULL and
    # they keep matching `event_type IS NULL`. Without the monotonic event_key cursor,
    # a full batch of such rows would re-select forever. With batch_size = 2 and 3 such
    # rows, the cursor makes each row visited exactly once across two batches.
    connection.execute('DELETE FROM fraudops.frd_events')
    insert_row('no-evt-1', { foo: 'bar' })
    insert_row('no-evt-2', {})
    insert_row('no-evt-3', { baz: { qux: 1 } })

    update_count = 0
    allow(connection).to receive(:execute).and_wrap_original do |orig, sql|
      update_count += 1 if sql.is_a?(String) && sql.start_with?('UPDATE fraudops.frd_events')
      orig.call(sql)
    end

    Timeout.timeout(15) do
      expect { task.invoke(2) }.not_to raise_error
    end

    # One set-based UPDATE per batch: 3 rows at batch_size 2 = a full batch + a
    # partial batch. A third UPDATE would mean a row was re-processed.
    expect(update_count).to eq(2)

    null_count = connection.exec_query(
      'SELECT COUNT(*) AS c FROM fraudops.frd_events WHERE event_type IS NULL',
    ).first['c']
    expect(null_count).to eq(3)
  end

  it 'backfills across multiple batches and terminates on a partial batch' do
    # 3 backfillable rows with batch_size = 2 forces a full batch then a partial batch,
    # exercising the cursor advance + partial-batch termination the single-row fixture
    # never does.
    connection.execute('DELETE FROM fraudops.frd_events')
    insert_row('multi-1', message)
    insert_row('multi-2', message)
    insert_row('multi-3', message)

    Timeout.timeout(15) do
      expect { task.invoke(2) }.not_to raise_error
    end

    count = connection.exec_query(
      "SELECT COUNT(*) AS c FROM fraudops.frd_events WHERE event_type = 'idv-phone-submitted'",
    ).first['c']
    expect(count).to eq(3)
  end

  it 'round-trips values containing quotes and backslashes' do
    tricky = message.deep_dup
    tricky[:events].values.first[:device_id] = %q(dev'quote\slash\\)
    connection.execute('DELETE FROM fraudops.frd_events')
    insert_row('evt-tricky', tricky)

    task.invoke

    device_id = connection.exec_query(
      "SELECT device_id FROM fraudops.frd_events WHERE event_key = 'evt-tricky'",
    ).first['device_id']
    expect(device_id).to eq(%q(dev'quote\slash\\))
  end

  it 'backfills a non-boolean success value as NULL, not truthy-coerced TRUE' do
    malformed = message.deep_dup
    malformed[:events].values.first[:success] = 'false'
    connection.execute('DELETE FROM fraudops.frd_events')
    insert_row('evt-badbool', malformed)

    task.invoke

    row = connection.exec_query(
      "SELECT event_type, success FROM fraudops.frd_events WHERE event_key = 'evt-badbool'",
    ).first
    expect(row['event_type']).to eq('idv-phone-submitted')
    expect(row['success']).to be_nil
  end

  it 'aborts on a non-numeric batch_size instead of silently no-opping' do
    expect { task.invoke('abc') }.to raise_error(SystemExit)
    expect(
      connection.exec_query(
        'SELECT COUNT(*) AS c FROM fraudops.frd_events WHERE event_type IS NOT NULL',
      ).first['c'],
    ).to eq(0)
  end

  it 'aborts on a zero batch_size' do
    expect { task.invoke('0') }.to raise_error(SystemExit)
  end
end
