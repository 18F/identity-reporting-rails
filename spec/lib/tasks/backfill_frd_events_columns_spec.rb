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

  after { task.reenable }

  before do
    connection.execute('DELETE FROM fraudops.frd_events')
    connection.execute(
      ActiveRecord::Base.send(
        :sanitize_sql_array,
        ['INSERT INTO fraudops.frd_events (event_key, message, dw_created_at) ' \
         'VALUES (?, ?::jsonb, CURRENT_TIMESTAMP)', 'evt-1', JSON.generate(message)],
      ),
    )
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
end
