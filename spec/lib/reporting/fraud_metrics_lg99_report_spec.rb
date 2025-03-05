require 'rails_helper'
require 'reporting/fraud_metrics_lg99_report'

RSpec.describe Reporting::FraudMetricsLg99Report do
  let(:time_range) { Date.new(2022, 1, 1).in_time_zone('UTC').all_month }
  let(:expected_lg99_metrics_table) do
    [
      ['Metric', 'Total', 'Range Start', 'Range End'],
      ['Unique users seeing LG-99', '5', time_range.begin.to_s,
       time_range.end.to_s],
    ]
  end
  let(:expected_suspended_metrics_table) do
    [
      ['Metric', 'Total', 'Range Start', 'Range End'],
      ['Unique users suspended', '2', time_range.begin.to_s, time_range.end.to_s],
      ['Average Days Creation to Suspension', '1.5', time_range.begin.to_s, time_range.end.to_s],
      ['Average Days Proofed to Suspension', '2.0', time_range.begin.to_s, time_range.end.to_s],
    ]
  end
  let(:expected_reinstated_metrics_table) do
    [
      ['Metric', 'Total', 'Range Start', 'Range End'],
      ['Unique users reinstated', '1', time_range.begin.to_s, time_range.end.to_s],
      ['Average Days to Reinstatement', '3.0', time_range.begin.to_s, time_range.end.to_s],
    ]
  end

  let(:events) do
    [
      {
        'user_id' => 'user1',
        'name' => 'IdV: Verify please call visited',
        'time' => time_range.begin,
      },
      {
        'user_id' => 'user1',
        'name' => 'IdV: Verify please call visited',
        'time' => time_range.begin + 1.day,
      },
      {
        'user_id' => 'user1',
        'name' => 'IdV: Verify setup errors visited',
        'time' => time_range.begin + 2.days,
      },

      {
        'user_id' => 'user2',
        'name' => 'IdV: Verify setup errors visited',
        'time' => time_range.begin + 3.days,
      },

      {
        'user_id' => 'user3',
        'name' => 'IdV: Verify please call visited',
        'time' => time_range.begin + 4.days,
      },
      {
        'user_id' => 'user3',
        'name' => 'IdV: Verify setup errors visited',
        'time' => time_range.begin + 5.days,
      },
      {
        'user_id' => 'user4',
        'name' => 'IdV: Verify please call visited',
        'time' => time_range.begin + 6.days,
      },
      {
        'user_id' => 'user4',
        'name' => 'IdV: Verify setup errors visited',
        'time' => time_range.begin + 7.days,
      },
      {
        'user_id' => 'user5',
        'name' => 'IdV: Verify please call visited',
        'time' => time_range.begin + 8.days,
      },
      {
        'user_id' => 'user5',
        'name' => 'IdV: Verify setup errors visited',
        'time' => time_range.begin + 9.days,
      },
      {
        'user_id' => 'user6',
        'name' => 'User Suspension: Suspended',
        'time' => time_range.begin,
      },
      {
        'user_id' => 'user6',
        'name' => 'User Suspension: Reinstated',
        'time' => time_range.end,
      },
      {
        'user_id' => 'user7',
        'name' => 'User Suspension: Suspended',
        'time' => time_range.end,
      },
    ]
  end

  subject(:report) { Reporting::FraudMetricsLg99Report.new(time_range:) }

  before do
    events.each do |event|
      Event.create!(
        id: SecureRandom.uuid,
        message: event,
        cloudwatch_timestamp: event['time'],
        name: event['name'],
        user_id: event['user_id'],
        new_event: true,
        success: true,
      )
    end

    user7.profiles.verified.last.update(created_at: 1.day.ago, activated_at: 1.day.ago) if user7
  end

  let!(:user6) do
    create(
      :user,
      :proofed,
      :reinstated,
      uuid: 'user6',
      suspended_at: 3.days.from_now,
      reinstated_at: 6.days.from_now,
    )
  end
  let!(:user7) { create(:user, :proofed, :suspended, uuid: 'user7') }

  describe '#lg99_metrics_table' do
    it 'renders a lg99 metrics table' do
      aggregate_failures do
        report.lg99_metrics_table.zip(expected_lg99_metrics_table).each do |actual, expected|
          expect(actual).to eq(expected)
        end
      end
    end
  end

  describe '#suspended_metrics_table' do
    it 'renders a suspended metrics table' do
      aggregate_failures do
        report.suspended_metrics_table.zip(expected_suspended_metrics_table).
          each do |actual, expected|
            expect(actual).to eq(expected)
          end
      end
    end
  end

  describe '#reinstated_metrics_table' do
    it 'renders a reinstated metrics table' do
      aggregate_failures do
        report.reinstated_metrics_table.zip(expected_reinstated_metrics_table).
          each do |actual, expected|
            expect(actual).to eq(expected)
          end
      end
    end
  end

  describe '#user_days_to_suspension_avg' do
    context 'when there are suspended users' do
      it 'returns average time to suspension' do
        expect(report.user_days_to_suspension_avg).to eq(1.5)
      end
    end

    context 'when there are no users' do
      let(:user6) { nil }
      let(:user7) { nil }

      it 'returns n/a' do
        expect(report.user_days_to_suspension_avg).to eq('n/a')
      end
    end
  end

  describe '#user_days_to_reinstatement_avg' do
    context 'where there are reinstated users' do
      it 'returns average time to reinstatement' do
        expect(report.user_days_to_reinstatement_avg).to eq(3.0)
      end
    end

    context 'when there are no users' do
      let(:user6) { nil }
      let(:user7) { nil }

      it 'returns n/a' do
        expect(report.user_days_to_reinstatement_avg).to eq('n/a')
      end
    end
  end

  describe '#user_days_proofed_to_suspended_avg' do
    context 'when there are suspended users' do
      it 'returns average time proofed to suspension' do
        expect(report.user_days_proofed_to_suspension_avg).to eq(2.0)
      end
    end

    context 'when there are no users' do
      let(:user6) { nil }
      let(:user7) { nil }

      it 'returns n/a' do
        expect(report.user_days_proofed_to_suspension_avg).to eq('n/a')
      end
    end
  end

  describe '#as_emailable_reports' do
    let(:expected_reports) do
      [
        {
          title: 'Monthly LG-99 Metrics Jan-2022',
          filename: 'lg99_metrics',
          table: expected_lg99_metrics_table,
        },
        {
          title: 'Monthly Suspended User Metrics Jan-2022',
          filename: 'suspended_metrics',
          table: expected_suspended_metrics_table,
        },
        {
          title: 'Monthly Reinstated User Metrics Jan-2022',
          filename: 'reinstated_metrics',
          table: expected_reinstated_metrics_table,
        },
      ]
    end
    it 'return expected table for email' do
      expect(report.as_reports).to eq expected_reports
    end
  end
end
