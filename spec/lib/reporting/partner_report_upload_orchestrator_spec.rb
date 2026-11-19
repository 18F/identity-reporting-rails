# frozen_string_literal: true

require 'rails_helper'
require 'stringio'
require 'reporting/partner_report_upload_orchestrator'

RSpec.describe Reporting::PartnerReportUploadOrchestrator do
  let(:output) { StringIO.new }
  let(:upload_job) { instance_double(Reports::PartnerReportDefault, result: nil) }
  let(:options) do
    { start_date: '2026-01-01', end_date: '2026-03-31', included_issuers: ['issuer1'] }
  end
  let(:result) do
    { uploaded_count: 10,
      failed_count: 2,
      skipped_count: 1,
      failures: [{ issuer: 'issuer1', error: 'upload failed' }],
      skipped: [{ issuer: 'issuer2', reason: 'missing id' }],
      missing_issuers: [] }
  end

  def run_with(answers, **overrides)
    described_class.run_interactively(
      **options.merge(overrides), input: StringIO.new(answers), output: output,
    )
  end

  before do
    allow(Reports::PartnerReportDefault).to receive(:new).and_return(upload_job)
    allow(upload_job).to receive(:upload_target).and_return(
      { environment: 'agnes', bucket: 'reports', prefix: 'agnes/portal/' },
    )
    allow(upload_job).to receive(:perform_now).and_return(result)
    allow(Reporting::PartnerReportPeriod).to receive(:start_date) do |args|
      date = Date.parse(args[:report_date].to_s)
      Date.new(date.year, date.month, 1).to_s
    end
  end

  it 'cancels without an exact yes, including end of input' do
    expect(upload_job).not_to receive(:perform_now)
    ["no\n", "YES\n", ''].each do |answers|
      summary = run_with(answers)
      expect(summary[:status]).to eq('canceled')
      expect(summary[:unattempted_periods].length).to eq(3)
      expect(summary[:uploaded_count]).to eq(0)
    end
  end

  it 'shows the target and runs sequentially until stop, retaining partial failure counts' do
    summary = run_with("yes\nnext\nstop\n")
    expect(Reports::PartnerReportDefault).to have_received(:new).with(
      Date.new(2026, 1, 31), 'v2',
      { report_cadence: 'monthly',
        included_issuers: ['issuer1'],
        excluded_issuers: nil,
        skip_confirmation_delay: true }
    )
    expect(upload_job).to have_received(:perform_now).twice
    expect(summary).to include(
      status: 'stopped', uploaded_count: 20, failed_count: 4,
      skipped_count: 2
    )
    expect(summary[:unattempted_periods].first[:period_start_date]).to eq('2026-03-01')
    expect(output.string).to include('s3://reports/agnes/portal/v2/', 'FAILED issuer1', '2 failed')
  end

  it 'runs all remaining periods without prompting for partial issuer failures' do
    summary = run_with("yes\nall\n")
    expect(upload_job).to have_received(:perform_now).exactly(3).times
    expect(summary[:status]).to eq('completed')
    expect(summary[:failed_count]).to eq(6)
    expect(summary[:failed_period_count]).to eq(0)
    expect(output.string.scan('Type "next"').length).to eq(1)
  end

  it 'stops safely on end of input between periods' do
    summary = run_with("yes\n")
    expect(upload_job).to have_received(:perform_now).once
    expect(summary[:status]).to eq('stopped')
  end

  it 'asks again after an invalid continuation choice' do
    summary = run_with("yes\ninvalid\nnext\nstop\n")
    expect(summary[:results].length).to eq(2)
    expect(output.string.scan('Type "next"').length).to eq(3)
  end

  it 'retains partial progress when a period raises' do
    allow(upload_job).to receive(:perform_now).and_raise(StandardError, 'warehouse unavailable')
    allow(upload_job).to receive(:result).and_return(result)
    summary = run_with("yes\nstop\n")
    expect(summary[:failed_period_count]).to eq(1)
    expect(summary[:uploaded_count]).to eq(10)
    expect(summary[:status]).to eq('stopped')
    expect(output.string).to include('PERIOD ERROR: warehouse unavailable')
  end

  it 'allows next after a period error' do
    allow(upload_job).to receive(:perform_now).and_raise(StandardError, 'warehouse unavailable')
    summary = run_with("yes\nnext\nstop\n")
    expect(summary[:failed_period_count]).to eq(2)
  end

  it 'pauses on a period error after all, then honors next' do
    calls = 0
    allow(upload_job).to receive(:perform_now) do
      calls += 1
      raise StandardError, 'warehouse unavailable' if calls == 2
      result
    end
    summary = run_with("yes\nall\nnext\n")
    expect(summary[:status]).to eq('completed')
    expect(summary[:results].length).to eq(3)
    expect(summary[:failed_period_count]).to eq(1)
    expect(output.string.scan('Type "next"').length).to eq(2)
  end

  it 'allows stopping on a period error after all' do
    calls = 0
    allow(upload_job).to receive(:perform_now) do
      calls += 1
      raise StandardError, 'warehouse unavailable' if calls == 2
      result
    end
    summary = run_with("yes\nall\nstop\n")
    expect(summary[:status]).to eq('stopped')
    expect(summary[:results].length).to eq(2)
  end

  it 'includes cross-year weekly periods using warehouse-resolved starts' do
    allow(Reporting::PartnerReportPeriod).to receive(:start_date).
      with(report_date: Date.new(2025, 12, 31), cadence: 'weekly').and_return('2025-12-29')
    summary = run_with(
      "yes\nall\n", start_date: '2025-12-31', end_date: '2026-01-06', report_cadence: 'weekly'
    )
    expect(summary[:planned_periods]).to eq(
      [
        { period_start_date: '2025-12-29',
          period_end_date: '2026-01-04' },
        { period_start_date: '2026-01-05',
          period_end_date: '2026-01-11' },
      ],
    )
  end

  it 'runs each day in an inclusive daily range' do
    allow(Reporting::PartnerReportPeriod).to receive(:start_date) do |args|
      args[:report_date].to_s
    end
    summary = run_with("yes\nall\n", report_cadence: 'daily', end_date: '2026-01-03')
    expect(summary[:planned_count]).to eq(3)
    expect(summary[:planned_periods].last).to eq(
      period_start_date: '2026-01-03', period_end_date: '2026-01-03',
    )
  end

  it 'finishes a single intersecting period without asking for a continuation choice' do
    summary = run_with("yes\n", end_date: '2026-01-15')
    expect(summary[:planned_count]).to eq(1)
    expect(summary[:status]).to eq('completed')
    expect(summary[:failed_period_count]).to eq(0)
    expect(output.string).not_to include('Type "next"')
  end

  it 'warns when selecting all eligible issuers' do
    run_with("no\n", included_issuers: nil)
    expect(output.string).to include('ALL ELIGIBLE ISSUERS')
  end

  it 'rejects invalid ranges, filters, cadences, versions, and excessive counts before uploads' do
    expect(upload_job).not_to receive(:perform_now)
    [
      { end_date: '2025-01-01' },
      { excluded_issuers: ['issuer2'] },
      { report_cadence: 'yearly' },
      { report_version: 'v3' },
      { included_issuers: 123 },
    ].each do |overrides|
      expect { run_with("yes\n", **overrides) }.to raise_error(ArgumentError)
    end
    expect do
      run_with("yes\n", start_date: '1900-01-01', end_date: '2026-01-01')
    end.to raise_error(ArgumentError, /exceeds/)
  end
end
