# frozen_string_literal: true

require 'reporting/partner_report_period'

module Reporting
  # Manual coordinator for partner report, not an ActiveJob
  # A date range includes every period it intersects

  # Steps: First, confirms the overwrite, then uploads
  # periods sequentially with next/all/stop prompts.
  # All mode continues through issuer failures but pauses on period-level errors.
  # Canceling or stopping leaves completed uploads intact and returns a progress summary.

  # Example: October 2025 through the latest completed month (all issuers):
  # require 'reporting/partner_report_upload_orchestrator'
  # Reporting::PartnerReportUploadOrchestrator.run_interactively(
  #   start_date: '2025-10-01', end_date: Date.current.beginning_of_month - 1,
  #   report_cadence: 'monthly', report_version: 'v2',
  # )

  class PartnerReportUploadOrchestrator
    MAX_PERIODS = 366

    def self.run_interactively(input: $stdin, output: $stdout, **options)
      new(**options).run_interactively(input: input, output: output)
    end

    def initialize(start_date:, end_date:, report_cadence: 'monthly', report_version: 'v2',
                   included_issuers: nil, excluded_issuers: nil)
      @start_date = Date.parse(start_date.to_s)
      @end_date = Date.parse(end_date.to_s)
      raise ArgumentError, 'start_date must be on or before end_date' if @start_date > @end_date
      unless PartnerReportPeriod::CADENCES.include?(report_cadence)
        raise ArgumentError, "Invalid report_cadence: #{report_cadence}"
      end
      unless Reports::PartnerReportDefault::VALID_VERSIONS.include?(report_version)
        raise ArgumentError, "Invalid report_version: #{report_version}"
      end
      @report_cadence = report_cadence
      @report_version = report_version
      @included_issuers = normalize_issuers(included_issuers)
      @excluded_issuers = normalize_issuers(excluded_issuers)
      if @included_issuers && @excluded_issuers
        raise ArgumentError, 'Cannot specify both included_issuers and excluded_issuers'
      end
    end

    def run_interactively(input: $stdin, output: $stdout)
      target = preview(output)
      output.puts 'Type yes to begin overwriting reports; anything else cancels:'
      confirmed = input.gets&.strip == 'yes'
      results = []
      run_all = false

      if confirmed
        periods.each do |period|
          result = run_period(period)
          results << result
          print_result(result, output)
          break if results.length == periods.length
          next if run_all && !result[:error]

          # Even in all mode, a period-level error needs an explicit continue
          choice = read_choice(input, output)
          break if choice == 'stop'
          run_all = choice == 'all'
        end
      end

      finish(target, results, confirmed, output)
    end

    private

    def normalize_issuers(issuers)
      return nil if issuers.nil?
      issuers = [issuers] if issuers.is_a?(String)
      unless issuers.is_a?(Array)
        raise ArgumentError, 'Issuers must be a string or array of strings'
      end
      values = issuers.compact.map(&:to_s).reject(&:empty?).uniq
      values.empty? ? nil : values
    end

    def periods
      @periods ||= begin
        first = Date.parse(
          PartnerReportPeriod.start_date(
            report_date: @start_date, cadence: @report_cadence,
          ),
        )
        dates = []
        while first <= @end_date
          raise ArgumentError, "Backfill exceeds #{MAX_PERIODS} periods; split the range" if
            dates.length >= MAX_PERIODS
          dates << { period_start_date: first.to_s,
                     period_end_date: PartnerReportPeriod.end_date(first, @report_cadence).to_s }
          first = PartnerReportPeriod.next_start(first, @report_cadence)
        end
        dates
      end
    end

    def preview(output)
      planned_periods = periods
      target = Reports::PartnerReportDefault.new.upload_target
      raise ArgumentError, 'Partner report bucket must be configured' if target[:bucket].blank?
      output.puts "WARNING: overwrite existing #{@report_version}/#{@report_cadence} reports in "\
                  "#{target[:environment]} (#{planned_periods.length} periods)."
      output.puts "Target: s3://#{target[:bucket]}/#{target[:prefix]}#{@report_version}/"
      output.puts "Issuers: "\
        "#{@included_issuers ? @included_issuers.join(', ') : 'ALL ELIGIBLE ISSUERS'}"
      output.puts "Excluding: #{@excluded_issuers.join(', ')}" if @excluded_issuers
      planned_periods.each do |period|
        output.puts "  #{period[:period_start_date]} through #{period[:period_end_date]}"
      end
      target
    end

    def run_period(period)
      job = Reports::PartnerReportDefault.new(
        Date.parse(period[:period_end_date]), @report_version,
        { report_cadence: @report_cadence,
          included_issuers: @included_issuers,
          excluded_issuers: @excluded_issuers,
          skip_confirmation_delay: true }
      )
      result = job.perform_now
      raise StandardError, 'Upload job did not return a structured result' unless result.is_a?(Hash)
      result.merge(period).merge(
        status: result[:failed_count].zero? && result[:skipped_count].zero? &&
                result[:missing_issuers].empty? ? 'completed' : 'partial',
      )
    rescue StandardError => e
      { uploaded_count: 0,
        failed_count: 0,
        skipped_count: 0,
        failures: [],
        skipped: [],
        missing_issuers: [] }.merge(job&.result || {}).merge(
          period, status: 'failed', error: e.message, error_class: e.class.name
        )
    end

    def print_result(result, output)
      output.puts "#{result[:period_start_date]} through #{result[:period_end_date]}: "\
                  "#{result[:uploaded_count]} uploaded, #{result[:failed_count]} failed, "\
                  "#{result[:skipped_count]} skipped, #{result[:missing_issuers].length} missing."
      result[:failures].each do |failure|
        output.puts "  FAILED #{failure[:issuer]}: #{failure[:error]}"
      end
      result[:skipped].each { |skip| output.puts "  SKIPPED #{skip[:issuer]}: #{skip[:reason]}" }
      result[:missing_issuers].each { |issuer| output.puts "  MISSING #{issuer}" }
      output.puts "  PERIOD ERROR: #{result[:error]}" if result[:error]
    end

    def read_choice(input, output)
      loop do
        output.puts 'Type "next" for the next period, "all" for remaining periods, or "stop":'
        choice = input.gets&.strip
        return 'stop' if choice.nil?
        return choice if %w[next all stop].include?(choice)
      end
    end

    def finish(target, results, confirmed, output)
      status = if !confirmed
                 'canceled'
               elsif results.length == periods.length
                 'completed'
               else
                 'stopped'
               end
      summary = { target: target,
                  report_version: @report_version,
                  report_cadence: @report_cadence,
                  included_issuers: @included_issuers,
                  excluded_issuers: @excluded_issuers,
                  status: status,
                  planned_count: periods.length,
                  planned_periods: periods,
                  results: results,
                  unattempted_periods: periods.drop(results.length) }
      %i[uploaded_count failed_count skipped_count].each do |key|
        summary[key] = summary[:results].sum { |result| result[key] }
      end
      summary[:failed_period_count] = summary[:results].count { |result| result[:error] }
      output.puts "Backfill #{summary[:status]}: #{summary[:results].length}/"\
                  "#{summary[:planned_count]} periods attempted; "\
                  "#{summary[:uploaded_count]} uploaded, #{summary[:failed_count]} failed, "\
                  "#{summary[:skipped_count]} skipped; "\
                  "#{summary[:failed_period_count]} period errors."
      summary
    end
  end
end
