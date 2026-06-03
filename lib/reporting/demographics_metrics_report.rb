# frozen_string_literal: true

require 'csv'
require 'reporting/json_path_helper'

module Reporting
  class DemographicsMetricsReport
    include Reporting::JsonPathHelper
    attr_reader :issuer_string, :time_range

    # Log event names used in SQL query
    SP_REDIRECT_EVENT = 'SP redirect initiated'
    DOC_AUTH_EVENT = 'IdV: doc auth verify proofing results'

    # @param [String] issuer_string
    # @param [Range<Time>] time_range
    def initialize(
      issuer_string:,
      time_range:
    )
      @issuer_string = issuer_string
      @time_range = time_range
    end

    def as_reports
      [
        {
          title: 'Definitions',
          table: definitions_table,
          filename: 'definitions',
        },
        {
          title: 'Overview',
          table: overview_table,
          filename: 'overview',
        },
        {
          title: 'Age Metrics',
          table: age_metrics_table,
          filename: 'age_metrics',
        },
        {
          title: 'State Metrics',
          table: state_metrics_table,
          filename: 'state_metrics',
        },
      ]
    end

    def definitions_table
      [
        ['Metric', 'Unit', 'Definition'],
        ['Age range/Verification Demographics', 'Count',
         'The number of users for this issuer who verified within ' \
           'the reporting period, grouped by age in ' \
           '10 year range.'],
        ['Geographic area/Verification Demographics', 'Count',
         'The number of users for this issuer who verified within ' \
           'the reporting period, grouped by state.'],
      ]
    end

    def overview_table
      [
        ['Report Timeframe', "#{time_range.begin} to #{time_range.end}"],
        ['Report Generated', Date.current.to_s],
        ['Issuer', issuer_string.to_s],
      ]
    end

    def age_metrics_table
      rows = [['Age Range', 'User Count']]
      age_bins.each do |range, count|
        rows << [range, count.to_s]
      end
      rows
    rescue StandardError => err
      # Don't upload malformed data that could break downstream ingestion
      Rails.logger.error "Failed to generate age metrics table for issuer"\
                         " #{issuer_string}: #{err.message}"
      raise err
    end

    def state_metrics_table
      rows = [['State', 'User Count']]
      state_counts.each do |state, count|
        rows << [state, count.to_s]
      end
      rows
    rescue StandardError => err
      # Don't upload malformed data that could break downstream ingestion
      Rails.logger.error "Failed to generate state metrics table for"\
                         " issuer #{issuer_string}: #{err.message}"
      raise err
    end

    private

    def user_data
      @user_data ||= begin
        result = DataWarehouseApplicationRecord.connection.exec_query(
          demographics_query,
          'DemographicsMetricsReport',
          query_parameters,
        ).to_a

        if result.empty?
          Rails.logger.info "No demographic data found for issuer #{issuer_string} in"\
                            " time range #{time_range.begin} to #{time_range.end}. "\
                            "Generating empty reports."
        end

        result
      end
    end

    # Ruby processing: Group by age ranges and count
    def age_bins
      current_year = Date.current.year
      bins = Hash.new(0)
      invalid_ages = 0
      nil_birth_years = 0

      user_data.each do |row|
        birth_year = row['birth_year']&.to_i

        if birth_year.nil?
          nil_birth_years += 1
          next
        end

        age = current_year - birth_year
        if age < 0 || age > 140
          invalid_ages += 1
          next
        end

        bin_start = (age / 10) * 10
        bin_label = "#{bin_start}-#{bin_start + 9}"
        bins[bin_label] += 1
      end

      # Log potential data quality issues
      total_records = user_data.length
      if nil_birth_years > 0 || invalid_ages > 0
        Rails.logger.warn "Demographics age data quality: #{total_records} total records, " \
                          "#{nil_birth_years} with nil birth_year, #{invalid_ages} with invalid age"
      end

      bins.sort_by { |range, _| range.split('-').first.to_i }.to_h
    end

    # Ruby processing: Group by state and count
    def state_counts
      counts = Hash.new(0)

      user_data.each do |row|
        state = row['state']&.upcase
        if state.blank?
          next # Expected for non-trivial amount of users verifying with passport
        end

        counts[state] += 1
      end

      counts.sort.to_h
    end

    # Single SQL query that gets raw user demographic data
    def demographics_query
      <<~SQL
        WITH base_events AS (
          SELECT 
            user_id,
            name,
            service_provider,
            message,
            cloudwatch_timestamp
          FROM logs.events 
          WHERE service_provider = $1
            AND cloudwatch_timestamp BETWEEN $2 AND $3
        ),
        sp_redirects AS (
          SELECT DISTINCT user_id
          FROM base_events
          WHERE name = $4
            AND #{extract_json_path('message', 'properties.event_properties.ial', type: 'INTEGER')} = 2
            AND #{extract_json_path('message', 'properties.sp_request.facial_match', type: 'BOOLEAN')} = TRUE
            AND #{extract_json_path('message', 'properties.sp_request.facial_match')} IS NOT NULL
            AND #{extract_json_path('message', 'properties.event_properties.ial')} IS NOT NULL
        ), 
        doc_auth_success AS (
          SELECT DISTINCT
            user_id, 
            #{extract_json_path('message', 'properties.event_properties.proofing_results.biographical_info.birth_year', type: 'INTEGER')} as birth_year,
            UPPER(#{extract_json_path('message', 'properties.event_properties.proofing_results.biographical_info.state_id_jurisdiction')}::TEXT) as state
          FROM base_events
          WHERE name = $5
            AND #{extract_json_path('message', 'properties.event_properties.success', type: 'BOOLEAN')} = TRUE
            AND #{extract_json_path('message', 'properties.event_properties.success')} IS NOT NULL
        )
        SELECT 
          d.user_id,
          d.birth_year,
          d.state
        FROM doc_auth_success d
        INNER JOIN sp_redirects s ON d.user_id = s.user_id
        ORDER BY d.user_id;
      SQL
    end

    def query_parameters
      [
        issuer_string,          # $1
        formatted_start_time,   # $2
        formatted_end_time,     # $3
        SP_REDIRECT_EVENT,      # $4
        DOC_AUTH_EVENT,         # $5
      ]
    end

    def formatted_start_time
      time_range.begin.strftime('%Y-%m-%dT%H:%M:%SZ')
    end

    def formatted_end_time
      time_range.end.strftime('%Y-%m-%dT%H:%M:%SZ')
    end
  end
end
