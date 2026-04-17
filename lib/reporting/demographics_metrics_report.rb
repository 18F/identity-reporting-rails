# frozen_string_literal: true

require 'csv'
require 'reporting/json_path_helper'

module Reporting
  class DemographicsMetricsReport
    include Reporting::JsonPathHelper
    attr_reader :issuers, :time_range, :agency_abbreviation

    # Log event names used in SQL query
    SP_REDIRECT_EVENT = 'SP redirect initiated'
    DOC_AUTH_EVENT = 'IdV: doc auth verify proofing results'

    # @param [Array<String>] issuers
    # @param [Range<Time>] time_range
    def initialize(
      issuers:,
      agency_abbreviation:,
      time_range:
    )
      @issuers = issuers
      @agency_abbreviation = agency_abbreviation
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
          title: "#{@agency_abbreviation} Age Metrics",
          table: age_metrics_table,
          filename: 'age_metrics',
        },
        {
          title: "#{@agency_abbreviation} State Metrics",
          table: state_metrics_table,
          filename: 'state_metrics',
        },
      ]
    end

    def definitions_table
      [
        ['Metric', 'Unit', 'Definition'],
        ['Age range/Verification Demographics', 'Count',
         "The number of #{@agency_abbreviation} users who verified within " \
           "the reporting period, grouped by age in " \
           "10 year range."],
        ['Geographic area/Verification Demographics', 'Count',
         "The number of #{@agency_abbreviation} users who verified within " \
           "the reporting period, grouped by state."],
      ]
    end

    def overview_table
      [
        ['Report Timeframe', "#{time_range.begin} to #{time_range.end}"],
        ['Report Generated', Time.zone.today.to_s],
        ['Issuer', issuers.present? ? issuers.join(', ') : 'All Issuers'],
      ]
    end

    def age_metrics_table
      rows = [['Age Range', 'User Count']]
      age_bins.each do |range, count|
        rows << [range, count.to_s]
      end
      rows
    rescue StandardError => err
      [
        ['Error', 'Message'],
        [err.class.name, err.message],
      ]
    end

    def state_metrics_table
      rows = [['State', 'User Count']]
      state_counts.each do |state, count|
        rows << [state, count.to_s]
      end
      rows
    rescue StandardError => err
      [
        ['Error', 'Message'],
        [err.class.name, err.message],
      ]
    end

    private

    def user_data
      @user_data ||= connection.execute(demographics_query).to_a  # ← Use helper's connection
    end

    # Ruby processing: Group by age ranges and count
    def age_bins
      current_year = Time.zone.today.year
      bins = Hash.new(0)

      user_data.each do |row|
        birth_year = row['birth_year']&.to_i
        next unless birth_year
        age = current_year - birth_year
        next if age < 0

        bin_start = (age / 10) * 10
        bin_label = "#{bin_start}-#{bin_start + 9}"
        bins[bin_label] += 1
      end

      # Sort by age range (e.g., 20-29, 30-39, 40-49)
      bins.sort_by { |range, _| range.split('-').first.to_i }.to_h
    end

    # Ruby processing: Group by state and count
    def state_counts
      counts = Hash.new(0)

      user_data.each do |row|
        state = row['state']&.upcase
        next unless state.present? && state != ''

        counts[state] += 1
      end

      # Sort alphabetically by state
      counts.sort.to_h
    end

    # Single SQL query that gets raw user demographic data
    def demographics_query
      Rails.logger.warn '=== CONNECTION DEBUG ==='
      Rails.logger.warn "Adapter name: #{connection.adapter_name}"
      Rails.logger.warn "Connection class: #{connection.class}"
      Rails.logger.warn '========================'

      <<~SQL
        WITH sp_redirects AS (
          SELECT DISTINCT user_id
          FROM logs.events 
          WHERE name = '#{SP_REDIRECT_EVENT}'
            AND service_provider IN (#{formatted_issuers})
            AND #{extract_json_path('message', 'properties.event_properties.ial', type: 'INTEGER')} = 2
            AND #{extract_json_path('message', 'properties.sp_request.facial_match', type: 'BOOLEAN')} = TRUE
            AND #{extract_json_path('message', 'properties.sp_request.facial_match')} IS NOT NULL
            AND #{extract_json_path('message', 'properties.event_properties.ial')} IS NOT NULL
            AND cloudwatch_timestamp BETWEEN '#{formatted_start_time}' AND '#{formatted_end_time}'
        ), 
        doc_auth_success AS (
          SELECT DISTINCT
            user_id, 
            #{extract_json_path('message', 'properties.event_properties.proofing_results.biographical_info.birth_year', type: 'INTEGER')} as birth_year,
            UPPER(#{extract_json_path('message', 'properties.event_properties.proofing_results.biographical_info.state_id_jurisdiction')}) as state
          FROM logs.events
          WHERE name = '#{DOC_AUTH_EVENT}'
            AND service_provider IN (#{formatted_issuers})
            AND #{extract_json_path('message', 'properties.event_properties.success', type: 'BOOLEAN')} = TRUE
            AND #{extract_json_path('message', 'properties.event_properties.success')} IS NOT NULL
            AND cloudwatch_timestamp BETWEEN '#{formatted_start_time}' AND '#{formatted_end_time}'
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

    def formatted_issuers
      issuers.map { |issuer| "'#{issuer}'" }.join(', ')
    end

    def formatted_start_time
      time_range.begin.strftime('%Y-%m-%dT%H:%M:%SZ')
    end

    def formatted_end_time
      time_range.end.strftime('%Y-%m-%dT%H:%M:%SZ')
    end
  end
end
