# frozen_string_literal: true
require 'csv'

module Reporting
  class DemographicsMetricsReport
    attr_reader :issuers, :time_range, :agency_abbreviation

    # Log event names used in SQL queries
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
      age_data.each do |row|
        rows << [row['age_range'], row['user_count'].to_s]
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
      state_data.each do |row|
        rows << [row['state'], row['user_count'].to_s]
      end
      rows
    rescue StandardError => err
      [
        ['Error', 'Message'],
        [err.class.name, err.message],
      ]
    end

    private

    def age_data
      @age_data ||= Event.connection.execute(age_demographics_query).to_a
    end

    def state_data
      @state_data ||= Event.connection.execute(state_demographics_query).to_a
    end

    def age_demographics_query
      <<~SQL
        WITH sp_redirects AS (
          SELECT DISTINCT user_id as user_id
          FROM logs.events 
          WHERE name = '#{SP_REDIRECT_EVENT}'
            AND message.service_provider IN (#{formatted_issuers})
            AND message.event_properties.ial = 2
            AND message.properties.sp_request.facial_match = TRUE
            AND message.event_properties.ial IS NOT NULL
            AND message.properties.sp_request.facial_match IS NOT NULL
            AND cloudwatch_timestamp BETWEEN '#{formatted_start_time}' AND '#{formatted_end_time}'
        ), 
        doc_auth_success AS (
          SELECT DISTINCT
            user_id,
            message.event_properties.proofing_results.biographical_info.birth_year::int as birth_year,
            UPPER(message.event_properties.proofing_results.biographical_info.state_id_jurisdiction::text) as state
          FROM logs.events
          WHERE name = '#{DOC_AUTH_EVENT}'
            AND message.service_provider IN (#{formatted_issuers})
            AND message.event_properties.success = TRUE
            AND message.event_properties.success IS NOT NULL
            AND message.event_properties.proofing_results.biographical_info.birth_year IS NOT NULL
            AND cloudwatch_timestamp BETWEEN '#{formatted_start_time}' AND '#{formatted_end_time}'
        ),
        joined_data AS (
          SELECT d.user_id, d.birth_year, d.state
          FROM doc_auth_success d
          INNER JOIN sp_redirects s ON d.user_id = s.user_id
        )
        SELECT 
          FLOOR((EXTRACT(YEAR FROM CURRENT_DATE) - birth_year) / 10) * 10 || '-' || 
          (FLOOR((EXTRACT(YEAR FROM CURRENT_DATE) - birth_year) / 10) * 10 + 9) as age_range,
          COUNT(user_id) as user_count
        FROM joined_data 
        WHERE birth_year IS NOT NULL
          AND birth_year > 1900
        GROUP BY age_range
        ORDER BY FLOOR((EXTRACT(YEAR FROM CURRENT_DATE) - birth_year) / 10) * 10;
      SQL
    end

    def state_demographics_query
      <<~SQL
        WITH sp_redirects AS (
          SELECT DISTINCT user_id as user_id
          FROM logs.events 
          WHERE name = '#{SP_REDIRECT_EVENT}'
            AND message.service_provider IN (#{formatted_issuers})
            AND message.event_properties.ial = 2
            AND message.properties.sp_request.facial_match = TRUE
            AND message.event_properties.ial IS NOT NULL
            AND message.properties.sp_request.facial_match IS NOT NULL
            AND cloudwatch_timestamp BETWEEN '#{formatted_start_time}' AND '#{formatted_end_time}'
        ), 
        doc_auth_success AS (
          SELECT DISTINCT
            user_id,
            message.event_properties.proofing_results.biographical_info.birth_year::int as birth_year,
            UPPER(message.event_properties.proofing_results.biographical_info.state_id_jurisdiction::text) as state
          FROM logs.events
          WHERE name = '#{DOC_AUTH_EVENT}'
            AND message.service_provider IN (#{formatted_issuers})
            AND message.event_properties.success = TRUE
            AND message.event_properties.success IS NOT NULL
            AND message.event_properties.proofing_results.biographical_info.state_id_jurisdiction IS NOT NULL
            AND cloudwatch_timestamp BETWEEN '#{formatted_start_time}' AND '#{formatted_end_time}'
        ),
        joined_data AS (
          SELECT d.user_id, d.birth_year, d.state
          FROM doc_auth_success d
          INNER JOIN sp_redirects s ON d.user_id = s.user_id
        )
        SELECT 
          state,
          COUNT(user_id) as user_count
        FROM joined_data
        WHERE state IS NOT NULL 
          AND state != ''
        GROUP BY state
        ORDER BY state;
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