# frozen_string_literal: true

require 'csv'

module Reporting
  class AuthenticationReport
    attr_reader :issuers, :time_range

    module Events
      OIDC_AUTH_REQUEST = 'OpenID Connect: authorization request'
      EMAIL_CONFIRMATION = 'User Registration: Email Confirmation'
      TWO_FA_SETUP_VISITED = 'User Registration: 2FA Setup visited'
      USER_FULLY_REGISTERED = 'User Registration: User Fully Registered'
      SP_REDIRECT = 'SP redirect initiated'

      def self.all_events
        constants.map { |c| const_get(c) }
      end
    end

    # @param [Array<String>] issuers
    # @param [Range<Time>] time_range
    def initialize(
      issuers:,
      time_range:
    )
      @issuers = issuers
      @time_range = time_range
    end

    def as_tables
      [
        overview_table,
        funnel_metrics_table,
      ]
    end

    def as_reports
      [
        {
          title: 'Overview',
          table: overview_table,
          filename: 'authentication_overview',
        },
        {
          title: 'Authentication Funnel Metrics',
          table: funnel_metrics_table,
          filename: 'authentication_funnel_metrics',
        },
      ]
    end

    def to_csvs
      as_tables.map do |table|
        CSV.generate do |csv|
          table.each do |row|
            csv << row
          end
        end
      end
    end

    # event name => set(user ids)
    # @return Hash<String,Set<String>>
    def data
      @data ||= begin
        event_users = Hash.new do |h, uuid|
          h[uuid] = Set.new
        end

        fetch_results.each do |row|
          event_users[row['name']] << row['user_id']
        end

        event_users
      end
    end

    def email_confirmation
      data[Events::EMAIL_CONFIRMATION].count
    end

    def two_fa_setup_visited
      @two_fa_setup_visited ||=
        (data[Events::TWO_FA_SETUP_VISITED] & data[Events::EMAIL_CONFIRMATION]).count
    end

    def user_fully_registered
      @user_fully_registered ||=
        (data[Events::USER_FULLY_REGISTERED] & data[Events::EMAIL_CONFIRMATION]).count
    end

    def sp_redirect_initiated_new_users
      @sp_redirect_initiated_new_users ||=
        (data[Events::SP_REDIRECT] & data[Events::EMAIL_CONFIRMATION]).count
    end

    def sp_redirect_initiated_all
      data[Events::SP_REDIRECT].count
    end

    def oidc_auth_request
      data[Events::OIDC_AUTH_REQUEST].count
    end

    def sp_redirect_initiated_after_oidc
      @sp_redirect_initiated_after_oidc ||=
        (data[Events::SP_REDIRECT] & data[Events::OIDC_AUTH_REQUEST]).count
    end

    def fetch_results
      email_confirmation = Events::EMAIL_CONFIRMATION

      @fetch_results ||= Event.
        where(service_provider: issuers, name: Events.all_events, cloudwatch_timestamp: time_range).
        where('name = ? AND success = true OR name != ?', email_confirmation, email_confirmation).
        select(:name, :user_id)
    end

    def overview_table
      [
        ['Report Timeframe', "#{time_range.begin} to #{time_range.end}"],
        # This needs to be Date.today so it works when run on the command line
        ['Report Generated', Date.today.to_s], # rubocop:disable Rails/Date
        ['Issuer', issuers.join(', ')],
        ['Total # of IAL1 Users', sp_redirect_initiated_all],
      ]
    end

    def funnel_metrics_table
      [
        ['Metric', 'Number of accounts', '% of total from start'],
        [
          'New Users Started IAL1 Verification',
          email_confirmation,
          format_as_percent(numerator: email_confirmation, denominator: email_confirmation),
        ],
        [
          'New Users Completed IAL1 Password Setup',
          two_fa_setup_visited,
          format_as_percent(numerator: two_fa_setup_visited, denominator: email_confirmation),
        ],
        [
          'New Users Completed IAL1 MFA',
          user_fully_registered,
          format_as_percent(numerator: user_fully_registered, denominator: email_confirmation),
        ],
        [
          'New IAL1 Users Consented to Partner',
          sp_redirect_initiated_new_users,
          format_as_percent(
            numerator: sp_redirect_initiated_new_users,
            denominator: email_confirmation,
          ),
        ],
      ]
    end

    # @return [String]
    def format_as_percent(numerator:, denominator:)
      (100 * numerator.to_f / denominator.to_f).round(2).to_s + '%'
    end
  end
end
