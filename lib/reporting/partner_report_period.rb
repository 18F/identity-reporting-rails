# frozen_string_literal: true

require 'date'

module Reporting
  module PartnerReportPeriod
    CADENCES = %w[monthly weekly daily].freeze

    def self.end_date(start_date, cadence)
      date = Date.parse(start_date.to_s)
      case cadence
      when 'monthly' then Date.new(date.year, date.month, -1)
      when 'weekly' then date + 6
      when 'daily' then date
      else raise ArgumentError, "Invalid report_cadence: #{cadence}"
      end
    end

    def self.next_start(start_date, cadence)
      end_date(start_date, cadence) + 1
    end
  end
end
