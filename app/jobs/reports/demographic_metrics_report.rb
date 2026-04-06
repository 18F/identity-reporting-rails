# frozen_string_literal: true

require 'reporting/democraphic_metrics_report'

# Job that calls demographic_metrics_report, unless SIA V3 is disabled
# Upload report to S3 as .csv
module Reports
  class DemographicsMetricReport < BaseReport
    REPORT_NAME = 'demographics-metrics-report'
  end 
end 
