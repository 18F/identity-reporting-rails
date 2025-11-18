cron_30m = '*/30 * * * *'
cron_5m = '0/5 * * * *'
# cron_10m = '0/10 * * * *'
cron_1d = '0 6 * * *' # 6:00am UTC or 2:00am EST
cron_24h = '0 0 * * *'
cron_24h_and_a_bit = '12 0 * * *' # 0000 UTC + 12 min, staggered from whatever else runs at 0000 UTC

if defined?(Rails::Console)
  Rails.logger.info 'job_configurations: console detected, skipping schedule'
else
  Rails.application.configure do # rubocop:disable Metrics/BlockLength
    config.good_job.cron = {
      # Queue heartbeat job to GoodJob
      heartbeat_job: {
        class: 'HeartbeatJob',
        cron: cron_5m,
      },
      # Queue data freshness check job for production table to GoodJob
      data_freshness_job: {
        class: 'DataFreshnessJob',
        cron: cron_30m,
      },
      # Queue redshift new user detection job to GoodJob
      redshift_new_user_detection_job: {
        class: 'RedshiftUnexpectedUserDetectionJob',
        cron: '2-59/5 * * * *',
        # runs every 5 minutes starting at 2 minutes past the hour to allow the user sync script
        # to complete at the top of the hour
      },
      # Queue schema service job to GoodJob
      extractor_row_checker_enqueue_job: {
        class: 'ExtractorRowCheckerEnqueueJob',
        cron: cron_1d,
      },
      # Queue redshift system tables sync
      redshift_system_table_sync: {
        class: 'RedshiftSystemTableSyncJob',
        cron: cron_1d,
      },
      # Queue RedshiftUnloadLogCheckerJob job to GoodJob
      redshift_unload_log_checker_job: {
        class: 'RedshiftUnloadLogCheckerJob',
        cron: cron_5m,
      },
      # Send fraud metrics to Team Judy
      fraud_metrics_report: {
        class: 'Reports::FraudMetricsReport',
        cron: cron_24h_and_a_bit,
        args: -> { [Time.zone.yesterday.end_of_day] },
      },
      # Idv Legacy Conversion Supplement Report to S3
      idv_legacy_conversion_supplement_report: {
        class: 'Reports::IdvLegacyConversionSupplementReport',
        cron: cron_24h,
        args: -> { [Time.zone.today] },
      },
      # todo: temporarily increased frequency  of IdvRedisToRedshiftJob and FraudOpsPiiDecryptJob
      # for testing and demo purposes. Either revert to staggered cron_10m after or
      # decide on final frequency
      # Queue IDV Redis to Redshift job to GoodJob
      # idv_redis_to_redshift_job: {
      #   class: 'IdvRedisToRedshiftJob',
      #   cron: '0/2 * * * *', # every 2 minutes
      # },
      # # Import FraudOps PII Decrypt Job
      # fraud_ops_pii_decrypt_job: {
      #   class: 'FraudOpsPiiDecryptJob',
      #   cron: '1/2 * * * *', # every 2 minutes, staggered 1 minute after IdvRedisToRedshiftJob
      # },
    }
    Rails.logger.info 'job_configurations: jobs scheduled with good_job.cron'
  end
end
