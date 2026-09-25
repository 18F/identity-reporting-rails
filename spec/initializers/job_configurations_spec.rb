require 'rails_helper'
require 'fugit'

RSpec.describe 'job_configurations initializer' do
  let(:cron_config) { Rails.application.config.good_job.cron }

  it 'schedules every configured job with a valid cron expression' do
    invalid_jobs = cron_config.reject { |_job_key, job_config| Fugit.parse_cron(job_config[:cron]) }

    expect(invalid_jobs).to be_empty
  end

  it 'staggers RedshiftSyncJob and QuicksightSyncJob so they do not run at the same minute' do
    redshift_cron = cron_config[:redshift_sync_job][:cron]
    quicksight_cron = cron_config[:quicksight_sync_job][:cron]

    expect(redshift_cron).to eq('5/15 * * * *')
    expect(quicksight_cron).to eq('10/15 * * * *')
    expect(redshift_cron).not_to eq(quicksight_cron)
  end

  it 'keeps FraudOpsEmailAddressesZeroEtlJob on the shared 15 minute schedule' do
    expect(cron_config[:fraud_ops_email_addresses_zero_etl_job][:cron]).to eq('*/15 * * * *')
  end

  it 'configures the expected job class for each staggered cron job' do
    expect(cron_config[:redshift_sync_job][:class]).to eq('RedshiftSyncJob')
    expect(cron_config[:quicksight_sync_job][:class]).to eq('QuicksightSyncJob')
    expect(cron_config[:fraud_ops_email_addresses_zero_etl_job][:class]).to eq(
      'FraudOpsEmailAddressesZeroEtlJob',
    )
  end
end
