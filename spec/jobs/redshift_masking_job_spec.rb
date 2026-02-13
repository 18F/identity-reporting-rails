# frozen_string_literal: true

require 'rails_helper'

# Stub out lib/common.rb and its AWS SDK dependencies so they don't need to be
# present in the test bundle. The job loads lib/common via require inside perform.
# Skip stubs if common.rb was already loaded by another spec (e.g. common_spec.rb).
unless defined?(RedshiftCommon::QueryExecutor) &&
       RedshiftCommon::QueryExecutor.instance_methods.any?
  module RedshiftCommon
    class Config
      def env_name; end
    end

    class AwsClients; end

    class QueryExecutor; end

    module UserQueries
      def self.fetch_users(_executor); end
    end

    module SqlQuoting
      def self.quote_grantee(grantee) = grantee
    end
  end
end

require_relative '../../app/services/mask'

RSpec.describe RedshiftMaskingJob, type: :job do
  let(:job) { described_class.new }
  let(:redshift_config) { instance_double(RedshiftCommon::Config, env_name: 'test') }
  let(:aws) { instance_double(RedshiftCommon::AwsClients) }
  let(:executor) { instance_double(RedshiftCommon::QueryExecutor) }

  let(:data_controls) do
    {
      'masking_policies' => {
        'user_types' => {},
        'columns' => [],
      },
    }
  end
  let(:users_yaml) { {} }

  before do
    allow(job).to receive(:require) # suppress lib/common require inside perform
    allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(true)
    allow(File).to receive(:read).with(described_class::DATA_CONTROLS_PATH).
      and_return(data_controls.to_yaml)
    allow(File).to receive(:read).with(described_class::USERS_YAML_PATH).
      and_return({ 'users' => users_yaml }.to_yaml)
    allow(RedshiftCommon::Config).to receive(:new).and_return(redshift_config)
    allow(RedshiftCommon::AwsClients).to receive(:new).with(redshift_config).and_return(aws)
    allow(RedshiftCommon::QueryExecutor).to receive(:new).and_return(executor)
    allow(RedshiftCommon::UserQueries).to receive(:fetch_users).with(executor).and_return([])
    allow(Rails.logger).to receive(:info)
    allow(Rails.logger).to receive(:warn)
  end

  describe '#perform' do
    context 'when job is disabled' do
      before do
        allow(IdentityConfig.store).to receive(:fraud_ops_tracker_enabled).and_return(false)
      end

      it 'skips execution and logs info' do
        expect(Rails.logger).to receive(:info).with(a_string_matching('disabled'))
        expect(RedshiftCommon::Config).not_to receive(:new)
        job.perform
      end
    end

    context 'when job is enabled' do
      let(:mock_column_types) { {} }
      let(:mock_policies) { [] }

      before do
        allow_any_instance_of(RedshiftMasking::DatabaseQueries).to receive(:fetch_column_types).
          and_return(mock_column_types)
        allow_any_instance_of(RedshiftMasking::DatabaseQueries).
          to receive(:fetch_existing_policies).and_return(mock_policies)
        allow_any_instance_of(RedshiftMasking::SqlExecutor).to receive(:create_masking_policies)
        allow_any_instance_of(RedshiftMasking::SqlExecutor).to receive(:apply_corrections)
        allow_any_instance_of(RedshiftMasking::PolicyBuilder).to receive(:build_expected_state).
          and_return([])
        allow_any_instance_of(RedshiftMasking::DriftDetector).to receive(:detect).
          and_return({ missing: [], extra: [], mismatched: [] })
      end

      it 'runs the masking policy sync' do
        expect_any_instance_of(RedshiftMasking::SqlExecutor).to receive(:create_masking_policies)
        expect_any_instance_of(RedshiftMasking::SqlExecutor).to receive(:apply_corrections)
        job.perform
      end

      it 'passes dry_run: true to SqlExecutor when specified' do
        expect(RedshiftMasking::SqlExecutor).to receive(:new).
          with(executor, anything, anything, dry_run: true).
          and_call_original
        allow_any_instance_of(RedshiftMasking::SqlExecutor).to receive(:create_masking_policies)
        allow_any_instance_of(RedshiftMasking::SqlExecutor).to receive(:apply_corrections)
        job.perform(dry_run: true)
      end
    end
  end
end
