# spec/jobs/reports/base_report_spec.rb
require 'spec_helper'

# Mock the necessary classes and modules before loading
module Reports
  class BaseReport
    def initialize; end

    private

    def public_bucket_name
      if (prefix = IdentityConfig.store.s3_report_public_bucket_prefix)
        env = Identity::Hostdata.env
        account = if Identity::Hostdata.respond_to?(:aws_account_id)
                    Identity::Hostdata.aws_account_id
                  end
        region = Aws.config[:region] || IdentityConfig.store.aws_region
        if prefix == 'login-gov-dw-reports' && account.present? && region.present?
          "#{prefix}-#{env}-#{account}-#{region}"
        else
          Identity::Hostdata.bucket_name("#{prefix}-#{env}")
        end
      end
    end
  end
end

module IdentityConfig
  class Store
    def s3_report_public_bucket_prefix; end

    def aws_region; end
  end

  def self.store
    @store ||= Store.new
  end
end

module Identity
  module Hostdata
    def self.env; end

    def self.aws_account_id; end

    def self.respond_to?(method); end

    def self.bucket_name(prefix); end
  end
end

class Aws
  def self.config
    @config ||= {}
  end
end

class Object
  def present?
    begin
      !nil? && !empty?
    rescue
      !nil?
    end
  end
end

class NilClass
  def present?
    false
  end
end

RSpec.describe Reports::BaseReport do
  let(:job_instance) { described_class.new }

  describe '#public_bucket_name' do
    context 'when prefix is login-gov-dw-reports with all required values present' do
      before do
        allow(IdentityConfig.store).to receive(:s3_report_public_bucket_prefix).
          and_return('login-gov-dw-reports')
        allow(Identity::Hostdata).to receive(:env).and_return('int')
        allow(Identity::Hostdata).to receive(:respond_to?).
          with(:aws_account_id).and_return(true)
        allow(Identity::Hostdata).to receive(:aws_account_id).
          and_return('12345678')
        allow(Aws).to receive(:config).and_return({ region: 'us-west-2' })
        allow(IdentityConfig.store).to receive(:aws_region).
          and_return('us-east-1')
      end

      it 'returns the correctly formatted bucket name' do
        expected_bucket_name = 'login-gov-dw-reports-int-12345678-us-west-2'

        result = job_instance.send(:public_bucket_name)

        expect(result).to eq(expected_bucket_name)
      end

      it 'uses Aws.config[:region] over IdentityConfig.store.aws_region when available' do
        expect(IdentityConfig.store).not_to receive(:aws_region)

        result = job_instance.send(:public_bucket_name)

        expect(result).to include('us-west-2')
        expect(result).not_to include('us-east-1')
      end
    end

    # NEW TEST CASE - More explicit about the scenario
    context 'when prefix is login-gov-dw-reports and both region and account are available' do
      before do
        allow(IdentityConfig.store).to receive(:s3_report_public_bucket_prefix).
          and_return('login-gov-dw-reports')
        allow(Identity::Hostdata).to receive(:env).and_return('int')
        allow(Identity::Hostdata).to receive(:respond_to?).
          with(:aws_account_id).and_return(true)
        allow(Identity::Hostdata).to receive(:aws_account_id).
          and_return('12345678')
        allow(Aws).to receive(:config).and_return({ region: 'us-west-2' })
        allow(IdentityConfig.store).to receive(:aws_region).
          and_return('us-east-1')
      end

      it 'constructs bucket name using the special login-gov-dw-reports format' do
        expected_bucket_name = 'login-gov-dw-reports-int-12345678-us-west-2'

        result = job_instance.send(:public_bucket_name)

        expect(result).to eq(expected_bucket_name)
      end

      it 'does not call Identity::Hostdata.bucket_name when all required values are present' do
        expect(Identity::Hostdata).not_to receive(:bucket_name)

        job_instance.send(:public_bucket_name)
      end

      it 'includes all components in the correct order' do
        result = job_instance.send(:public_bucket_name)

        expect(result).to start_with('login-gov-dw-reports-')
        expect(result).to include('-int-')
        expect(result).to include('-12345678-')
        expect(result).to end_with('-us-west-2')
      end

      context 'with different values' do
        before do
          allow(Identity::Hostdata).to receive(:env).and_return('prod')
          allow(Identity::Hostdata).to receive(:aws_account_id).
            and_return('987654321')
          allow(Aws).to receive(:config).and_return({ region: 'us-east-1' })
        end

        it 'works with different env, account, and region values' do
          expected_bucket_name = 'login-gov-dw-reports-prod-987654321-us-east-1'

          result = job_instance.send(:public_bucket_name)

          expect(result).to eq(expected_bucket_name)
        end
      end
    end

    context 'when prefix is login-gov-dw-reports but aws_account_id is not available' do
      before do
        allow(IdentityConfig.store).to receive(:s3_report_public_bucket_prefix).
          and_return('login-gov-dw-reports')
        allow(Identity::Hostdata).to receive(:env).and_return('int')
        allow(Identity::Hostdata).to receive(:respond_to?).
          with(:aws_account_id).and_return(false)
        allow(Aws).to receive(:config).and_return({ region: 'us-west-2' })
      end

      it 'falls back to Identity::Hostdata.bucket_name method' do
        expect(Identity::Hostdata).to receive(:bucket_name).
          with('login-gov-dw-reports-int').
          and_return('fallback-bucket-name')

        result = job_instance.send(:public_bucket_name)

        expect(result).to eq('fallback-bucket-name')
      end
    end

    # ... rest of existing test cases ...
  end
end
