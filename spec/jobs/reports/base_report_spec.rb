# frozen_string_literal: true

require 'rails_helper'

RSpec.describe Reports::BaseReport do
  subject(:report) { described_class.new }

  describe '#generate_s3_paths' do
    let(:name) { 'test-report' }
    let(:extension) { 'json' }
    let(:now) { Time.zone.parse('2026-02-12T12:00:00Z') }
    let(:host_data_env) { 'prod' }

    before do
      allow(Identity::Hostdata).to receive(:env).and_return(host_data_env)
    end

    context 'with a directory' do
      it 'inserts the directory between the environment and the report name' do
        latest, dated = report.send(
          :generate_s3_paths, name, extension, directory: 'idp', now: now
        )

        expect(latest).to eq('prod/idp/test-report/latest.test-report.json')
        expect(dated).to eq('prod/idp/test-report/2026/2026-02-12.test-report.json')
      end
    end

    context 'with a directory and a subname' do
      it 'inserts the directory and includes the subname in the filename' do
        latest, dated = report.send(
          :generate_s3_paths, name, extension, directory: 'idp', subname: 'detail', now: now
        )

        expect(latest).to eq('prod/idp/test-report/latest.test-report/detail.json')
        expect(dated).to eq('prod/idp/test-report/2026/2026-02-12.test-report/detail.json')
      end
    end

    context 'without a directory' do
      it 'omits the directory segment from the path' do
        latest, dated = report.send(
          :generate_s3_paths, name, extension, now: now
        )

        expect(latest).to eq('prod/test-report/latest.test-report.json')
        expect(dated).to eq('prod/test-report/2026/2026-02-12.test-report.json')
      end
    end

    context 'with a different environment' do
      let(:host_data_env) { 'staging' }

      it 'uses the correct environment prefix with the directory' do
        latest, dated = report.send(
          :generate_s3_paths, name, extension, directory: 'pivcac', now: now
        )

        expect(latest).to eq('staging/pivcac/test-report/latest.test-report.json')
        expect(dated).to eq('staging/pivcac/test-report/2026/2026-02-12.test-report.json')
      end
    end
  end
end
