require 'rails_helper'

RSpec.describe QuicksightUnexpectedUserDetectionJob, type: :job do
  let(:rails_job) { QuicksightUnexpectedUserDetectionJob.new }
  let(:logger) { instance_double(IdentityJobLogSubscriber) }
  let(:log_entry) { instance_double(Logger) }
  let!(:user_config_path) { Rails.root.join('spec', 'fixtures', 'users.yml') }

  before do
    allow(IdentityJobLogSubscriber).to receive(:new).and_return(logger)
    allow(logger).to receive(:logger).and_return(log_entry)
    allow(Identity::Hostdata).to receive(:env).and_return('testenv')
  end

  describe '#perform' do
    context 'when unexpected users exist in QuickSight' do
      it 'logs the unexpected users detected' do
      end
    end

    context 'when no unexpected users exist in QuickSight' do
      it 'does not log any info' do
      end
    end
  end
end
