# frozen_string_literal: true

require 'rails_helper'

RSpec.describe RedshiftRoleAudit do
  let(:connection) do
    instance_double(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter).tap do |double|
      allow(double).to receive(:quote) { |value| "'#{value}'" }
      allow(double).to receive(:quote_column_name) { |value| %("#{value}") }
    end
  end
  let(:roles) { ['pii_viewers', 'dw_admin', 'sys:monitor'] }
  let(:user_grants) { [{ 'user_name' => 'pii_reader', 'role_name' => 'pii_viewers' }] }
  let(:role_grants) { [{ 'role_name' => 'dw_admin', 'granted_role_name' => 'sys:monitor' }] }
  let(:privileges) { Hash.new([]) }
  let(:executed) { [] }
  let(:errors) { [] }
  let(:revoke) { false }
  let(:logger) { instance_double(Logger, info: nil, warn: nil, error: nil) }
  let(:intended) do
    {
      'pii_viewers' => { members: ['pii_reader'], assigned_roles: [] },
      'dw_admin' => { members: [], assigned_roles: ['sys:monitor'] },
    }
  end
  let(:execute) do
    lambda do |sql|
      case sql
      when /FROM svv_roles/ then roles.map { |name| { 'role_name' => name } }
      when /FROM svv_user_grants/ then user_grants
      when /FROM svv_role_grants/ then role_grants
      when /FROM svv_relation_privileges/ then privileges[:relation]
      when /FROM svv_schema_privileges/ then privileges[:schema]
      when /FROM svv_database_privileges/ then privileges[:database]
      when /FROM svv_system_privileges/ then privileges[:system]
      else
        executed << sql
        []
      end
    end
  end
  let(:audit) do
    described_class.new(
      connection: connection, execute: execute, on_error: ->(e) { errors << e.message },
      revoke: revoke, logger: logger
    )
  end

  def findings(type)
    audit.audit_roles(intended).select { |finding| finding.type == type }
  end

  it 'logs a clean result and changes nothing when the cluster matches the config' do
    expect(audit.audit_roles(intended)).to be_empty
    expect(logger).to have_received(:info).with(/"clean":true/)
    expect(executed).to be_empty
  end

  context 'with an unexpected direct member' do
    let(:user_grants) do
      [
        { 'user_name' => 'pii_reader', 'role_name' => 'pii_viewers' },
        { 'user_name' => 'IAM:someone', 'role_name' => 'pii_viewers' },
      ]
    end

    it 'reports it and does not revoke while revoke is off' do
      expect(findings('role_member_unexpected').map { |f| f.detail[:user] }).
        to eq(['IAM:someone'])
      expect(logger).to have_received(:warn).
        with(/"finding":"role_member_unexpected".*"revoke_enabled":false.*"revoke_attempted":false/)
      expect(executed).to be_empty
    end

    context 'when revoke is on' do
      let(:revoke) { true }

      it 'revokes only that user and reports the attempt' do
        findings('role_member_unexpected')

        expect(executed).to eq(['REVOKE ROLE "pii_viewers" FROM "IAM:someone"'])
        expect(logger).to have_received(:warn).with(/"revoke_attempted":true/)
      end
    end
  end

  context 'with an admin option the config does not grant' do
    let(:revoke) { true }
    let(:user_grants) do
      [{ 'user_name' => 'pii_reader', 'role_name' => 'pii_viewers', 'admin_option' => true }]
    end

    it 'revokes the admin option and keeps the membership' do
      findings('role_admin_option_unexpected')

      expect(executed).to eq(['REVOKE ADMIN OPTION FOR ROLE "pii_viewers" FROM "pii_reader"'])
    end
  end

  context 'with a role that inherits a managed role' do
    let(:revoke) { true }
    let(:role_grants) do
      [
        { 'role_name' => 'dw_admin', 'granted_role_name' => 'sys:monitor' },
        { 'role_name' => 'analyst_role', 'granted_role_name' => 'pii_viewers' },
      ]
    end
    let(:roles) { ['pii_viewers', 'dw_admin', 'sys:monitor', 'analyst_role'] }

    it 'reports the inheritor and the unmanaged role, and revokes only the edge' do
      expect(findings('role_inheritor_unexpected').first.detail).
        to eq(grantee: 'analyst_role', granted: 'pii_viewers')
      expect(findings('role_unmanaged').map(&:role)).to eq(['analyst_role'])
      expect(executed.uniq).to eq(['REVOKE ROLE "pii_viewers" FROM ROLE "analyst_role"'])
    end

    it 'never revokes a sys: role from a role it does not manage' do
      role_grants << { 'role_name' => 'sys:superuser', 'granted_role_name' => 'pii_viewers' }
      roles << 'sys:superuser'

      audit.audit_roles(intended)

      expect(executed.join).not_to include('sys:superuser')
    end
  end

  context 'with a nested role the config does not assign' do
    let(:revoke) { true }
    let(:role_grants) do
      [
        { 'role_name' => 'dw_admin', 'granted_role_name' => 'sys:monitor' },
        { 'role_name' => 'dw_admin', 'granted_role_name' => 'sys:secadmin' },
      ]
    end

    it 'revokes the extra nested role and keeps the configured one' do
      expect(findings('role_nested_unexpected').map { |f| f.detail[:granted] }).
        to eq(['sys:secadmin'])
      expect(executed.uniq).to eq(['REVOKE ROLE "sys:secadmin" FROM ROLE "dw_admin"'])
    end
  end

  context 'when a user reaches a managed role through a chain of roles' do
    let(:user_grants) do
      [
        { 'user_name' => 'pii_reader', 'role_name' => 'pii_viewers' },
        { 'user_name' => 'IAM:chained', 'role_name' => 'analyst_role' },
      ]
    end
    let(:role_grants) do
      [
        { 'role_name' => 'analyst_role', 'granted_role_name' => 'middle_role' },
        { 'role_name' => 'middle_role', 'granted_role_name' => 'pii_viewers' },
      ]
    end

    it 'reports the effective holder with the chain and never revokes it directly' do
      holders = findings('role_effective_user_unexpected')

      expect(holders.map { |f| f.detail[:user] }).to eq(['IAM:chained'])
      expect(holders.first.detail[:via]).to contain_exactly('analyst_role', 'middle_role')
      expect(holders.first.revoke_sql).to be_nil
    end
  end

  context 'with a role whose feature flag is off (no intended members)' do
    let(:intended) do
      { 'pii_viewers' => { members: [], assigned_roles: [] } }
    end
    let(:roles) { ['pii_viewers'] }
    let(:role_grants) { [] }

    it 'treats every remaining member as unexpected' do
      expect(findings('role_member_unexpected').map { |f| f.detail[:user] }).
        to eq(['pii_reader'])
    end
  end

  context 'with a role whose intended state is unknown' do
    let(:intended) { { 'pii_viewers' => nil } }
    let(:revoke) { true }
    let(:user_grants) { [{ 'user_name' => 'anyone', 'role_name' => 'pii_viewers' }] }
    let(:roles) { ['pii_viewers'] }
    let(:role_grants) { [] }

    it 'fails closed by comparing nothing and revoking nothing' do
      audit.audit_roles(intended)

      expect(executed).to be_empty
    end
  end

  describe '#audit_privileges' do
    let(:privileges) do
      {
        relation: [
          { 'namespace_name' => 'marts',
            'relation_name' => 'users',
            'privilege_type' => 'SELECT',
            'identity_name' => 'pii_viewers' },
        ],
        schema: [
          { 'namespace_name' => 'marts',
            'privilege_type' => 'USAGE',
            'identity_name' => 'pii_viewers',
            'privilege_scope' => 'SCHEMA' },
          { 'namespace_name' => 'marts',
            'privilege_type' => 'SELECT',
            'identity_name' => 'pii_viewers',
            'privilege_scope' => 'TABLES' },
        ],
        database: [
          { 'database_name' => 'analytics',
            'privilege_type' => 'CREATE',
            'identity_name' => 'pii_viewers',
            'privilege_scope' => 'DATABASE' },
          { 'database_name' => 'analytics',
            'privilege_type' => 'SELECT',
            'identity_name' => 'pii_viewers',
            'privilege_scope' => 'TABLES' },
        ],
        system: [{ 'system_privilege' => 'CREATE USER', 'identity_name' => 'pii_viewers' }],
      }
    end

    it 'reports every object privilege on a managed role without revoking by default' do
      result = audit.audit_privileges(['pii_viewers'])

      expect(result.map(&:type).uniq).to eq(['role_privilege_unexpected'])
      expect(result.size).to eq(6)
      expect(executed).to be_empty
    end

    context 'when revoke is on' do
      let(:revoke) { true }

      it 'revokes the forms it knows and leaves scoped permissions to a person' do
        audit.audit_privileges(['pii_viewers'])

        expect(executed).to contain_exactly(
          'REVOKE SELECT ON TABLE "marts"."users" FROM ROLE "pii_viewers"',
          'REVOKE USAGE ON SCHEMA "marts" FROM ROLE "pii_viewers"',
          'REVOKE CREATE ON DATABASE "analytics" FROM ROLE "pii_viewers"',
          'REVOKE CREATE USER FROM ROLE "pii_viewers"',
        )
        expect(logger).to have_received(:warn).with(/"revocable":false/).twice
      end
    end

    it 'quotes identifiers that contain punctuation' do
      privileges[:relation] = [
        { 'namespace_name' => 'marts',
          'relation_name' => 'we.ird',
          'privilege_type' => 'SELECT',
          'identity_name' => 'pii_viewers' },
      ]
      audit_with_revoke = described_class.new(
        connection: connection, execute: execute, on_error: ->(_e) {}, revoke: true,
        logger: logger
      )

      audit_with_revoke.audit_privileges(['pii_viewers'])

      expect(executed).to include('REVOKE SELECT ON TABLE "marts"."we.ird" FROM ROLE "pii_viewers"')
    end

    it 'makes no query when there are no managed roles' do
      expect(audit.audit_privileges([])).to be_empty
    end
  end

  context 'when a revoke fails' do
    let(:revoke) { true }
    let(:user_grants) do
      [
        { 'user_name' => 'IAM:a', 'role_name' => 'pii_viewers' },
        { 'user_name' => 'IAM:b', 'role_name' => 'pii_viewers' },
      ]
    end
    let(:execute) do
      lambda do |sql|
        case sql
        when /FROM svv_roles/ then roles.map { |name| { 'role_name' => name } }
        when /FROM svv_user_grants/ then user_grants
        when /FROM svv_role_grants/ then role_grants
        else
          executed << sql
          raise ActiveRecord::StatementInvalid, 'permission denied' if sql.include?('IAM:a')

          []
        end
      end
    end

    it 'records the failure, logs it, and still attempts the remaining revokes' do
      audit.audit_roles(intended)

      expect(errors).to eq(['permission denied'])
      expect(executed).to eq(
        ['REVOKE ROLE "pii_viewers" FROM "IAM:a"', 'REVOKE ROLE "pii_viewers" FROM "IAM:b"'],
      )
      expect(logger).to have_received(:error).with(/revoke failed/)
    end
  end

  it 'does not report a clean audit when a catalog read fails' do
    failing = described_class.new(
      connection: connection, on_error: ->(_e) {}, logger: logger,
      execute: ->(_sql) { raise ActiveRecord::StatementInvalid, 'catalog unavailable' }
    )

    expect { failing.audit_roles(intended) }.to raise_error(ActiveRecord::StatementInvalid)
    expect(logger).not_to have_received(:info).with(/clean/)
  end
end
