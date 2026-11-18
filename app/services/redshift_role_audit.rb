# frozen_string_literal: true

# Compares the cluster's role membership, role inheritance and role object
# privileges with redshift_config.yaml. It always reports drift. It only revokes
# when `revoke` is true (IdentityConfig redshift_role_audit_revoke), and it never
# drops a role, touches a `sys:` role's own grants, or changes users' direct
# object privileges or groups.
class RedshiftRoleAudit
  NAME = 'RedshiftRoleAudit'
  SYSTEM_ROLE_PREFIX = 'sys:'
  PRIVILEGE_PATTERN = /\A[A-Z][A-Z ]*\z/
  RELATION_PRIVILEGES = %w[SELECT INSERT UPDATE DELETE REFERENCES DROP].freeze
  SCHEMA_PRIVILEGES = %w[CREATE USAGE ALTER DROP].freeze
  DATABASE_PRIVILEGES = %w[CREATE TEMP TEMPORARY ALTER].freeze

  Finding = Struct.new(:type, :role, :detail, :revoke_sql, keyword_init: true)

  # connection: only used to quote identifiers and values.
  # execute: callable that runs SQL and returns rows. It must raise on failure, so
  #   a failed catalog read is never reported as a clean audit.
  # on_error: called with the exception when a revoke fails; the remaining revokes
  #   still run, and the caller decides how the failure surfaces.
  def initialize(connection:, execute:, on_error:, revoke: false, logger: Rails.logger)
    @connection = connection
    @execute = execute
    @on_error = on_error
    @revoke = revoke
    @logger = logger
    @catalog = Catalog.new(execute: execute, connection: connection)
  end

  # intended: { role_name => { members: [...], assigned_roles: [...] } } for every
  # role in the config. A nil value means the intended state is unknown, so the
  # role's members and nested roles are not compared (fail closed).
  def audit_roles(intended)
    managed = intended.keys
    user_grants = @catalog.user_grants
    role_grants = @catalog.role_grants

    findings = unmanaged_role_findings(managed) +
               member_findings(intended, user_grants) +
               edge_findings(intended, role_grants) +
               effective_user_findings(intended, user_grants, role_grants)
    report('roles', findings)
  end

  def audit_privileges(role_names)
    findings = @catalog.privileges(role_names).map { |privilege| privilege_finding(privilege) }
    report('privileges', findings)
  end

  private

  def unmanaged_role_findings(managed)
    @catalog.roles.filter_map do |role|
      next if managed.include?(role) || role.start_with?(SYSTEM_ROLE_PREFIX)

      Finding.new(type: 'role_unmanaged', role: role, detail: {})
    end
  end

  def member_findings(intended, user_grants)
    user_grants.filter_map do |grant|
      role = grant['role_name']
      config = intended[role]
      next unless config

      user = grant['user_name']
      if !config[:members].include?(user)
        Finding.new(
          type: 'role_member_unexpected', role: role, detail: { user: user },
          revoke_sql: "REVOKE ROLE #{q(role)} FROM #{q(user)}"
        )
      elsif truthy?(grant['admin_option'])
        Finding.new(
          type: 'role_admin_option_unexpected', role: role, detail: { user: user },
          revoke_sql: "REVOKE ADMIN OPTION FOR ROLE #{q(role)} FROM #{q(user)}"
        )
      end
    end
  end

  def edge_findings(intended, role_grants)
    allowed = intended_edges(intended)

    role_grants.filter_map do |edge|
      grantee = edge['role_name']
      granted = edge['granted_role_name']
      next if allowed.include?([grantee, granted])

      type = if intended.key?(grantee)
               next if intended[grantee].nil?

               'role_nested_unexpected'
             elsif intended.key?(granted)
               'role_inheritor_unexpected'
             end
      next unless type

      # The role that is granted is the managed one in an inheritor finding.
      role = type == 'role_nested_unexpected' ? grantee : granted
      revocable = !grantee.start_with?(SYSTEM_ROLE_PREFIX)
      Finding.new(
        type: type, role: role, detail: { grantee: grantee, granted: granted },
        revoke_sql: revocable ? "REVOKE ROLE #{q(granted)} FROM ROLE #{q(grantee)}" : nil
      )
    end
  end

  # Report-only. Direct members and unexpected edges are the fixable causes; this
  # shows who ends up holding a managed role through a chain of roles.
  def effective_user_findings(intended, user_grants, role_grants)
    actual_edges = role_grants.map { |e| [e['role_name'], e['granted_role_name']] }
    actual_direct = user_grants.group_by { |g| g['role_name'] }.
      transform_values { |grants| grants.map { |g| g['user_name'] } }
    intended_direct = intended.transform_values { |config| config ? config[:members] : [] }

    intended.keys.flat_map do |role|
      expected = holders(role, intended_edges(intended), intended_direct)
      holders(role, actual_edges, actual_direct).filter_map do |user, via|
        next if expected.key?(user) || via == [role]

        Finding.new(
          type: 'role_effective_user_unexpected', role: role,
          detail: { user: user, via: via - [role] }
        )
      end
    end
  end

  def intended_edges(intended)
    intended.flat_map do |role, config|
      Array(config && config[:assigned_roles]).map { |granted| [role, granted] }
    end
  end

  # Users who hold `role` directly or through roles that inherit it, mapped to the
  # shortest chain of roles (holder first) that reaches it. One entry per user, so a
  # role with many nested paths reports each user once.
  def holders(role, edges, direct)
    paths = { role => [role] }
    queue = [role]
    until queue.empty?
      current = queue.shift
      edges.each do |grantee, granted|
        next if granted != current || paths.key?(grantee)

        paths[grantee] = [grantee] + paths[current]
        queue << grantee
      end
    end

    paths.each_with_object({}) do |(holder_role, path), users|
      direct.fetch(holder_role, []).each { |user| users[user] ||= path }
    end
  end

  def privilege_finding(privilege)
    Finding.new(
      type: 'role_privilege_unexpected', role: privilege[:role],
      detail: privilege.except(:role).compact, revoke_sql: privilege_revoke_sql(privilege)
    )
  end

  # Only forms with a known REVOKE syntax are revocable. Anything else, such as
  # scoped permissions, is reported for a person to handle.
  def privilege_revoke_sql(privilege)
    name = privilege[:privilege].to_s.upcase
    return unless PRIVILEGE_PATTERN.match?(name)

    role = q(privilege[:role])
    case privilege[:kind]
    when 'relation'
      return unless RELATION_PRIVILEGES.include?(name)

      "REVOKE #{name} ON TABLE #{q(privilege[:schema])}.#{q(privilege[:object])} FROM ROLE #{role}"
    when 'schema'
      return unless SCHEMA_PRIVILEGES.include?(name) && privilege[:scope].to_s.upcase == 'SCHEMA'

      "REVOKE #{name} ON SCHEMA #{q(privilege[:schema])} FROM ROLE #{role}"
    when 'database'
      return unless DATABASE_PRIVILEGES.include?(name)
      return unless privilege[:scope].to_s.upcase == 'DATABASE'

      "REVOKE #{name} ON DATABASE #{q(privilege[:object])} FROM ROLE #{role}"
    when 'system'
      "REVOKE #{name} FROM ROLE #{role}"
    end
  end

  def report(scope, findings)
    attempted = @revoke ? findings.select(&:revoke_sql) : []
    attempted.each { |finding| revoke(finding) }

    if findings.empty?
      @logger.info({ name: NAME, scope: scope, clean: true }.to_json)
      return findings
    end

    findings.each do |finding|
      @logger.warn(
        {
          name: NAME,
          scope: scope,
          finding: finding.type,
          role: finding.role,
          detail: finding.detail,
          revocable: finding.revoke_sql.present?,
          revoke_enabled: @revoke,
          revoke_attempted: attempted.include?(finding),
        }.to_json,
      )
    end
    @logger.warn(
      {
        name: NAME,
        scope: scope,
        findings_detected: findings.size,
        revoke_enabled: @revoke,
        revoke_attempted: attempted.size,
      }.to_json,
    )
    findings
  end

  def revoke(finding)
    @execute.call(finding.revoke_sql)
  rescue ActiveRecord::StatementInvalid => e
    @logger.error(
      {
        name: NAME,
        error: 'revoke failed',
        finding: finding.type,
        role: finding.role,
        message: e.message,
      }.to_json,
    )
    @on_error.call(e)
  end

  def q(identifier)
    @connection.quote_column_name(identifier)
  end

  def truthy?(value)
    [true, 't', 'true'].include?(value)
  end
end
