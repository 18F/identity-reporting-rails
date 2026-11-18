# frozen_string_literal: true

class RedshiftRoleAudit
  # Read-only catalog queries. Every row it returns is metadata, never table data.
  class Catalog
    def initialize(execute:, connection:)
      @execute = execute
      @connection = connection
    end

    def roles
      rows('SELECT role_name FROM svv_roles').map { |row| row['role_name'] }
    end

    def user_grants
      rows('SELECT user_name, role_name, admin_option FROM svv_user_grants')
    end

    # role_name is the grantee, granted_role_name is the role it inherits.
    def role_grants
      rows('SELECT role_name, granted_role_name FROM svv_role_grants')
    end

    def privileges(role_names)
      return [] if role_names.empty?

      list = role_names.map { |name| @connection.quote(name) }.join(', ')
      relation_privileges(list) + schema_privileges(list) +
        database_privileges(list) + system_privileges(list)
    end

    private

    def rows(sql)
      @execute.call(sql).to_a
    end

    def relation_privileges(list)
      rows(<<~SQL).map do |row|
        SELECT namespace_name, relation_name, privilege_type, identity_name
        FROM svv_relation_privileges
        WHERE identity_type = 'role' AND identity_name IN (#{list})
      SQL
        {
          kind: 'relation',
          role: row['identity_name'],
          privilege: row['privilege_type'],
          schema: row['namespace_name'],
          object: row['relation_name'],
        }
      end
    end

    def schema_privileges(list)
      rows(<<~SQL).map do |row|
        SELECT namespace_name, privilege_type, identity_name, privilege_scope
        FROM svv_schema_privileges
        WHERE identity_type = 'role' AND identity_name IN (#{list})
      SQL
        {
          kind: 'schema',
          role: row['identity_name'],
          privilege: row['privilege_type'],
          schema: row['namespace_name'],
          scope: row['privilege_scope'],
        }
      end
    end

    def database_privileges(list)
      rows(<<~SQL).map do |row|
        SELECT database_name, privilege_type, identity_name, privilege_scope
        FROM svv_database_privileges
        WHERE identity_type = 'role' AND identity_name IN (#{list})
      SQL
        {
          kind: 'database',
          role: row['identity_name'],
          privilege: row['privilege_type'],
          object: row['database_name'],
          scope: row['privilege_scope'],
        }
      end
    end

    def system_privileges(list)
      rows(<<~SQL).map do |row|
        SELECT system_privilege, identity_name
        FROM svv_system_privileges
        WHERE identity_type = 'role' AND identity_name IN (#{list})
      SQL
        { kind: 'system', role: row['identity_name'], privilege: row['system_privilege'] }
      end
    end
  end
end
