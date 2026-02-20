if ActiveRecord.version > Gem::Version.new('8.0.4')
  warn 'Unexpected ActiveRecord version, double check that the constructor ' \
       'monkeypatch is still needed'
end

require 'active_record/connection_adapters/redshift_adapter'

module IdentityReporting
  module SchemaStatementsOverride
    # ActiveRecord passes in options as a hash, but as of Ruby 3.0, they are interpreted
    # separately than keyword options.
    # This monkeypatch accepts either form
    def create_database(name, positional_options = {}, **keyword_options)
      super(name, **positional_options.merge(keyword_options).transform_keys(&:to_sym))
    end

    def create_schema(schema_name, *args, **kwargs)
      super
    rescue ActiveRecord::StatementInvalid => e
      duplicate_schema_error = e.cause.is_a?(PG::DuplicateSchema) ||
                               e.message.include?('PG::DuplicateSchema')

      raise unless duplicate_schema_error
    end
  end

  module RedshiftColumnMethodsOverride
    def column(name, type, **options)
      return super unless %i[json jsonb].include?(type&.to_sym)

      super(name, 'SUPER', **options)
    end

    def primary_key(name, type = :primary_key, **options)
      return super unless type == :uuid

      options[:default] = options.fetch(:default, 'uuid_generate_v4()')
      options[:primary_key] = true
      column(name, type, **options)
    end

    def json(name, **options)
      column(name, 'SUPER', **options)
    end

    def jsonb(name, **options)
      column(name, 'SUPER', **options)
    end
  end
end

ActiveRecord::ConnectionAdapters::Redshift::SchemaStatements.
  send(:prepend, IdentityReporting::SchemaStatementsOverride)
ActiveRecord::ConnectionAdapters::Redshift::ColumnMethods.
  send(:prepend, IdentityReporting::RedshiftColumnMethodsOverride)
