if ActiveRecord.version > Gem::Version.new('8.1.3')
  warn 'Unexpected ActiveRecord version, double check that the constructor ' \
       'monkeypatch is still needed'
end

require 'active_record/connection_adapters/redshift_adapter'

module ActiveRecord
  module ConnectionAdapters
    class AbstractAdapter
      unless method_defined?(:check_if_write_query)
        def check_if_write_query(sql)
          if preventing_writes? && write_query?(sql)
            raise ActiveRecord::ReadOnlyError,
                  "Write query attempted while in readonly mode: #{sql}"
          end
        end
      end
    end

    module DatabaseStatements
      unless method_defined?(:mark_transaction_written_if_write)
        def mark_transaction_written_if_write(sql)
          transaction = current_transaction
          if transaction.open?
            transaction.written ||= write_query?(sql)
          end
        end
      end
    end
  end
end

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

  module RedshiftRails81ColumnCompat
    def initialize(name, cast_type, default, sql_type_metadata = nil, null = true, default_function = nil, **options)
      ActiveRecord::ConnectionAdapters::Column.instance_method(:initialize).bind_call(
        self, name, cast_type, default, sql_type_metadata, null, default_function, **options
      )
    end
  end

  module RedshiftRails81SchemaStatementsCompat
    def new_column(name, default, sql_type_metadata = nil, null = true, _table_name = nil, default_function = nil)
      cast_type =
        if sql_type_metadata
          get_oid_type(
            sql_type_metadata.oid.to_i,
            sql_type_metadata.fmod.to_i,
            name,
            sql_type_metadata.sql_type,
          )
        end
      ActiveRecord::ConnectionAdapters::RedshiftColumn.new(
        name,
        cast_type,
        default,
        sql_type_metadata,
        null,
        default_function,
      )
    end
  end
end

if ActiveRecord.version >= Gem::Version.new('8.1.0')
  ActiveRecord::ConnectionAdapters::RedshiftColumn.prepend(IdentityReporting::RedshiftRails81ColumnCompat)
  ActiveRecord::ConnectionAdapters::Redshift::SchemaStatements.prepend(
    IdentityReporting::RedshiftRails81SchemaStatementsCompat,
  )
end

ActiveRecord::ConnectionAdapters::Redshift::SchemaStatements.
  send(:prepend, IdentityReporting::SchemaStatementsOverride)
ActiveRecord::ConnectionAdapters::Redshift::ColumnMethods.
  send(:prepend, IdentityReporting::RedshiftColumnMethodsOverride)
