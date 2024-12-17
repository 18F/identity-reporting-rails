# if ActiveRecord.version >= Gem::Version.new('7.2.0')
#   warn 'Unexpected ActiveRecord version, double check that the constructor ' \
#        'monkeypatch is still needed'
# end

# require 'active_record/connection_adapters/redshift_adapter'

# module IdentityReporting
#   module SchemaStatementsOverride
#     # ActiveRecord passes in options as a hash, but as of Ruby 3.0, they are interpreted
#     # separately than keyword options.
#     # This monkeypatch accepts either form
#     def create_database(name, positional_options = {}, **keyword_options)
#       super(name, **positional_options.merge(keyword_options).transform_keys(&:to_sym))
#     end
#   end
# end

# ActiveRecord::ConnectionAdapters::Redshift::SchemaStatements.
#   send(:prepend, IdentityReporting::SchemaStatementsOverride)
