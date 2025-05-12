class AddColumnsToEventsAndProductionTable < ActiveRecord::Migration[7.2]
  def up
    if connection.adapter_name.downcase.include?('redshift')

      execute <<-SQL
        ALTER TABLE logs.events
          ADD COLUMN id VARCHAR(256) NOT NULL DEFAULT '';
      SQL

      execute <<-SQL
        ALTER TABLE logs.events ADD PRIMARY KEY (id);
      SQL

      execute <<-SQL
        ALTER TABLE logs.production
          ADD COLUMN uuid VARCHAR(256) NOT NULL DEFAULT '';
      SQL

      execute <<-SQL
        ALTER TABLE logs.production ADD PRIMARY KEY (uuid) ;
      SQL
      
    else      
      # Add columns to 'logs.events' table
      add_column 'logs.events', :id, :string, null: false
      # Add columns to 'logs.production' table
      add_column 'logs.production', :uuid, :string, null: false
    end

    add_column 'logs.events', :name, :string
    add_column 'logs.events', :time, :timestamp
    add_column 'logs.events', :visitor_id, :string
    add_column 'logs.events', :visit_id, :string
    add_column 'logs.events', :log_filename, :string
    add_column 'logs.events', :new_event, :boolean, null: true
    add_column 'logs.events', :path, :string, limit: 12000
    add_column 'logs.events', :user_id, :string
    add_column 'logs.events', :locale, :string
    add_column 'logs.events', :user_ip, :string
    add_column 'logs.events', :hostname, :string
    add_column 'logs.events', :pid, :integer
    add_column 'logs.events', :service_provider, :string
    add_column 'logs.events', :trace_id, :string
    add_column 'logs.events', :git_sha, :string
    add_column 'logs.events', :git_branch, :string
    add_column 'logs.events', :user_agent, :string, limit: 12000
    add_column 'logs.events', :browser_name, :string
    add_column 'logs.events', :browser_version, :string
    add_column 'logs.events', :browser_platform_name, :string
    add_column 'logs.events', :browser_platform_version, :string
    add_column 'logs.events', :browser_device_name, :string
    add_column 'logs.events', :browser_mobile, :boolean, null: true
    add_column 'logs.events', :browser_bot, :boolean, null: true
    add_column 'logs.events', :success, :boolean, null: true
    
    # Add columns to 'logs.production' table
    add_column 'logs.production', :method, :string
    add_column 'logs.production', :path, :string, limit: 12000
    add_column 'logs.production', :format, :string
    add_column 'logs.production', :controller, :string
    add_column 'logs.production', :action, :string
    add_column 'logs.production', :status, :integer
    add_column 'logs.production', :duration, :float
    add_column 'logs.production', :git_sha, :string
    add_column 'logs.production', :git_branch, :string
    add_column 'logs.production', :timestamp, :timestamp
    add_column 'logs.production', :pid, :integer, null: true
    add_column 'logs.production', :user_agent, :string, limit: 12000
    add_column 'logs.production', :ip, :string
    add_column 'logs.production', :host, :string
    add_column 'logs.production', :trace_id, :string
  end
end
