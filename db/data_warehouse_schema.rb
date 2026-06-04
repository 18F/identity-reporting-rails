# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# This file is the source Rails uses to define your schema when running `bin/rails
# db:schema:load`. When creating a new database, `bin/rails db:schema:load` tends to
# be faster and is potentially less error prone than running all of your
# migrations from scratch. Old migrations may fail to apply correctly if those
# migrations use external dependencies or application code.
#
# It's strongly recommended that you check this file into your version control system.

ActiveRecord::Schema[8.1].define(version: 2026_06_03_120000) do
  create_schema "fraudops"
  create_schema "logs"
  create_schema "marts"
  create_schema "qa_marts"
  create_schema "system_tables"

  # These are extensions that must be enabled in order to support this database
  enable_extension "pg_catalog.plpgsql"

  create_table "logs.events", id: false, force: :cascade do |t|
    t.boolean "browser_bot"
    t.string "browser_device_name"
    t.boolean "browser_mobile"
    t.string "browser_name"
    t.string "browser_platform_name"
    t.string "browser_platform_version"
    t.string "browser_version"
    t.datetime "cloudwatch_timestamp", precision: nil
    t.string "git_branch"
    t.string "git_sha"
    t.string "hostname"
    t.string "id", null: false
    t.string "locale"
    t.string "log_filename"
    t.jsonb "message"
    t.string "name"
    t.boolean "new_event"
    t.string "path", limit: 12000
    t.integer "pid"
    t.string "service_provider"
    t.boolean "success"
    t.datetime "time", precision: nil
    t.string "trace_id"
    t.string "user_agent", limit: 12000
    t.string "user_id"
    t.string "user_ip"
    t.string "visit_id"
    t.string "visitor_id"
  end

  create_table "logs.production", id: false, force: :cascade do |t|
    t.string "action"
    t.datetime "cloudwatch_timestamp", precision: nil
    t.string "controller"
    t.decimal "duration", precision: 15, scale: 4
    t.string "format"
    t.string "git_branch"
    t.string "git_sha"
    t.string "host"
    t.string "ip"
    t.jsonb "message"
    t.string "method"
    t.string "path", limit: 12000
    t.integer "pid"
    t.integer "status"
    t.datetime "timestamp", precision: nil
    t.string "trace_id"
    t.string "user_agent", limit: 12000
    t.string "uuid", null: false
  end

  create_table "logs.sync_metadata", force: :cascade do |t|
    t.datetime "created_at", null: false
    t.datetime "last_sync_time", precision: nil
    t.string "table_name"
    t.datetime "updated_at", null: false
  end

  create_table "logs.unextracted_events", id: false, force: :cascade do |t|
    t.datetime "cloudwatch_timestamp", precision: nil
    t.jsonb "message"
  end

  create_table "logs.unextracted_production", id: false, force: :cascade do |t|
    t.datetime "cloudwatch_timestamp", precision: nil
    t.jsonb "message"
  end

  create_table "fraudops.frd_encrypted_events", primary_key: "event_key", id: { type: :string, limit: 256 }, force: :cascade do |t|
    t.string "bucket_name", limit: 256
    t.datetime "dw_created_at", precision: nil
    t.datetime "dw_processed_at", precision: nil
    t.string "message", limit: 65535
    t.date "partition_dt"
  end

  create_table "fraudops.frd_events", primary_key: "event_key", id: { type: :string, limit: 256 }, force: :cascade do |t|
    t.datetime "dw_created_at", precision: nil
    t.datetime "event_timestamp", precision: nil
    t.jsonb "message"
    t.string "user_id", limit: 256
  end

end
