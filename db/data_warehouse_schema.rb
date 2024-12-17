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

ActiveRecord::Schema[7.2].define(version: 2024_11_04_093856) do
  create_schema "idp"
  create_schema "logs"
  create_schema "system_tables"
  create_schema "test_pg_catalog"

  # These are extensions that must be enabled in order to support this database
  enable_extension "plpgsql"

  create_table "events", id: false, force: :cascade do |t|
    t.jsonb "message"
    t.datetime "cloudwatch_timestamp", precision: nil
    t.string "id", null: false
    t.string "name"
    t.datetime "time", precision: nil
    t.string "visitor_id"
    t.string "visit_id"
    t.string "log_filename"
    t.boolean "new_event"
    t.string "path", limit: 12000
    t.string "user_id"
    t.string "locale"
    t.string "user_ip"
    t.string "hostname"
    t.integer "pid"
    t.string "service_provider"
    t.string "trace_id"
    t.string "git_sha"
    t.string "git_branch"
    t.string "user_agent", limit: 12000
    t.string "browser_name"
    t.string "browser_version"
    t.string "browser_platform_name"
    t.string "browser_platform_version"
    t.string "browser_device_name"
    t.boolean "browser_mobile"
    t.boolean "browser_bot"
    t.boolean "success"
  end

  create_table "good_job_batches", id: :uuid, default: -> { "gen_random_uuid()" }, force: :cascade do |t|
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.text "description"
    t.jsonb "serialized_properties"
    t.text "on_finish"
    t.text "on_success"
    t.text "on_discard"
    t.text "callback_queue_name"
    t.integer "callback_priority"
    t.datetime "enqueued_at"
    t.datetime "discarded_at"
    t.datetime "finished_at"
  end

  create_table "good_job_executions", id: :uuid, default: -> { "gen_random_uuid()" }, force: :cascade do |t|
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.uuid "active_job_id", null: false
    t.text "job_class"
    t.text "queue_name"
    t.jsonb "serialized_params"
    t.datetime "scheduled_at"
    t.datetime "finished_at"
    t.text "error"
    t.integer "error_event", limit: 2
    t.index ["active_job_id", "created_at"], name: "index_good_job_executions_on_active_job_id_and_created_at"
  end

  create_table "good_job_processes", id: :uuid, default: -> { "gen_random_uuid()" }, force: :cascade do |t|
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.jsonb "state"
  end

  create_table "good_job_settings", id: :uuid, default: -> { "gen_random_uuid()" }, force: :cascade do |t|
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.text "key"
    t.jsonb "value"
    t.index ["key"], name: "index_good_job_settings_on_key", unique: true
  end

  create_table "good_jobs", id: :uuid, default: -> { "gen_random_uuid()" }, force: :cascade do |t|
    t.text "queue_name"
    t.integer "priority"
    t.jsonb "serialized_params"
    t.datetime "scheduled_at"
    t.datetime "performed_at"
    t.datetime "finished_at"
    t.text "error"
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.uuid "active_job_id"
    t.text "concurrency_key"
    t.text "cron_key"
    t.uuid "retried_good_job_id"
    t.datetime "cron_at"
    t.uuid "batch_id"
    t.uuid "batch_callback_id"
    t.boolean "is_discrete"
    t.integer "executions_count"
    t.text "job_class"
    t.integer "error_event", limit: 2
    t.index ["active_job_id", "created_at"], name: "index_good_jobs_on_active_job_id_and_created_at"
    t.index ["active_job_id"], name: "index_good_jobs_on_active_job_id"
    t.index ["batch_callback_id"], name: "index_good_jobs_on_batch_callback_id", where: "(batch_callback_id IS NOT NULL)"
    t.index ["batch_id"], name: "index_good_jobs_on_batch_id", where: "(batch_id IS NOT NULL)"
    t.index ["concurrency_key"], name: "index_good_jobs_on_concurrency_key_when_unfinished", where: "(finished_at IS NULL)"
    t.index ["cron_key", "created_at"], name: "index_good_jobs_on_cron_key_and_created_at_cond", where: "(cron_key IS NOT NULL)"
    t.index ["cron_key", "cron_at"], name: "index_good_jobs_on_cron_key_and_cron_at_cond", unique: true, where: "(cron_key IS NOT NULL)"
    t.index ["finished_at"], name: "index_good_jobs_jobs_on_finished_at", where: "((retried_good_job_id IS NULL) AND (finished_at IS NOT NULL))"
    t.index ["priority", "created_at"], name: "index_good_jobs_jobs_on_priority_created_at_when_unfinished", order: { priority: "DESC NULLS LAST" }, where: "(finished_at IS NULL)"
    t.index ["queue_name", "scheduled_at"], name: "index_good_jobs_on_queue_name_and_scheduled_at", where: "(finished_at IS NULL)"
    t.index ["scheduled_at"], name: "index_good_jobs_on_scheduled_at", where: "(finished_at IS NULL)"
  end

  create_table "production", id: false, force: :cascade do |t|
    t.jsonb "message"
    t.datetime "cloudwatch_timestamp", precision: nil
    t.string "uuid", null: false
    t.string "method"
    t.string "path", limit: 12000
    t.string "format"
    t.string "controller"
    t.string "action"
    t.integer "status"
    t.float "duration"
    t.string "git_sha"
    t.string "git_branch"
    t.datetime "timestamp", precision: nil
    t.integer "pid"
    t.string "user_agent", limit: 12000
    t.string "ip"
    t.string "host"
    t.string "trace_id"
  end

  create_table "stl_query", id: false, force: :cascade do |t|
    t.integer "userid"
    t.integer "query"
    t.string "label"
    t.bigint "xid"
    t.integer "pid"
    t.string "database"
    t.string "querytxt"
    t.datetime "starttime", precision: nil
    t.datetime "endtime", precision: nil
    t.integer "aborted"
    t.integer "insert_pristine"
    t.integer "concurency_scalling_status"
  end

  create_table "sync_metadata", force: :cascade do |t|
    t.string "table_name"
    t.datetime "last_sync_time", precision: nil
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
  end

  create_table "unextracted_events", id: false, force: :cascade do |t|
    t.jsonb "message"
    t.datetime "cloudwatch_timestamp", precision: nil
  end

  create_table "unextracted_production", id: false, force: :cascade do |t|
    t.jsonb "message"
    t.datetime "cloudwatch_timestamp", precision: nil
  end
end
