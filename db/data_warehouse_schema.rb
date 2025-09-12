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

ActiveRecord::Schema[7.2].define(version: 2025_09_03_184139) do
  create_schema "fcms"
  create_schema "idp"
  create_schema "logs"
  create_schema "marts"
  create_schema "qa_marts"
  create_schema "system_tables"
  create_schema "test_pg_catalog"

  # These are extensions that must be enabled in order to support this database
  enable_extension "plpgsql"

  create_table "agencies", force: :cascade do |t|
    t.string "name", null: false
    t.string "abbreviation"
  end

  create_table "articles", id: false, force: :cascade do |t|
    t.integer "id"
    t.string "title"
    t.text "content"
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
  end

  create_table "events", id: false, force: :cascade do |t|
    t.text "message"
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
    t.datetime "jobs_finished_at"
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
    t.text "error_backtrace", array: true
    t.uuid "process_id"
    t.interval "duration"
    t.index ["active_job_id", "created_at"], name: "index_good_job_executions_on_active_job_id_and_created_at"
    t.index ["process_id", "created_at"], name: "index_good_job_executions_on_process_id_and_created_at"
  end

  create_table "good_job_processes", id: :uuid, default: -> { "gen_random_uuid()" }, force: :cascade do |t|
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.jsonb "state"
    t.integer "lock_type", limit: 2
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
    t.text "labels", array: true
    t.uuid "locked_by_id"
    t.datetime "locked_at"
    t.index ["active_job_id", "created_at"], name: "index_good_jobs_on_active_job_id_and_created_at"
    t.index ["batch_callback_id"], name: "index_good_jobs_on_batch_callback_id", where: "(batch_callback_id IS NOT NULL)"
    t.index ["batch_id"], name: "index_good_jobs_on_batch_id", where: "(batch_id IS NOT NULL)"
    t.index ["concurrency_key", "created_at"], name: "index_good_jobs_on_concurrency_key_and_created_at"
    t.index ["concurrency_key"], name: "index_good_jobs_on_concurrency_key_when_unfinished", where: "(finished_at IS NULL)"
    t.index ["cron_key", "created_at"], name: "index_good_jobs_on_cron_key_and_created_at_cond", where: "(cron_key IS NOT NULL)"
    t.index ["cron_key", "cron_at"], name: "index_good_jobs_on_cron_key_and_cron_at_cond", unique: true, where: "(cron_key IS NOT NULL)"
    t.index ["finished_at"], name: "index_good_jobs_jobs_on_finished_at", where: "((retried_good_job_id IS NULL) AND (finished_at IS NOT NULL))"
    t.index ["labels"], name: "index_good_jobs_on_labels", where: "(labels IS NOT NULL)", using: :gin
    t.index ["locked_by_id"], name: "index_good_jobs_on_locked_by_id", where: "(locked_by_id IS NOT NULL)"
    t.index ["priority", "created_at"], name: "index_good_job_jobs_for_candidate_lookup", where: "(finished_at IS NULL)"
    t.index ["priority", "created_at"], name: "index_good_jobs_jobs_on_priority_created_at_when_unfinished", order: { priority: "DESC NULLS LAST" }, where: "(finished_at IS NULL)"
    t.index ["priority", "scheduled_at"], name: "index_good_jobs_on_priority_scheduled_at_unfinished_unlocked", where: "((finished_at IS NULL) AND (locked_by_id IS NULL))"
    t.index ["queue_name", "scheduled_at"], name: "index_good_jobs_on_queue_name_and_scheduled_at", where: "(finished_at IS NULL)"
    t.index ["scheduled_at"], name: "index_good_jobs_on_scheduled_at", where: "(finished_at IS NULL)"
  end

  create_table "iaa_gtcs", force: :cascade do |t|
    t.string "gtc_number"
    t.integer "mod_number", default: 0, null: false
    t.date "start_date"
    t.date "end_date"
    t.decimal "estimated_amount", precision: 12, scale: 2
    t.bigint "partner_account_id"
  end

  create_table "iaa_orders", force: :cascade do |t|
    t.integer "order_number"
    t.integer "mod_number", default: 0
    t.date "start_date"
    t.date "end_date"
    t.decimal "estimated_amount", precision: 12, scale: 2
    t.integer "pricing_model", default: 2
    t.bigint "iaa_gtc_id"
  end

  create_table "integration_usages", force: :cascade do |t|
    t.bigint "iaa_order_id"
    t.bigint "integration_id"
  end

  create_table "integrations", force: :cascade do |t|
    t.string "issuer"
    t.string "name"
    t.integer "dashboard_identifier"
    t.bigint "partner_account_id"
    t.bigint "integration_status_id"
    t.bigint "service_provider_id"
  end

  create_table "partner_account_statuses", force: :cascade do |t|
    t.string "name", null: false
    t.integer "order"
    t.string "partner_name"
  end

  create_table "partner_accounts", force: :cascade do |t|
    t.string "name", null: false
    t.text "description"
    t.string "requesting_agency", null: false
    t.date "became_partner"
    t.bigint "agency_id"
    t.bigint "partner_account_status_id"
    t.bigint "crm_id"
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
    t.decimal "duration", precision: 15, scale: 4
    t.string "git_sha"
    t.string "git_branch"
    t.datetime "timestamp", precision: nil
    t.integer "pid"
    t.string "user_agent", limit: 12000
    t.string "ip"
    t.string "host"
    t.string "trace_id"
  end

  create_table "profiles", force: :cascade do |t|
    t.integer "user_id", null: false
    t.boolean "active", default: false, null: false
    t.datetime "verified_at"
    t.datetime "activated_at"
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.integer "deactivation_reason"
    t.jsonb "proofing_components", default: {}
    t.string "initiating_service_provider_issuer"
    t.datetime "fraud_review_pending_at"
    t.datetime "fraud_rejection_at"
    t.datetime "gpo_verification_pending_at"
    t.integer "fraud_pending_reason"
    t.datetime "gpo_verification_expired_at"
    t.integer "idv_level"
    t.datetime "in_person_verification_pending_at"
    t.datetime "timestamp", precision: nil
    t.index ["user_id"], name: "index_profiles_on_user_id"
  end

  create_table "service_providers", force: :cascade do |t|
    t.string "issuer", null: false
    t.string "friendly_name"
    t.text "description"
    t.text "metadata_url"
    t.text "acs_url"
    t.text "assertion_consumer_logout_service_url"
    t.text "logo"
    t.string "signature"
    t.string "block_encryption", default: "aes256-cbc", null: false
    t.text "sp_initiated_login_url"
    t.text "return_to_sp_url"
    t.json "attribute_bundle"
    t.boolean "active", default: false, null: false
    t.boolean "approved", default: false, null: false
    t.boolean "native", default: false, null: false
    t.string "redirect_uris", default: [], array: true
    t.integer "agency_id"
    t.text "failure_to_proof_url"
    t.integer "ial"
    t.boolean "piv_cac", default: false, null: false
    t.boolean "piv_cac_scoped_by_email", default: false, null: false
    t.boolean "pkce", default: false, null: false
    t.string "push_notification_url"
    t.jsonb "help_text", default: {"sign_in" => {}, "sign_up" => {}, "forgot_password" => {}}
    t.boolean "allow_prompt_login", default: false, null: false
    t.boolean "signed_response_message_requested", default: false, null: false
    t.string "remote_logo_key"
    t.date "launch_date"
    t.string "iaa"
    t.date "iaa_start_date"
    t.date "iaa_end_date"
    t.string "app_id"
    t.integer "default_aal"
    t.string "certs"
    t.boolean "email_nameid_format_allowed", default: false, null: false
    t.boolean "use_legacy_name_id_behavior", default: false, null: false
    t.boolean "irs_attempts_api_enabled", default: false, null: false
    t.boolean "in_person_proofing_enabled", default: false, null: false
    t.string "post_idv_follow_up_url"
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
  end

  create_table "sp_upgraded_biometric_profiles", force: :cascade do |t|
    t.datetime "upgraded_at", null: false
    t.bigint "user_id", null: false
    t.string "idv_level", null: false
    t.string "issuer", null: false
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index ["issuer", "upgraded_at"], name: "index_sp_upgraded_biometric_profiles_on_issuer_and_upgraded_at"
    t.index ["user_id"], name: "index_sp_upgraded_biometric_profiles_on_user_id"
  end

  create_table "sp_upgraded_facial_match_profiles", force: :cascade do |t|
    t.datetime "upgraded_at"
    t.bigint "user_id"
    t.string "idv_level"
    t.string "issuer"
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
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

  create_table "stl_unload_log", id: false, force: :cascade do |t|
    t.integer "userid"
    t.integer "query"
    t.integer "pid"
    t.string "path"
    t.datetime "start_time", precision: nil
    t.datetime "end_time", precision: nil
    t.bigint "line_count"
    t.bigint "transfer_size"
    t.string "file_format"
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

  create_table "users", force: :cascade do |t|
    t.datetime "reset_password_sent_at"
    t.datetime "confirmed_at"
    t.integer "second_factor_attempts_count", default: 0, null: false
    t.string "uuid", null: false
    t.datetime "second_factor_locked_at"
    t.datetime "phone_confirmed_at"
    t.datetime "direct_otp_sent_at"
    t.string "unique_session_id"
    t.integer "otp_delivery_preference", default: 0, null: false
    t.datetime "remember_device_revoked_at"
    t.string "email_language"
    t.datetime "accepted_terms_at"
    t.datetime "suspended_at"
    t.datetime "reinstated_at"
    t.datetime "password_compromised_checked_at"
    t.datetime "piv_cac_recommended_dismissed_at"
    t.datetime "second_mfa_reminder_dismissed_at"
    t.datetime "sign_in_new_device_at"
    t.datetime "webauthn_platform_recommended_dismissed_at"
    t.datetime "created_at"
    t.datetime "updated_at"
  end
end
