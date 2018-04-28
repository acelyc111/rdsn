#pragma once

#include "meta_data.h"

namespace dsn {
namespace replication {

class meta_service;
class server_state;
class compact_service;

struct policy_record
{
    int64_t id = 0;
    int64_t start_time = 0;
    int64_t end_time = 0;
    std::set<int32_t> app_ids;              // "app_ids" is copied from policy.app_ids when
                                            // a new compact task is generated.
                                            // The policy's app set may be changed,
                                            // but policy_record.app_ids never change.

    DEFINE_JSON_SERIALIZATION(id,
                              start_time,
                              end_time,
                              app_ids)
};

class compact_policy {
public:
    bool enable = true;
    int32_t start_time = 0;
    int32_t interval_seconds = 0;
    std::string policy_name;
    std::set<int32_t> app_ids;
    std::map<std::string, std::string> opts;

    static const int32_t history_count_to_keep = 7;

    DEFINE_JSON_SERIALIZATION(enable,
                              start_time,
                              interval_seconds,
                              policy_name,
                              app_ids,
                              opts)
};

struct compact_progress {
    int32_t unfinish_apps_count = 0;
    std::map<gpid, bool> gpid_finish;
    std::map<app_id, int32_t> app_unfinish_partition_count;
    std::map<app_id, bool> skipped_app;     // if app is dropped when starting a new compact
                                            // or under compacting, we just skip compact this app

    void reset() {
        unfinish_apps_count = 0;
        gpid_finish.clear();
        app_unfinish_partition_count.clear();
        skipped_app.clear();
    }
};

class compact_policy_context {
public:
    explicit compact_policy_context(compact_service *service)
            : _compact_service(service) {}
    ~compact_policy_context() {}

    void set_policy(compact_policy &&p);
    void set_policy(const compact_policy &p);

    void start();

private:
    void issue_new_compact();
    void continue_current_compact();
    void retry_issue_new_compact();
    bool should_start_compact();
    bool start_in_1hour(int start_sec_of_day);
    void init_current_record();
    void init_progress();

    void start_compact_app(int32_t app_id);
    bool skip_compact_app(int32_t app_id);
    void start_compact_partition(gpid pid);
    void start_compact_primary(gpid pid,
                               const dsn::rpc_address &replica);
    void on_compact_reply(error_code err,
                          compact_response &&response,
                          gpid pid,
                          const dsn::rpc_address &replica);
    bool finish_compact_partition(gpid pid,
                                  bool finish,
                                  const dsn::rpc_address &source);
    void finish_compact_app(int32_t app_id);
    void finish_compact_policy();

    void sync_record_to_remote_storage(const policy_record &record,
                                       task_ptr sync_task,
                                       bool create_new_node);
    void remove_record_on_remote_storage(const policy_record &record);

    void add_record(const policy_record &record);
    std::list<policy_record> get_compact_records();
    bool is_under_compacting();
    compact_policy get_policy();

private:
    friend class compact_service;
    compact_service *_compact_service;

    dsn::service::zlock _lock;
    compact_policy _policy;
    policy_record _cur_record;
    std::map<int64_t, policy_record> _history_records;
    compact_progress _progress;

    std::string _record_sig;                // policy_name@record_id, used for logging
};

class compact_service {
public:
    struct compact_service_option {
        std::chrono::milliseconds meta_retry_delay = 10000_ms;
        std::chrono::milliseconds reconfiguration_retry_delay = 15000_ms;
        std::chrono::milliseconds issue_new_op_interval = 300000_ms;
        std::chrono::milliseconds request_compact_period = 10000_ms;
    };
    typedef std::function<std::shared_ptr<compact_policy_context>(compact_service *)> policy_factory;

    explicit compact_service(meta_service *meta_svc,
                             const std::string &policy_meta_root,
                             const policy_factory &factory);

    void start();

    void add_policy(dsn_message_t msg);
    void query_policy(dsn_message_t msg);
    void modify_policy(dsn_message_t msg);

    meta_service *get_meta_service() const { return _meta_svc; }
    server_state *get_state() const { return _state; }
    const compact_service_option &get_option() const { return _opt; }
    std::string get_record_path(const std::string &policy_name, int64_t compact_id);

private:
    void start_sync_policies();
    error_code sync_policies_from_remote_storage();

    void create_policy_root(dsn::task_ptr callback);
    void do_add_policy(dsn_message_t req,
                       std::shared_ptr<compact_policy_context> policy_cxt_ptr,
                       const std::string &hint_msg);
    void modify_policy_on_remote_storage(dsn_message_t req,
                                         const compact_policy &policy,
                                         std::shared_ptr<compact_policy_context> &policy_cxt_ptr);

    std::string get_policy_path(const std::string &policy_name);
    bool is_valid_policy_name(const std::string &policy_name);

private:
    policy_factory _factory;
    meta_service *_meta_svc;
    server_state *_state;

    // storage root path on zookeeper
    std::string _policy_root;

    compact_service_option _opt;

    dsn::service::zlock _lock;
    std::map<std::string, std::shared_ptr<compact_policy_context>> _policy_cxts;
};
}
}
