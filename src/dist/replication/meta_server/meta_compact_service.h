#pragma once

#include "meta_data.h"

namespace dsn {
namespace replication {

class meta_service;
class server_state;
class compact_service;

enum class policy_status: int32_t {
    ALIVE = 1,
    DELETING = 2
};

typedef std::underlying_type<policy_status>::type policy_status_under;

struct policy_record
{
    int32_t info_status = (policy_status_under)policy_status::ALIVE;
    int64_t id = 0;
    int64_t start_time = 0;
    int64_t end_time = 0;

    // "app_ids" is copied from policy.app_ids when
    // a new compact is generated. The policy's
    // app set may be changed, but policy_record.app_ids
    // never change.
    std::set<int32_t> app_ids;
    std::map<int32_t, std::string> app_names;

    DEFINE_JSON_SERIALIZATION(id,
                              start_time,
                              end_time,
                              app_ids,
                              app_names,
                              info_status)
};

class compact_policy : public policy_info {          // TODO policy_info is needed？
public:
    bool is_disable = false;
    int32_t start_time = 0;
    int32_t history_count_to_keep = 7;
    int32_t interval_seconds = 0;
    std::set<int32_t> app_ids;
    std::map<int32_t, std::string> app_names;

    DEFINE_JSON_SERIALIZATION(policy_name,
                              app_ids,
                              app_names,
                              interval_seconds,
                              is_disable,
                              start_time)
};

struct compact_progress {
    int32_t max_replica_count = 0;
    int32_t unfinished_apps = 0;                                    // unfinished apps count
    std::map<gpid, std::set<dsn::rpc_address>> partition_progress;  // gpid => <rpc_address => progress>
    std::map<gpid, dsn::task_ptr> compact_requests;                 // gpid => compact task
    std::map<app_id, int32_t> app_unfinish_partition_count;         // app_id => unfinish partition count
    // if app is dropped when starting a new compact or under compacting, we just skip compact this app
    std::map<app_id, bool> is_app_skipped;                          // true when app is invalid

    void reset() {
        unfinished_apps = 0;
        partition_progress.clear();
        compact_requests.clear();
        app_unfinish_partition_count.clear();
        is_app_skipped.clear();
    }
};

// policy level (multi apps)
class compact_policy_context {
public:
    explicit compact_policy_context(compact_service *service)
            : _compact_service(service) {
    }

    ~compact_policy_context() {}

    void set_policy(compact_policy &&p);
    void set_policy(const compact_policy &p);

    void start();

private:
    void issue_new_compact();
    void retry_issue_new_compact();
    bool should_start_compact();
    bool time_in_1hour(int start_sec_of_day);
    void prepare_current_compact_on_new();
    void initialize_compact_progress();
    void continue_current_compact();
    void start_compact_app_meta(int32_t app_id);
    void start_compact_app(int32_t app_id);
    void start_compact_partition(gpid pid);
    bool valid_replicas(const std::vector<dsn::rpc_address> &replicas);
    void on_compact_reply(error_code err,
                          compact_response &&response,
                          gpid pid,
                          const dsn::rpc_address &replica,
                          bool is_primary);
    void issue_gc_policy_record_task();
    void update_compact_duration();
    void gc_policy_record(const policy_record &record);
    void remove_local_record(int64_t id);
    bool update_partition_progress(gpid pid,
                                   bool finish,
                                   const dsn::rpc_address &source);
    void finish_compact_app(int32_t app_id);

    void write_compact_record(const policy_record &record,
                              dsn::task_ptr write_callback);
    void sync_record_to_remote_storage(const policy_record &record,
                                      task_ptr sync_task,
                                      bool create_new_node);
    void sync_remove_compact_record(const policy_record &record,
                                    dsn::task_ptr sync_task);
    void add_record(const policy_record &record);
    std::list<policy_record> get_compact_records();
    bool is_under_compacting();
    compact_policy get_policy();

private:
    friend class compact_service;
    compact_service *_compact_service;

    // lock the data-structure below
    dsn::service::zlock _lock;

    // policy related
    compact_policy _policy;

    // compact related
    policy_record _cur_record;
    // id --> policy_record
    std::map<int64_t, policy_record> _compact_history;        // key:时间,由小到大
    compact_progress _progress;
    std::string _compact_sig; // policy_name@id, used when print compact related log

    perf_counter_wrapper _counter_recent_compact_duration;
};

// cluster level
class compact_service {
public:
    struct compact_service_option
    {
        std::chrono::milliseconds meta_retry_delay;
        std::chrono::milliseconds reconfiguration_retry_delay;
        std::chrono::milliseconds issue_new_op_interval;
        std::chrono::milliseconds request_compact_period;
    };

    typedef std::function<std::shared_ptr<compact_policy_context>(compact_service *)> policy_factory;
    explicit compact_service(meta_service *meta_svc,
                             const std::string &policy_meta_root,
                             const policy_factory &factory);

    void start();

    void add_policy(dsn_message_t msg);
    void do_add_policy(dsn_message_t req,
                       std::shared_ptr<compact_policy_context> policy_cxt_ptr,
                       const std::string &hint_msg);
    void do_update_policy_to_remote_storage(dsn_message_t req,
                                            const compact_policy &policy,
                                            std::shared_ptr<compact_policy_context> &policy_cxt_ptr);
    void query_policy(dsn_message_t msg);
    void modify_policy(dsn_message_t msg);

    meta_service *get_meta_service() const { return _meta_svc; }
    server_state *get_state() const { return _state; }
    const compact_service_option &get_option() const { return _opt; }
    std::string get_record_path(const std::string &policy_name, int64_t compact_id);

private:
    void start_create_policy_meta_root(dsn::task_ptr callback);
    void start_sync_policies();
    error_code sync_policies_from_remote_storage();
    std::string get_policy_path(const std::string &policy_name);
    bool is_valid_policy_name(const std::string &policy_name);

private:
    policy_factory _factory;
    meta_service *_meta_svc;
    server_state *_state;

    // _lock is only used to lock _policy_cxts
    dsn::service::zlock _lock;
    // policy_name -> compact_policy_context
    std::map<std::string, std::shared_ptr<compact_policy_context>> _policy_cxts;  // poicy name => compact_policy_context   // TODO 可以改名

    // the root of policy metas, stored on remote_storage(zookeeper)
    std::string _policy_meta_root;

    compact_service_option _opt;
};
}
}
