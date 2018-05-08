/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#pragma once

#include "meta_data.h"

namespace dsn {
namespace replication {

class meta_service;
class server_state;
class compact_service;

typedef rpc_holder<configuration_add_compact_policy_request,
                   configuration_add_compact_policy_response> add_compact_policy_rpc;
typedef rpc_holder<configuration_modify_compact_policy_request,
                   configuration_modify_compact_policy_response> modify_compact_policy_rpc;
typedef rpc_holder<configuration_query_compact_policy_request,
                   configuration_query_compact_policy_response> query_compact_policy_rpc;

class compact_record_json: public compact_record {
public:
    explicit compact_record_json() {}
    compact_record_json(const compact_record &o): compact_record(o) {}

    DEFINE_JSON_SERIALIZATION(id,
                              start_time,
                              end_time,
                              app_ids)
};

class compact_policy_json: public compact_policy {
public:
    explicit compact_policy_json() {}
    compact_policy_json(const compact_policy &o): compact_policy(o) {}
    compact_policy_json(compact_policy&&o): compact_policy(o) {}

    void enable_isset() {
        __isset.policy_name = true;
        __isset.enable = true;
        __isset.start_time = true;
        __isset.interval_seconds = true;
        __isset.app_ids = true;
        __isset.opts = true;
    }

    DEFINE_JSON_SERIALIZATION(policy_name,
                              enable,
                              start_time,
                              interval_seconds,
                              app_ids,
                              opts)
};

struct compact_progress {
    int32_t unfinish_apps_count = 0;
    std::map<gpid, bool> gpid_finish;
    std::map<app_id, int32_t> app_unfinish_partition_count;
    std::map<app_id, bool> skipped_app;     // if app is dropped when starting a new compact
                                            // or on compacting, we just skip compact this app

    void reset() {
        unfinish_apps_count = 0;
        gpid_finish.clear();
        app_unfinish_partition_count.clear();
        skipped_app.clear();
    }
};

class compact_policy_scheduler;
class comapct_policy_executor {
public:
    explicit comapct_policy_executor(compact_service *svc,
                                     compact_policy_scheduler *policy_scheduler)
            : _compact_service(svc),
              _policy_scheduler(policy_scheduler) {}
    void init(const compact_policy &policy,
              int64_t start_time);
    void execute();
    bool on_compacting();
    compact_record get_current_record();

private:
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

private:
    compact_service *_compact_service;
    compact_policy_scheduler *_policy_scheduler;
    compact_policy _policy;

    dsn::service::zlock _lock;
    compact_record _cur_record;
    compact_progress _progress;
    std::string _record_sig;                // policy_name@record_id, used for logging
};

// Macros for writing log message prefixed by _record_sig.
#define dinfo_compact_record(...) dinfo_f("[{}] {}", _record_sig, fmt::format(__VA_ARGS__));
#define ddebug_compact_record(...) ddebug_f("[{}] {}", _record_sig, fmt::format(__VA_ARGS__));
#define dwarn_compact_record(...) dwarn_f("[{}] {}", _record_sig, fmt::format(__VA_ARGS__));
#define derror_compact_record(...) derror_f("[{}] {}", _record_sig, fmt::format(__VA_ARGS__));
#define dfatal_compact_record(...) dfatal_f("[{}] {}", _record_sig, fmt::format(__VA_ARGS__));
#define dassert_compact_record(x, ...) dassert_f(x, "[{}] {}", _record_sig, fmt::format(__VA_ARGS__));

class compact_policy_scheduler {
public:
    explicit compact_policy_scheduler(compact_service *svc)
            : _compact_service(svc), _executor(svc, this) {}

    void set_policy(compact_policy &&p);
    void set_policy(const compact_policy &p);

    // continue to execute an unfinished compact task if needed,
    // check whether a compact task is executable for every issue_new_op_interval
    // start a compact task when it's executable
    void start();

    void finish_one_execution(const compact_record &record);

private:
    void issue_new_compact();
    void retry_issue_new_compact();
    bool should_start_compact();
    bool start_in_1hour(int start_sec_of_day);

    void sync_record_to_remote_storage(const compact_record &record,
                                       task_ptr sync_task,
                                       bool create_new_node);
    void remove_record_from_remote_storage(const compact_record &record);

    void add_record(const compact_record &record);
    std::vector<compact_record> get_compact_records();
    bool on_compacting();
    compact_policy get_policy();

private:
    static const int32_t history_count_to_keep = 7;

    friend class compact_service;
    compact_service *_compact_service;
    comapct_policy_executor _executor;

    dsn::service::zlock _lock;
    compact_policy _policy;
    std::map<int64_t, compact_record> _history_records;
};

#define dinfo_compact_policy(...) dinfo_f("[{}] {}", _policy.policy_name, fmt::format(__VA_ARGS__));
#define ddebug_compact_policy(...) ddebug_f("[{}] {}", _policy.policy_name, fmt::format(__VA_ARGS__));
#define dwarn_compact_policy(...) dwarn_f("[{}] {}", _policy.policy_name, fmt::format(__VA_ARGS__));
#define derror_compact_policy(...) derror_f("[{}] {}", _policy.policy_name, fmt::format(__VA_ARGS__));
#define dfatal_compact_policy(...) dfatal_f("[{}] {}", _policy.policy_name, fmt::format(__VA_ARGS__));
#define dassert_compact_policy(x, ...) dassert_f(x, "[{}] {}", _policy.policy_name, fmt::format(__VA_ARGS__));

class compact_service {
public:
    struct compact_service_option {
        std::chrono::milliseconds meta_retry_delay = 10000_ms;
        std::chrono::milliseconds update_configuration_delay = 15000_ms;
        std::chrono::milliseconds retry_new_compact_delay = 300000_ms;
        std::chrono::milliseconds request_compact_period = 10000_ms;
    };

    compact_service(meta_service *meta_svc,
                    const std::string &policy_meta_root);

    // sync compact policies from remote storage,
    // and start compact task for each policy
    void start();

    void add_policy(add_compact_policy_rpc &add_rpc);
    void query_policy(query_compact_policy_rpc &query_rpc);
    void modify_policy(modify_compact_policy_rpc &modify_rpc);

    meta_service *get_meta_service() const { return _meta_svc; }
    server_state *get_state() const { return _state; }
    const compact_service_option &get_option() const { return _opt; }
    std::string get_record_path(const std::string &policy_name, int64_t compact_id);

private:
    void start_sync_policies();
    error_code sync_policies_from_remote_storage();

    void create_policy_root(dsn::task_ptr callback);
    void do_add_policy(add_compact_policy_rpc &add_rpc,
                       std::shared_ptr<compact_policy_scheduler> policy_scheduler);
    void modify_policy_on_remote_storage(modify_compact_policy_rpc &modify_rpc,
                                         const compact_policy &policy,
                                         std::shared_ptr<compact_policy_scheduler> policy_scheduler);

    std::string get_policy_path(const std::string &policy_name);
    bool is_valid_policy_name(const std::string &policy_name);

private:
    meta_service *_meta_svc;
    server_state *_state;

    // storage root path on zookeeper
    std::string _policy_root;

    compact_service_option _opt;

    dsn::service::zlock _lock;
    std::map<std::string, std::shared_ptr<compact_policy_scheduler>> _policy_schedulers;
};
}
}
