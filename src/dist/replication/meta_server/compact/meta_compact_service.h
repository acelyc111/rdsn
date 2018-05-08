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

#include "../meta_data.h"
#include "policy_deadline_detector.h"

namespace dsn {
namespace replication {

class meta_service;
class server_state;

typedef rpc_holder<configuration_add_compact_policy_request,
                   configuration_add_compact_policy_response> add_compact_policy_rpc;
typedef rpc_holder<configuration_modify_compact_policy_request,
                   configuration_modify_compact_policy_response> modify_compact_policy_rpc;
typedef rpc_holder<configuration_query_compact_policy_request,
                   configuration_query_compact_policy_response> query_compact_policy_rpc;

class compact_service {
public:
    compact_service(meta_service *meta_svc,
                    const std::string &policy_meta_root);
    ~compact_service() { _tracker.cancel_outstanding_tasks(); }

    // sync compact policies from remote storage,
    // and start compact task for each policy
    void start();

    void add_policy(add_compact_policy_rpc &add_rpc);
    void query_policy(query_compact_policy_rpc &query_rpc);
    void modify_policy(modify_compact_policy_rpc &modify_rpc);

private:
    void start_sync_policies();
    error_code sync_policies_from_remote_storage();

    void create_policy_root(dsn::task_ptr callback);
    void do_add_policy(add_compact_policy_rpc &add_rpc,
                       std::shared_ptr<compact_policy_scheduler> policy_scheduler);
    void modify_policy_on_remote_storage(modify_compact_policy_rpc &modify_rpc,
                                         const compact_policy &policy,
                                         std::shared_ptr<compact_policy_scheduler> policy_scheduler);

    bool is_valid_policy_name(const std::string &policy_name);

    meta_service *get_meta_service() const { return _meta_svc; }
    server_state *get_service_state() const { return _svc_state; }
    std::string get_policy_path(const std::string &policy_name);
    std::string get_record_path(const std::string &policy_name, int64_t compact_id);

private:
    static constexpr std::chrono::milliseconds rs_retry_delay = 10000_ms;

    friend class comapct_policy_executor;
    friend class compact_policy_scheduler;
    meta_service *_meta_svc;
    server_state *_svc_state;
    dsn::task_tracker _tracker;

    // storage root path on zookeeper
    std::string _policy_root;

    dsn::service::zlock _lock;
    std::map<std::string, std::shared_ptr<compact_policy_scheduler>> _policy_schedulers;
};

}   // namespace replication
}   // namespace dsn
