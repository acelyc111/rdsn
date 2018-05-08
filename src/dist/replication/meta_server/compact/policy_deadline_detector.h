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

#include "comapct_policy_executor.h"

namespace dsn {
namespace replication {

class compact_service;

class compact_policy_scheduler {
public:
    compact_policy_scheduler(compact_service *svc, compact_policy&& policy)
            : _compact_service(svc), _executor(svc, this), _policy(std::move(policy)) {}
    ~compact_policy_scheduler() { _tracker.cancel_outstanding_tasks(); }

    // continue to execute an unfinished compact task if needed,
    // check whether a compact task is executable for every issue_new_op_interval
    // start a compact task when it's executable
    void start();
//        void schedule();

    void on_finish(const compact_record &record);

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
    void set_policy(const compact_policy &policy);
    compact_policy get_policy();

private:
    static const int32_t history_count_to_keep = 7;
    static constexpr std::chrono::milliseconds rs_retry_delay = 10000_ms;
    static constexpr std::chrono::milliseconds retry_new_compact_delay = 300000_ms;

    friend class compact_service;
    compact_service *_compact_service;
    comapct_policy_executor _executor;
    dsn::task_tracker _tracker;

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

}   // namespace replication
}   // namespace dsn
