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

#include <cstdint>
#include <map>
#include <dsn/tool-api/gpid.h>
#include <dsn/dist/replication/replication_other_types.h>

namespace dsn {
namespace replication {

class compact_service;
class policy_deadline_checker;

struct compact_progress
{
    uint32_t unfinished_apps_count = 0;
    std::map<gpid, bool> gpid_finish;
    std::map<app_id, int32_t> app_unfinished_partition_count;
    std::map<app_id, bool> skipped_app; // if app is dropped when starting a new compact
                                        // or on compacting, we just skip compact this app

    void reset()
    {
        unfinished_apps_count = 0;
        gpid_finish.clear();
        app_unfinished_partition_count.clear();
        skipped_app.clear();
    }
};

class compact_policy_worker
{
public:
    compact_policy_worker(compact_service *svc, policy_deadline_checker *policy_scheduler)
        : _compact_service(svc), _policy_scheduler(policy_scheduler)
    {
    }
    ~compact_policy_worker() { _tracker.cancel_outstanding_tasks(); }

    void init(const compact_policy &policy, int64_t start_time);
    void execute();
    bool on_compacting();
    compact_record get_current_record();

private:
    void init_progress();

    void start_compact_app(int32_t app_id);
    bool skip_compact_app(int32_t app_id);
    void start_compact_partition(gpid pid);
    void start_compact_primary(gpid pid, const dsn::rpc_address &replica);

    void on_compact_reply(error_code err,
                          compact_response &&response,
                          gpid pid,
                          const dsn::rpc_address &replica);

    bool finish_compact_partition(gpid pid, bool finish, const dsn::rpc_address &source);
    void finish_compact_app(int32_t app_id);
    void finish_compact_policy();

private:
    compact_service *_compact_service;
    policy_deadline_checker *_policy_scheduler;
    compact_policy _policy;
    dsn::task_tracker _tracker;

    dsn::service::zlock _lock;
    compact_record _cur_record;
    compact_progress _progress;
    std::string _record_sig; // policy_name@record_id, used for logging
};

// Macros for writing log message prefixed by _record_sig.
#define dinfo_compact_record(...) dinfo_f("[{}] {}", _record_sig, fmt::format(__VA_ARGS__));
#define ddebug_compact_record(...) ddebug_f("[{}] {}", _record_sig, fmt::format(__VA_ARGS__));
#define dwarn_compact_record(...) dwarn_f("[{}] {}", _record_sig, fmt::format(__VA_ARGS__));
#define derror_compact_record(...) derror_f("[{}] {}", _record_sig, fmt::format(__VA_ARGS__));
#define dfatal_compact_record(...) dfatal_f("[{}] {}", _record_sig, fmt::format(__VA_ARGS__));
#define dassert_compact_record(x, ...)                                                             \
    dassert_f(x, "[{}] {}", _record_sig, fmt::format(__VA_ARGS__));

} // namespace replication
} // namespace dsn
