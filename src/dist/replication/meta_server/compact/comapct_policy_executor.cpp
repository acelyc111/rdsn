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

#include "comapct_policy_executor.h"

#include <boost/lexical_cast.hpp>

#include "dsn/cpp/zlocks.h"
#include <dsn/dist/fmt_logging.h>
#include "meta_compact_service.h"
#include "../server_state.h"

namespace dsn {
namespace replication {

using namespace dsn::service;

void comapct_policy_executor::init(const compact_policy &policy,
                                   int64_t start_time) {
    _policy = policy;
    _cur_record.id = _cur_record.start_time = start_time;
    _cur_record.app_ids = _policy.app_ids;
    _record_sig = _policy.policy_name
                  + "@"
                  + std::to_string(_cur_record.id);
    init_progress();
}

void comapct_policy_executor::execute() {
    for (const int32_t &app_id : _cur_record.app_ids) {
        if (_progress.app_unfinished_partition_count.count(app_id) != 0) {
            start_compact_app(app_id);
        } else {
            finish_compact_app(app_id);
        }
    }
}

bool comapct_policy_executor::on_compacting() {
    return _cur_record.start_time > 0 &&
           _cur_record.end_time <= 0;
}

compact_record comapct_policy_executor::get_current_record() {
    return _cur_record;
}

void comapct_policy_executor::init_progress() {
    zauto_lock l(_lock);

    _progress.reset();
    _progress.unfinished_apps_count = _cur_record.app_ids.size();    // all apps
    for (const int32_t &app_id : _cur_record.app_ids) {
        std::vector<partition_configuration> partitions;
        if (_compact_service->get_service_state()->get_partition_config(app_id, partitions)) {
            // available apps
            _progress.skipped_app[app_id] = false;
            _progress.app_unfinished_partition_count[app_id] = partitions.size();
            for (const auto &pc : partitions) {
                _progress.gpid_finish[pc.pid] = false;
            }
        } else {
            // unavailable apps
            _progress.skipped_app[app_id] = true;
            dwarn_compact_record("app({}) is not available", app_id);
        }
    }
}

void comapct_policy_executor::start_compact_app(int32_t app_id) {
    if (skip_compact_app(app_id)) {
        ddebug_compact_record("skip to compact app({})", app_id);
        return;
    }

    auto iter = _progress.app_unfinished_partition_count.find(app_id);
    dassert_compact_record(iter != _progress.app_unfinished_partition_count.end(),
                           "can't find app({}) in policy",
                           app_id);

    ddebug_compact_record("start to compact app({}), partition_count={}",
                          app_id,
                          iter->second);
    for (int32_t i = 0; i < iter->second; ++i) {
        start_compact_partition(gpid(app_id, i));
    }
}

bool comapct_policy_executor::skip_compact_app(int32_t app_id) {
    if (!_compact_service->get_service_state()->is_app_available(app_id)) {
        dwarn_compact_record("app({}) is not available, just ignore it",
                             app_id);
        auto iter = _progress.app_unfinished_partition_count.find(app_id);
        dassert_compact_record(iter != _progress.app_unfinished_partition_count.end(),
                               "can't find app({}) in policy",
                               app_id);

        _progress.skipped_app[app_id] = true;
        for (int32_t pidx = 0; pidx < iter->second; ++pidx) {
            finish_compact_partition(gpid(app_id, pidx),
                                     true,
                                     dsn::rpc_address());
        }
        return true;
    }

    return false;
}

void comapct_policy_executor::start_compact_partition(gpid pid)
{
    ddebug_compact_record("start to compact gpid({})", pid);

    dsn::rpc_address primary;
    if (!_compact_service->get_service_state()->get_primary(pid, primary)) {
        dwarn_compact_record("app({}) is not available, just ignore it",
                             pid.get_app_id());

        _progress.skipped_app[pid.get_app_id()] = true;
        finish_compact_partition(pid,
                                 true,
                                 dsn::rpc_address());
        return;
    }

    if (primary.is_invalid()) {
        dwarn_compact_record("gpid({})'s replica is invalid now, retry this partition later",
                             pid);
        tasking::enqueue(
            LPC_DEFAULT_CALLBACK,
            &_tracker,
            [this, pid]() {
                start_compact_partition(pid);
            },
            0,
            update_configuration_delay);
    } else {
        start_compact_primary(pid, primary);
    }
}

void comapct_policy_executor::start_compact_primary(gpid pid,
                                                    const dsn::rpc_address &primary) {
    compact_request req;
    req.id = _cur_record.id;
    req.pid = pid;
    req.policy_name = _policy.policy_name;
    req.opts = _policy.opts;
    dsn_message_t request = dsn_msg_create_request(RPC_POLICY_COMPACT,
                                                   0,
                                                   pid.thread_hash());
    dsn::marshall(request, req);
    rpc::call(primary,
              request,
              &_tracker,
              [this, pid, primary](error_code err,
                     compact_response &&response) {
                  on_compact_reply(err, std::move(response), pid, primary);
              });
    ddebug_compact_record("send compact_request to {}@{}",
                          pid,
                          primary.to_string());
}

void comapct_policy_executor::on_compact_reply(error_code err,
                                              compact_response &&response,
                                              gpid pid,
                                              const dsn::rpc_address &primary) {
    dwarn_compact_record("on_compact_reply, pid({})", pid);
    if (err == dsn::ERR_OK &&
        response.err == dsn::ERR_OK) {
        dassert_compact_record(response.policy_name == _policy.policy_name,
                               "policy name({}) doesn't match, {}@{}",
                               response.policy_name.c_str(),
                               pid,
                               primary.to_string());
        dassert_compact_record(response.pid == pid,
                               "compact pid({} vs {}) doesn't match @{}",
                               response.pid,
                               pid,
                               primary.to_string());
        dassert_compact_record(response.id <= _cur_record.id,
                               "{}@{} has bigger id({})",
                               pid,
                               primary.to_string(),
                               response.id);

        if (response.id < _cur_record.id) {
            dwarn_compact_record("{}@{} got a lower id({}), ignore it",
                                 pid,
                                 primary.to_string(),
                                 response.id);
        } else if (finish_compact_partition(pid, response.is_finished, primary)) {
            return;
        }
    } else {
        dwarn_compact_record("compact got error {}@{}, rpc({}), logic({})",
                             pid,
                             primary.to_string(),
                             err.to_string(),
                             response.err.to_string());
    }

    // start another turn of compact no matter we encounter error or not finished
    tasking::enqueue(
        LPC_DEFAULT_CALLBACK,
        &_tracker,
        [this, pid, primary]() {
            start_compact_primary(pid, primary);
        },
        0,
        request_compact_period);
}

bool comapct_policy_executor::finish_compact_partition(gpid pid,
                                                      bool finish,
                                                      const dsn::rpc_address &source) {
    zauto_lock l(_lock);
    if (_progress.gpid_finish[pid]) {
        dwarn_compact_record("pid({}) has finished, ignore the response from {}",
                             pid,
                             source.to_string());
        return true;
    }

    if (!finish) {
        ddebug_compact_record("compaction on {}@{} is not finish",
                             pid,
                             source.to_string());
        return false;
    }

    _progress.gpid_finish[pid] = true;
    ddebug_compact_record("compaction on {}@{} has finished",
                         pid,
                         source.to_string());

    auto app_unfinish_partition_count = --_progress.app_unfinished_partition_count[pid.get_app_id()];
    ddebug_compact_record("finish compact for gpid({}), {} partitions left on app_id({})",
                          pid,
                          app_unfinish_partition_count,
                          pid.get_app_id());

    if (app_unfinish_partition_count == 0) {
        finish_compact_app(pid.get_app_id());
    }

    return true;
}

void comapct_policy_executor::finish_compact_app(int32_t app_id)
{
    ddebug_compact_record("finish compact for app({})", app_id);

    if (--_progress.unfinished_apps_count == 0) {
        finish_compact_policy();
    }
}

void comapct_policy_executor::finish_compact_policy()
{
    ddebug_compact_record("finish compact for policy");
    _cur_record.end_time = dsn_now_s();

    _policy_scheduler->on_finish(_cur_record);
}

}   // namespace replication
}   // namespace dsn
