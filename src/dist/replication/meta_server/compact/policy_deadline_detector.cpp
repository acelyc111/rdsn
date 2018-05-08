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

#include "policy_deadline_detector.h"
#include <dsn/dist/fmt_logging.h>

#include "compact_common.h"
#include "../meta_service.h"

namespace dsn {
namespace replication {

void compact_policy_scheduler::on_finish(const compact_record &record) {
    task_ptr compact_task =
        tasking::create_task(LPC_DEFAULT_CALLBACK,
                             &_tracker,
                             [this, record = std::move(record)]() {
            // store compact record into memory
            {
                zauto_lock l(_lock);
                auto iter = _history_records.emplace(record.id, record);
                dassert_compact_policy(iter.second,
                                       "add compact record into history list");
            }
            issue_new_compact();
        });
    sync_record_to_remote_storage(record, compact_task, false);
}

void compact_policy_scheduler::sync_record_to_remote_storage(const compact_record &record,
                                                           task_ptr sync_task,
                                                           bool create_new_node)
{
    auto callback = [this, record, sync_task, create_new_node](dsn::error_code err) {
        if (dsn::ERR_OK == err ||
            (create_new_node && dsn::ERR_NODE_ALREADY_EXIST == err)) {
            ddebug_compact_policy("sync compact_record to remote storage successfully");
            if (sync_task != nullptr) {
                sync_task->enqueue();
            } else {
                dwarn_compact_policy("empty sync_task");
            }
        } else if (dsn::ERR_TIMEOUT == err) {
            derror_compact_policy("sync compact_record to remote storage got timeout, retry it later");
            tasking::enqueue(
                LPC_DEFAULT_CALLBACK,
                &_tracker,
                [this, record, sync_task, create_new_node]() {
                    sync_record_to_remote_storage(std::move(record),
                                                  std::move(sync_task),
                                                  create_new_node);
                },
                0,
                rs_retry_delay);
        } else {
            dassert_compact_policy(false,
                                   "we can't handle this right now, error({})",
                                   err.to_string());
        }
    };

    std::string record_path = _compact_service->get_record_path(_policy.policy_name, record.id);
    ddebug_compact_policy("record_path={}",
                          record_path);
    dsn::blob record_data = dsn::json::json_forwarder<compact_record_json>::encode(compact_record_json(record));
    if (create_new_node) {
        if (_history_records.size() > history_count_to_keep) {
            const compact_record &record = _history_records.begin()->second;
            ddebug_compact_policy("start to gc compact record({})",
                                  record.id);

            tasking::create_task(
                LPC_DEFAULT_CALLBACK,
                &_tracker,
                [this, record]() {
                    remove_record_from_remote_storage(record);
                })->enqueue();
        }

        _compact_service->get_meta_service()->get_remote_storage()->create_node(
            record_path,
            LPC_DEFAULT_CALLBACK,
            callback,
            record_data,
            &_tracker);
    } else {
        _compact_service->get_meta_service()->get_remote_storage()->set_data(
            record_path,
            record_data,
            LPC_DEFAULT_CALLBACK,
            callback,
            &_tracker);
    }
}

bool compact_policy_scheduler::start_in_1hour(int start_time)
{
    dassert(0 <= start_time && start_time < 86400, "");
    int now = ::dsn::utils::sec_of_day();
    return (start_time <= now && now < start_time + 3600) ||
           (start_time > now && now + 86400 - start_time <= 3600);
}

bool compact_policy_scheduler::should_start_compact()
{
    uint64_t last_compact_start_time = 0;
    if (!_history_records.empty()) {
        last_compact_start_time = _history_records.rbegin()->second.start_time;
    }

    if (last_compact_start_time == 0) {
        //  the first time to compact
        return start_in_1hour(_policy.start_time);
    } else {
        uint64_t next_compact_time =
            last_compact_start_time + _policy.interval_seconds;
        return next_compact_time <= dsn_now_s();
    }
}

void compact_policy_scheduler::retry_issue_new_compact()
{
    tasking::enqueue(
        LPC_DEFAULT_CALLBACK,
        &_tracker,
        [this]() {
            issue_new_compact();
        },
        0,
        retry_new_compact_delay);
}

void compact_policy_scheduler::issue_new_compact() {
    zauto_lock l(_lock);

    // before issue new compact, we check whether the policy is dropped
    if (!_policy.enable) {
        ddebug_compact_policy("policy is not enable, try it later");
        retry_issue_new_compact();
        return;
    }

    if (!should_start_compact()) {
        ddebug_compact_policy("compact time is not arrived, try it later");
        retry_issue_new_compact();
        return;
    }

    _executor.init(_policy, static_cast<int64_t>(dsn_now_s()));

    task_ptr compact_task
        = tasking::create_task(
            LPC_DEFAULT_CALLBACK,
            &_tracker,
            [this]() {
                _executor.execute();
            });
    sync_record_to_remote_storage(_executor.get_current_record(), compact_task, true);
}

void compact_policy_scheduler::start() {
    if (_executor.on_compacting()) {
        _executor.execute();
    } else {
        issue_new_compact();
    }
}

void compact_policy_scheduler::add_record(const compact_record &record) {
    zauto_lock l(_lock);

    const compact_record &cur_record = _executor.get_current_record();
    if (record.end_time <= 0) {
        ddebug_compact_policy("encounter an unfinished compact_record({}), start_time({}), continue it later",
                              record.id,
                              record.start_time);
        dassert_compact_policy(cur_record.start_time == 0,
                               "shouldn't have multiple unfinished compact instance in a policy, {} vs {}",
                               cur_record.id,
                               record.id);
        dassert_compact_policy(_history_records.empty() || record.id > _history_records.rbegin()->first,
                               "id({}) in history larger than current({})",
                               _history_records.rbegin()->first,
                               record.id);
        _executor.init(_policy, record.start_time);
    } else {
        ddebug_compact_policy("add compact history, id({}), start_time({}), end_time({})",
                              record.id,
                              record.start_time,
                              record.end_time);
        dassert_compact_policy(cur_record.end_time == 0 || record.id < cur_record.id,
                               "id({}) in history larger than current({})",
                               record.id,
                               cur_record.id);

        auto result_pair = _history_records.emplace(record.id, record);
        dassert_compact_policy(result_pair.second,
                               "conflict compact id({})",
                               record.id);
    }
}

std::vector<compact_record> compact_policy_scheduler::get_compact_records() {
    zauto_lock l(_lock);

    std::vector<compact_record> records;
    if (on_compacting()) {
        records.emplace_back(_executor.get_current_record());
    }
    for (const auto &record : _history_records) {
        records.emplace_back(record.second);
    }

    return records;
}

bool compact_policy_scheduler::on_compacting() {
    return _executor.on_compacting();
}

void compact_policy_scheduler::set_policy(const compact_policy &policy)
{
    zauto_lock l(_lock);

    _policy = policy;
}

compact_policy compact_policy_scheduler::get_policy()
{
    zauto_lock l(_lock);
    return _policy;
}

void compact_policy_scheduler::remove_record_from_remote_storage(const compact_record &record)
{
    ddebug_compact_policy("start to gc compact_record: id({}), start_time({}), end_time({})",
                          record.id,
                          ::dsn::utils::time_s_to_date_time(record.start_time).c_str(),
                          ::dsn::utils::time_s_to_date_time(record.end_time).c_str());

    auto callback = [this, record](dsn::error_code err) {
        if (err == dsn::ERR_OK ||
            err == dsn::ERR_OBJECT_NOT_FOUND) {
            ddebug_compact_policy("remove compact_record on remote storage successfully, record_id({})",
                     record.id);

            zauto_lock l(_lock);
            _history_records.erase(record.id);
        } else if (err == dsn::ERR_TIMEOUT) {
            derror_compact_policy("remove compact_record on remote storage got timeout, retry it later");
            tasking::enqueue(
                LPC_DEFAULT_CALLBACK,
                &_tracker,
                [this, record]() {
                    remove_record_from_remote_storage(record);
                },
                0,
                rs_retry_delay);
        } else {
            dassert_compact_policy(false,
                      "we can't handle this right now, error({})",
                      err.to_string());
        }
    };

    std::string compact_record_path =
        _compact_service->get_record_path(_policy.policy_name, record.id);
    _compact_service->get_meta_service()->get_remote_storage()->delete_node(
        compact_record_path,
        true,
        LPC_DEFAULT_CALLBACK,
        callback,
        &_tracker);
}

}   // namespace replication
}   // namespace dsn
