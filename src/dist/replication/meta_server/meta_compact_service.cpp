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

#include "meta_compact_service.h"

#include <dsn/utility/chrono_literals.h>
#include <dsn/dist/fmt_logging.h>
#include "dist/replication/meta_server/meta_service.h"
#include "dist/replication/meta_server/server_state.h"

namespace dsn {
namespace replication {

void compact_policy_context::start_compact_app(int32_t app_id)
{
    if (skip_compact_app(app_id)) {
        ddebug_f("skip to compact app({})", app_id);
        return;
    }

    auto iter = _progress.app_unfinish_partition_count.find(app_id);
    dassert_f(iter != _progress.app_unfinish_partition_count.end(),
              "{}: can't find app({}) in unfinished apps",
              _record_sig.c_str(),
              app_id);

    ddebug_f("start to compact app({}), partition_count={}",
             app_id, iter->second);
    for (int32_t i = 0; i < iter->second; ++i) {
        start_compact_partition(gpid(app_id, i));
    }
}

bool compact_policy_context::skip_compact_app(int32_t app_id)
{
    bool app_available = false;
    {
        zauto_read_lock l;
        _compact_service->get_state()->lock_read(l);
        const std::shared_ptr<app_state> &app = _compact_service->get_state()->get_app(app_id);
        if (app != nullptr &&
            app->status == app_status::AS_AVAILABLE) {
            app_available = true;
        }
    }

    // if app is dropped when app start to compact, we just skip compact this app this time
    if (!app_available) {
        dwarn_f("{}: can't compact app({}), perhaps it has been removed, treat it as compact finished",
                _record_sig.c_str(),
                app_id);
        auto iter = _progress.app_unfinish_partition_count.find(app_id);
        dassert_f(iter != _progress.app_unfinish_partition_count.end(),
                  "{}: can't find app({}) in app_unfinish_partition_count",
                  _record_sig.c_str(),
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

void compact_policy_context::start_compact_partition(gpid pid)
{
    ddebug_f("start to compact gpid({})",
             pid);
    dsn::rpc_address primary;
    // check the partition status
    {
        zauto_read_lock l;
        _compact_service->get_state()->lock_read(l);
        const std::shared_ptr<app_state> &app = _compact_service->get_state()->get_app(pid.get_app_id());
        if (app == nullptr ||
            app->status != app_status::AS_AVAILABLE) {
            dwarn_f("{}: app({}) is not available, just ignore it",
                    _record_sig.c_str(),
                    pid.get_app_id());
            _progress.skipped_app[pid.get_app_id()] = true;
            finish_compact_partition(pid,
                                     true,
                                     dsn::rpc_address());
            return;
        }

        primary = app->partitions[pid.get_partition_index()].primary;
    }

    if (primary.is_invalid()) {
        dwarn_f("{}: gpid({})'s replica is invalid right now, retry this partition later",
                _record_sig.c_str(),
                pid);
        tasking::enqueue(
            LPC_DEFAULT_CALLBACK,
            nullptr,
            [this, pid]() {
                zauto_lock l(_lock);
                start_compact_partition(pid);
            },
            0,
            _compact_service->get_option().reconfiguration_retry_delay);
    } else {
        start_compact_primary(pid, primary);
    }
}

void compact_policy_context::start_compact_primary(gpid pid,
                                                   const dsn::rpc_address &primary)
{
    compact_request req;
    req.id = _cur_record.id;
    req.pid = pid;
    req.policy_name = _policy.policy_name;
    req.opts = _policy.opts;
    dsn_message_t request = dsn_msg_create_request(RPC_POLICY_COMPACT,
                                                   0,
                                                   pid.thread_hash());
    dsn::marshall(request, req);
    dsn::task_ptr rpc_task =
        rpc::create_rpc_response_task(request,
                                      nullptr,
                                      [this, pid, primary](error_code err,
                                                           compact_response &&response) {
                on_compact_reply(err, std::move(response), pid, primary);
            });
    ddebug_f("{}: send compact_request to {}@{}",
             _record_sig.c_str(),
             pid,
             primary.to_string());
    _compact_service->get_meta_service()->send_request(request, primary, rpc_task);
}

void compact_policy_context::on_compact_reply(error_code err,
                                              compact_response &&response,
                                              gpid pid,
                                              const dsn::rpc_address &primary)
{
    dwarn_f("on_compact_reply, pid({})", pid);
    if (err == dsn::ERR_OK &&
        response.err == dsn::ERR_OK) {
        zauto_lock l(_lock);

        dassert_f(response.policy_name == _policy.policy_name,
                  "policy name({} vs {}) doesn't match, {}@{}",
                  _policy.policy_name.c_str(),
                  response.policy_name.c_str(),
                  pid,
                  primary.to_string());
        dassert_f(response.pid == pid,
                  "{}: compact pid({} vs {}) doesn't match @{}",
                  _policy.policy_name.c_str(),
                  response.pid,
                  pid,
                  primary.to_string());
        dassert_f(response.id <= _cur_record.id,
                  "{}: {}@{} has bigger id({} vs {})",
                  _record_sig.c_str(),
                  pid,
                  primary.to_string(),
                  response.id,
                  _cur_record.id);

        if (response.id < _cur_record.id) {
            dwarn_f("{}: {}@{} got a lower id({} vs {}), ignore it",
                    _record_sig.c_str(),
                    pid,
                    primary.to_string(),
                    response.id,
                    _cur_record.id);
        } else {
            // NOTICE: if a partition is finished, we don't try to resend the command again
            if (finish_compact_partition(pid, response.finish, primary)) {
                return;
            }
        }
    } else {
        dwarn_f("{}: compact got error {}@{}, rpc({}), logic({})",
                _record_sig.c_str(),
                pid,
                primary.to_string(),
                err.to_string(),
                response.err.to_string());
    }

    // start another turn of compact no matter we encounter error or not finished
    tasking::enqueue(
        LPC_DEFAULT_CALLBACK,
        nullptr,
        [this, pid, primary]() {
            start_compact_primary(pid, primary);
        },
        0,
        _compact_service->get_option().request_compact_period);
}

bool compact_policy_context::finish_compact_partition(gpid pid,
                                                      bool finish,
                                                      const dsn::rpc_address &source)
{
    if (_progress.gpid_finish[pid]) {
        dwarn_f("{}: pid({}) has finished, ignore the response from {}",
                _record_sig.c_str(),
                pid,
                source.to_string());
        return true;
    }

    if (!finish) {
        dwarn_f("{}: compaction on {}@{} is not finish",
                _record_sig.c_str(),
                pid,
                source.to_string());
        return false;
    }

    _progress.gpid_finish[pid] = true;
    dwarn_f("{}: compaction on {}@{} has finished",
            _record_sig.c_str(),
            pid,
            source.to_string());

    auto app_unfinish_partition_count = --_progress.app_unfinish_partition_count[pid.get_app_id()];
    ddebug_f("{}: finish compact for gpid({}), {} partitions left on app_id({})",
             _record_sig.c_str(),
             pid,
             app_unfinish_partition_count,
             pid.get_app_id());

    if (app_unfinish_partition_count == 0) {
        finish_compact_app(pid.get_app_id());
    }

    return true;
}

void compact_policy_context::finish_compact_app(int32_t app_id)
{
    ddebug_f("{}: finish compact for app({})", _record_sig.c_str(), app_id);

    if (--_progress.unfinish_apps_count == 0) {
        finish_compact_policy();
    }
}

void compact_policy_context::finish_compact_policy()
{
    ddebug_f("{}: finish compact for policy", _record_sig.c_str());
    _cur_record.end_time = dsn_now_s();

    task_ptr compact_task =
        tasking::create_task(LPC_DEFAULT_CALLBACK,
                             nullptr,
                             [this]() {
            // store compact record into memory
            zauto_lock l(_lock);
            auto iter = _history_records.emplace(_cur_record.id, _cur_record);
            dassert_f(iter.second,
                      "add compact record({}.{}) into history list",
                      _policy.policy_name.c_str(),
                      _cur_record.id);

            _cur_record.start_time = 0;
            _cur_record.end_time = 0;
            issue_new_compact();
        });
    sync_record_to_remote_storage(_cur_record, compact_task, false);
}

void compact_policy_context::init_progress()
{
    _progress.reset();

    zauto_read_lock l;
    _compact_service->get_state()->lock_read(l);

    // NOTICE: the unfinish_apps_count is initialized with the app-set's size
    // even if some apps are not available.
    _progress.unfinish_apps_count = _cur_record.app_ids.size();
    for (const int32_t &app_id : _cur_record.app_ids) {
        const std::shared_ptr<app_state> &app = _compact_service->get_state()->get_app(app_id);
        _progress.skipped_app[app_id] = true;
        if (app == nullptr) {
            dwarn_f("{}: app id({}) is invalid",
                    _policy.policy_name.c_str(),
                    app_id);
        } else if (app->status != app_status::AS_AVAILABLE) {
            dwarn_f("{}: {} is not available, status({})",
                    _policy.policy_name.c_str(),
                    app->get_logname(),
                    enum_to_string(app->status));
        } else {
            _progress.skipped_app[app_id] = false;
            // NOTICE: only available apps have entry in
            // app_unfinish_partition_count & gpid_finish
            _progress.app_unfinish_partition_count[app_id] = app->partition_count;
            for (const auto &pc : app->partitions) {
                _progress.gpid_finish[pc.pid] = false;
            }
        }
    }
}

void compact_policy_context::init_current_record()
{
    _cur_record.id = _cur_record.start_time = static_cast<int64_t>(dsn_now_s());
    _cur_record.app_ids = _policy.app_ids;

    init_progress();
    _record_sig = _policy.policy_name
                   + "@"
                   + boost::lexical_cast<std::string>(_cur_record.id);
}

void compact_policy_context::sync_record_to_remote_storage(const policy_record &record,
                                                           task_ptr sync_task,
                                                           bool create_new_node)
{
    auto callback = [this, record, sync_task, create_new_node](dsn::error_code err) {
        if (dsn::ERR_OK == err ||
            (create_new_node && dsn::ERR_NODE_ALREADY_EXIST == err)) {
            ddebug_f("{}: synced policy_record({}) to remote storage successfully.",
                     _policy.policy_name.c_str(),
                     record.id);
            if (sync_task != nullptr) {
                sync_task->enqueue();
            } else {
                dwarn_f("{}: empty sync_task", _policy.policy_name.c_str());
            }
        } else if (dsn::ERR_TIMEOUT == err) {
            derror_f("{}: sync compact info({}) to remote storage got timeout, retry it later",
                     _policy.policy_name.c_str(),
                     record.id);
            tasking::enqueue(
                LPC_DEFAULT_CALLBACK,
                nullptr,
                [this, record, sync_task, create_new_node]() {
                    zauto_lock l(_lock);
                    sync_record_to_remote_storage(std::move(record),
                                                  std::move(sync_task),
                                                  create_new_node);
                },
                0,
                _compact_service->get_option().meta_retry_delay);
        } else {
            dassert_f(false,
                      "{}: we can't handle this right now, error({})",
                      _record_sig.c_str(),
                      err.to_string());
        }
    };

    std::string record_path = _compact_service->get_record_path(_policy.policy_name, record.id);
    dsn::blob record_data = dsn::json::json_forwarder<policy_record>::encode(record);
    if (create_new_node) {
        if (_history_records.size() > _policy.history_count_to_keep) {
            const policy_record &record = _history_records.begin()->second;
            ddebug_f("{}: start to gc compact record({})",
                     _policy.policy_name.c_str(),
                     record.id);

            tasking::create_task(
                LPC_DEFAULT_CALLBACK,
                nullptr,
                [this, record]() {
                    remove_record_on_remote_storage(record);
                })->enqueue();
        }

        _compact_service->get_meta_service()->get_remote_storage()->create_node(
            record_path,
            LPC_DEFAULT_CALLBACK,
            callback,
            record_data,
            nullptr);
    } else {
        _compact_service->get_meta_service()->get_remote_storage()->set_data(
            record_path,
            record_data,
            LPC_DEFAULT_CALLBACK,
            callback,
            nullptr);
    }
}

void compact_policy_context::continue_current_compact()
{
    for (const int32_t &app_id : _cur_record.app_ids) {
        if (_progress.app_unfinish_partition_count.count(app_id) != 0) {
            start_compact_app(app_id);
        } else {
            finish_compact_app(app_id);
        }
    }
}

bool compact_policy_context::start_in_1hour(int start_time)
{
    dassert(0 <= start_time && start_time < 86400, "");
    int now = ::dsn::utils::sec_of_day();
    return (start_time <= now && now < start_time + 3600) ||
           (start_time > now && now + 86400 - start_time <= 3600);
}

bool compact_policy_context::should_start_compact()
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

void compact_policy_context::retry_issue_new_compact()
{
    tasking::enqueue(
        LPC_DEFAULT_CALLBACK,
        nullptr,
        [this]() {
            zauto_lock l(_lock);
            issue_new_compact();
        },
        0,
        _compact_service->get_option().issue_new_op_interval);
}

void compact_policy_context::issue_new_compact()
{
    // before issue new compact, we check whether the policy is dropped
    if (!_policy.enable) {
        ddebug_f("{}: policy is not enable, try it later",
                 _policy.policy_name.c_str());
        retry_issue_new_compact();
        return;
    }

    if (!should_start_compact()) {
        ddebug_f("{}: compact time is not arrived, try it later",
                 _policy.policy_name.c_str());
        retry_issue_new_compact();
        return;
    }

    init_current_record();

    // if all apps are dropped, we don't issue a new compact
    if (_progress.app_unfinish_partition_count.empty()) {
        dwarn_f("{}: all apps have been dropped, retry it later",
                _record_sig.c_str());
        retry_issue_new_compact();
        return;
    }

    task_ptr compact_task
        = tasking::create_task(
            LPC_DEFAULT_CALLBACK,
            nullptr,
            [this]() {
                zauto_lock l(_lock);
                continue_current_compact();
            });
    sync_record_to_remote_storage(_cur_record, compact_task, true);
}

void compact_policy_context::start()
{
    zauto_lock l(_lock);

    if (_cur_record.start_time == 0) {
        issue_new_compact();
    } else {
        continue_current_compact();
    }
}

void compact_policy_context::add_record(const policy_record &record)
{
    zauto_lock l(_lock);
    if (record.end_time <= 0) {
        ddebug_f("{}: encounter an unfinished policy_record({}), start_time({}), continue it later",
                 _policy.policy_name.c_str(),
                 record.id,
                 record.start_time);
        dassert_f(_cur_record.start_time == 0,
                  "{}: shouldn't have multiple unfinished compact instance in a policy, {} vs {}",
                  _policy.policy_name.c_str(),
                  _cur_record.id,
                  record.id);
        dassert_f(_history_records.empty() || record.id > _history_records.rbegin()->first,
                  "{}: id({}) in history larger than current({})",
                  _policy.policy_name.c_str(),
                  _history_records.rbegin()->first,
                  record.id);
        _cur_record = record;
        init_progress();
        _record_sig = _policy.policy_name
                       + "@"
                       + boost::lexical_cast<std::string>(_cur_record.id);
    } else {
        ddebug_f("{}: add compact history, id({}), start_time({}), end_time({})",
                 _policy.policy_name.c_str(),
                 record.id,
                 record.start_time,
                 record.end_time);
        dassert_f(_cur_record.end_time == 0 || record.id < _cur_record.id,
                  "{}: id({}) in history larger than current({})",
                  _policy.policy_name.c_str(),
                  record.id,
                  _cur_record.id);

        auto result_pair = _history_records.emplace(record.id, record);
        dassert_f(result_pair.second,
                  "{}: conflict compact id({})",
                  _policy.policy_name.c_str(),
                  record.id);
    }
}

std::list<policy_record> compact_policy_context::get_compact_records()
{
    zauto_lock l(_lock);

    std::list<policy_record> records;
    for (const auto &record : _history_records) {
        records.emplace_back(record.second);
    }
    if (_cur_record.start_time > 0) {
        records.emplace_front(_cur_record);
    }

    return records;
}

bool compact_policy_context::is_under_compacting()
{
    zauto_lock l(_lock);
    return  (_cur_record.start_time > 0 &&
             _cur_record.end_time <= 0);
}

void compact_policy_context::set_policy(compact_policy &&policy)
{
    zauto_lock l(_lock);

    _policy = std::move(policy);
}

void compact_policy_context::set_policy(const compact_policy &policy)
{
    zauto_lock l(_lock);

    _policy = policy;
}

compact_policy compact_policy_context::get_policy()
{
    zauto_lock l(_lock);
    return _policy;
}

void compact_policy_context::remove_record_on_remote_storage(const policy_record &record)
{
    ddebug_f("{}: start to gc policy_record: id({}), start_time({}), end_time({})",
         _policy.policy_name.c_str(),
         record.id,
         ::dsn::utils::time_to_date(record.start_time).c_str(),
         ::dsn::utils::time_to_date(record.end_time).c_str());

    auto callback = [this, record](dsn::error_code err) {
        if (err == dsn::ERR_OK ||
            err == dsn::ERR_OBJECT_NOT_FOUND) {
            ddebug_f("{}: remove policy_record on remote storage successfully, record_id({})",
                     _policy.policy_name.c_str(),
                     record.id);

            zauto_lock l(_lock);
            _history_records.erase(record.id);
        } else if (err == dsn::ERR_TIMEOUT) {
            derror_f("{}: remove policy_record on remote storage got timeout, retry it later",
                     _policy.policy_name.c_str());
            tasking::enqueue(
                LPC_DEFAULT_CALLBACK,
                nullptr,
                [this, record]() {
                    remove_record_on_remote_storage(record);
                },
                0,
                _compact_service->get_option().meta_retry_delay);
        } else {
            dassert_f(false,
                      "{}: we can't handle this right now, error({})",
                      _policy.policy_name.c_str(),
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
        nullptr);
}

compact_service::compact_service(meta_service *meta_svc,
                                 const std::string &policy_root,
                                 const policy_factory &factory)
        : _factory(factory),
          _meta_svc(meta_svc),
          _policy_root(policy_root)
{
    _state = _meta_svc->get_server_state();
}

void compact_service::start()
{
    dsn::task_ptr sync_task =
        tasking::create_task(
            LPC_DEFAULT_CALLBACK,
            nullptr,
            [this]() {
                start_sync_policies();
            });
    create_policy_root(sync_task);
}

void compact_service::create_policy_root(dsn::task_ptr sync_task)
{
    dinfo_f("create policy root({}) on remote storage",
            _policy_root.c_str());
    _meta_svc->get_remote_storage()->create_node(
        _policy_root,
        LPC_DEFAULT_CALLBACK,
        [this, sync_task](dsn::error_code err) {
            if (err == dsn::ERR_OK ||
                err == dsn::ERR_NODE_ALREADY_EXIST) {
                ddebug_f("create policy root({}) succeed, with err({})",
                         _policy_root.c_str(),
                         err.to_string());
                sync_task->enqueue();
            } else if (err == dsn::ERR_TIMEOUT) {
                derror_f("create policy root({}) timeout, try it later",
                         _policy_root.c_str());
                dsn::tasking::enqueue(
                    LPC_DEFAULT_CALLBACK,
                    nullptr,
                    std::bind(&compact_service::create_policy_root,
                              this,
                              sync_task),
                    0,
                    _opt.meta_retry_delay);
            } else {
                dassert_f(false, "we can't handle this error({}) right now",
                          err.to_string());
            }
        }
    );
}

void compact_service::start_sync_policies()
{
    ddebug("start to sync policies from remote storage");
    dsn::error_code err = sync_policies_from_remote_storage();
    if (err == dsn::ERR_OK) {
        for (auto &policy_ctx : _policy_cxts) {
            ddebug_f("policy({}) start", policy_ctx.first.c_str());
            policy_ctx.second->start();
        }
    } else if (err == dsn::ERR_TIMEOUT) {
        derror("sync policies got timeout, retry it later");
        dsn::tasking::enqueue(
            LPC_DEFAULT_CALLBACK,
            nullptr,
            std::bind(&compact_service::start_sync_policies,
                      this),
            0,
            _opt.meta_retry_delay);
    } else {
        dassert(false,
                "sync policies from remote storage encounter error({})",
                err.to_string());
    }
}

error_code compact_service::sync_policies_from_remote_storage()
{
    // policy on remote storage:
    //      -- _policy_root/policy_name1/compact_id_1
    //      --                               /compact_id_2
    //                           policy_name2/compact_id_1
    //                                       /compact_id_2
    error_code err = dsn::ERR_OK;
    ::dsn::clientlet tracker(1);

    auto parse_history_records =
        [this, &err, &tracker](const std::string &policy_name) {
            auto add_history_record =
                [this, &err, policy_name](error_code ec,
                                          const dsn::blob &value) {
                if (ec == dsn::ERR_OK) {
                    dinfo_f("sync a policy record string({}) from remote storage",
                            value.data());
                    ::dsn::json::string_tokenizer tokenizer(value);
                    policy_record tcompact_record;
                    tcompact_record.decode_json_state(tokenizer);

                    std::shared_ptr<compact_policy_context> policy_ctx = nullptr;
                    {
                        zauto_lock l(_lock);
                        auto it = _policy_cxts.find(policy_name);
                        dassert_f(it != _policy_cxts.end(), "");
                        policy_ctx = it->second;
                    }
                    policy_ctx->add_record(tcompact_record);
                } else {
                    err = ec;
                    ddebug_f("init policy_record from remote storage failed({})",
                             ec.to_string());
                }
            };

            std::string specified_policy_path = get_policy_path(policy_name);
            _meta_svc->get_remote_storage()->get_children(
                specified_policy_path,
                LPC_DEFAULT_CALLBACK,
                [this, &err, &tracker, policy_name, add_history_record](error_code ec,
                                                                        const std::vector<std::string> &record_ids) {
                    if (ec == dsn::ERR_OK) {
                        if (!record_ids.empty()) {
                            for (const auto &record_id : record_ids) {
                                int64_t id = boost::lexical_cast<int64_t>(record_id);
                                std::string record_path = get_record_path(policy_name, id);
                                ddebug_f("start to acquire record({}.{})",
                                         policy_name.c_str(),
                                         id);
                                _meta_svc->get_remote_storage()->get_data(
                                    record_path,
                                    TASK_CODE_EXEC_INLINED,
                                    std::move(add_history_record),
                                    &tracker);
                            }
                        } else {
                            ddebug_f("policy({}) has not started a compact process",
                                     policy_name.c_str());
                        }
                    } else {
                        err = ec;
                        derror_f("get compact policy({}) record failed({}) from remote storage",
                                 policy_name.c_str(),
                                 ec.to_string());
                    }
                },
                &tracker);
    };

    auto parse_one_policy =
        [this, &err, &tracker, &parse_history_records](const std::string &policy_name) {
            ddebug_f("start to acquire the context of policy({})",
                     policy_name.c_str());
            auto policy_path = get_policy_path(policy_name);
            _meta_svc->get_remote_storage()->get_data(
                policy_path,
                LPC_DEFAULT_CALLBACK,
                [this, &err, &parse_history_records, policy_name](error_code ec,
                                                                  const dsn::blob &value) {
                    if (ec == dsn::ERR_OK) {
                        ::dsn::json::string_tokenizer tokenizer(value);
                        compact_policy tpolicy;
                        tpolicy.decode_json_state(tokenizer);
                        std::shared_ptr<compact_policy_context> policy_ctx = _factory(this);
                        policy_ctx->set_policy(std::move(tpolicy));

                        {
                            zauto_lock l(_lock);
                            _policy_cxts.insert(std::make_pair(policy_name, policy_ctx));
                        }
                        parse_history_records(policy_name);
                    } else {
                        err = ec;
                        derror_f("parse policy({}) failed({})",
                                 policy_name.c_str(),
                                 ec.to_string());
                    }
                },
                &tracker);
        };

    _meta_svc->get_remote_storage()->get_children(
        _policy_root,
        LPC_DEFAULT_CALLBACK,
        [&err, &tracker, &parse_one_policy](error_code ec,
                                            const std::vector<std::string> &policy_names) {
            if (ec == dsn::ERR_OK) {
                for (const auto &policy_name : policy_names) {
                    parse_one_policy(policy_name);
                }
            } else {
                err = ec;
                derror_f("get policy dirs from remote storage failed{}",
                         ec.to_string());
            }
        },
        &tracker);

    dsn_task_tracker_wait_all(tracker.tracker());
    return err;
}

void compact_service::add_policy(add_compact_policy_rpc &add_rpc)
{
    auto &request = add_rpc.request();

    const compact_policy_entry &policy = request.policy;
    std::set<int32_t> app_ids;
    {
        zauto_read_lock l;
        _state->lock_read(l);

        for (auto &app_id : policy.app_ids) {
            const std::shared_ptr<app_state> &app = _state->get_app(app_id);
            if (app == nullptr) {
                derror_f("app_id({}) doesn't exist, can't add it to policy({})",
                         app_id,
                         policy.policy_name.c_str());
                add_rpc.response().hint_message += "invalid app_id(" + std::to_string(app_id) + ")\n";
            } else {
                app_ids.insert(app_id);
            }
        }
    }

    bool valid_policy = false;
    std::shared_ptr<compact_policy_context> policy_ctx = nullptr;
    if (!app_ids.empty()) {
        zauto_lock l(_lock);
        if (!is_valid_policy_name(policy.policy_name)) {
            ddebug_f("policy({}) is already exist", policy.policy_name.c_str());
        } else {
            policy_ctx = _factory(this);
            valid_policy = true;
        }
    }

    if (valid_policy) {
        ddebug_f("add compact policy({})", policy.policy_name.c_str());
        compact_policy tmp;
        tmp.policy_name = policy.policy_name;
        tmp.start_time = policy.start_time;
        tmp.interval_seconds = policy.interval_seconds;
        tmp.app_ids = app_ids;
        tmp.opts = policy.opts;
        policy_ctx->set_policy(tmp);

        do_add_policy(add_rpc, policy_ctx);
    } else {
        add_rpc.response().err = dsn::ERR_INVALID_PARAMETERS;
    }
}

void compact_service::do_add_policy(add_compact_policy_rpc &add_rpc,
                                    std::shared_ptr<compact_policy_context> policy_ctx)
{
    const compact_policy &policy = policy_ctx->get_policy();
    dsn::blob value = json::json_forwarder<compact_policy>::encode(policy);
    _meta_svc->get_remote_storage()->create_node(
        get_policy_path(policy.policy_name),
        LPC_DEFAULT_CALLBACK,
        [this, add_rpc, policy_ctx, policy_name = policy.policy_name](error_code err) {
            if (err == dsn::ERR_OK ||
                err == dsn::ERR_NODE_ALREADY_EXIST) {
                ddebug_f("create compact policy({}) on remote storage succeed",
                         policy_name.c_str());

                add_rpc.response().err = dsn::ERR_OK;

                {
                    zauto_lock l(_lock);
                    _policy_cxts.insert(std::make_pair(policy_name, policy_ctx));
                }

                ddebug_f("policy({}) start", policy_name);
                policy_ctx->start();
            } else if (err == dsn::ERR_TIMEOUT) {
                derror_f("create compact policy on remote storage timeout, retry it later");

                tasking::enqueue(
                    LPC_DEFAULT_CALLBACK,
                    nullptr,
                    std::bind(&compact_service::do_add_policy,
                              this,
                              add_rpc,
                              policy_ctx),
                    0,
                    _opt.meta_retry_delay);
                return;
            } else {
                dassert_f(false,
                          "create compact policy({}) on remote storage occurred error({})",
                          policy_name.c_str(),
                          err.to_string());
            }
        },
        value);
}

void compact_service::modify_policy(modify_compact_policy_rpc &modify_rpc)
{
    auto &request = modify_rpc.request();
    auto &response = modify_rpc.response();
    response.err = dsn::ERR_OK;

    const compact_policy_entry &req_policy = request.policy;
    std::shared_ptr<compact_policy_context> policy_ctx = nullptr;
    {
        zauto_lock (_lock);
        auto iter = _policy_cxts.find(req_policy.policy_name);
        if (iter == _policy_cxts.end()) {
            dwarn_f("policy_name({}) not found",
                    req_policy.policy_name.c_str());
            response.err = dsn::ERR_INVALID_PARAMETERS;
        } else {
            policy_ctx = iter->second;
        }
    }

    if (policy_ctx == nullptr) {
        return;
    }

    compact_policy cur_policy = policy_ctx->get_policy();

    bool have_modify_policy = false;

    // modify app_ids
    if (req_policy.__isset.app_ids) {
        std::set<int32_t> app_ids;
        {
            zauto_read_lock l;
            _state->lock_read(l);

            for (const auto &app_id : req_policy.app_ids) {
                const auto &app = _state->get_app(app_id);
                if (app == nullptr) {
                    dwarn_f("add invalid app_id({}) to policy({}), ignore it",
                            app_id,
                            cur_policy.policy_name.c_str());
                } else {
                    app_ids.insert(app_id);
                    have_modify_policy = true;
                }
            }
        }

        if (!app_ids.empty()) {
            std::stringstream sslog;
            sslog << "set policy(" << cur_policy.policy_name << ")'s app_ids as ("
                  << ::dsn::utils::sequence_container_to_string(app_ids, ',') << ")";
            ddebug_f("{}", sslog.str().c_str());

            cur_policy.app_ids = app_ids;
            have_modify_policy = true;
        }
    }

    // modify enable
    if (req_policy.__isset.enable) {
        if (req_policy.enable) {
            if (cur_policy.enable) {
                ddebug_f("policy({}) has been enabled already", cur_policy.policy_name.c_str());
                response.err = dsn::ERR_OK;
                response.hint_message = std::string("policy has been enabled already");
            } else {
                ddebug_f("policy({}) is marked to enabled", cur_policy.policy_name.c_str());
                cur_policy.enable = true;
                have_modify_policy = true;
            }
        } else {
            if (policy_ctx->is_under_compacting()) {
                ddebug_f("policy({}) is under compacting, not allow to disabled",
                         cur_policy.policy_name.c_str());
                response.err = dsn::ERR_BUSY;
            } else if (cur_policy.enable) {
                ddebug_f("policy({}) is marked to disabled", cur_policy.policy_name.c_str());
                cur_policy.enable = false;
                have_modify_policy = true;
            } else {
                ddebug_f("policy({}) has been disabled already", cur_policy.policy_name.c_str());
                response.err = dsn::ERR_OK;
                response.hint_message = std::string("policy has been disabled already");
            }
        }
    }

    // modify interval_seconds
    if (req_policy.__isset.interval_seconds) {
        if (req_policy.interval_seconds > 0) {
            ddebug_f("policy({}) will change compact interval from {}s to {}s",
                     cur_policy.policy_name.c_str(),
                     cur_policy.interval_seconds,
                     req_policy.interval_seconds);
            cur_policy.interval_seconds = req_policy.interval_seconds;
            have_modify_policy = true;
        } else {
            dwarn_f("ignore policy({}) invalid interval_seconds({})",
                    cur_policy.policy_name.c_str(),
                    req_policy.interval_seconds);
        }
    }

    // modify start_time
    if (req_policy.__isset.start_time) {
        ddebug_f("policy({}) change start_time from {} to {}",
                 cur_policy.policy_name.c_str(),
                 ::dsn::utils::sec_of_day_to_hm(cur_policy.start_time).c_str(),
                 ::dsn::utils::sec_of_day_to_hm(req_policy.start_time).c_str());
        cur_policy.start_time = req_policy.start_time;
        have_modify_policy = true;
    }

    if (req_policy.__isset.opts) {
        ddebug_f("policy({}) change opts from {} to {}",
                 cur_policy.policy_name.c_str(),
                 ::dsn::utils::kv_map_to_string(cur_policy.opts, ',', '=').c_str(),
                 ::dsn::utils::kv_map_to_string(req_policy.opts, ',', '=').c_str());
        cur_policy.opts = req_policy.opts;
        have_modify_policy = true;
    }

    if (have_modify_policy) {
        modify_policy_on_remote_storage(modify_rpc, cur_policy, policy_ctx);
    }
}

void compact_service::modify_policy_on_remote_storage(modify_compact_policy_rpc &modify_rpc,
                                                      const compact_policy &policy,
                                                      std::shared_ptr<compact_policy_context> &policy_ctx)
{
    std::string policy_path = get_policy_path(policy.policy_name);
    dsn::blob value = json::json_forwarder<compact_policy>::encode(policy);
    _meta_svc->get_remote_storage()->set_data(
        policy_path,
        value,
        LPC_DEFAULT_CALLBACK,
        [this, modify_rpc, policy, policy_ctx](error_code err) {
            if (err == dsn::ERR_OK) {
                ddebug_f("modify compact policy({}) to remote storage succeed",
                         policy.policy_name.c_str());

                policy_ctx->set_policy(policy);
                modify_rpc.response().err = dsn::ERR_OK;
            } else if (err == dsn::ERR_TIMEOUT) {
                derror_f("modify compact policy({}) to remote storage failed, retry it later",
                         policy.policy_name.c_str(),
                         _opt.meta_retry_delay.count());

                tasking::enqueue(
                    LPC_DEFAULT_CALLBACK,
                    nullptr,
                    std::bind(&compact_service::modify_policy_on_remote_storage,
                              this,
                              modify_rpc,
                              policy,
                              policy_ctx),
                    0,
                    _opt.meta_retry_delay);
            } else {
                dassert_f(false,
                          "modify compact policy({}) to remote storage occurred error({})",
                          policy.policy_name.c_str(),
                          err.to_string());
            }
        });
}

void compact_service::query_policy(query_compact_policy_rpc &query_rpc)
{
    auto &response = query_rpc.response();
    response.err = dsn::ERR_OK;

    std::set<std::string> policy_names = query_rpc.request().policy_names;
    if (policy_names.empty()) {
        // default all the policy
        zauto_lock l(_lock);
        for (const auto &name_cxt : _policy_cxts) {
            policy_names.emplace(name_cxt.first);
        }
    }

    for (const auto &policy_name : policy_names) {
        std::shared_ptr<compact_policy_context> policy_ctx = nullptr;
        {
            zauto_lock l(_lock);
            auto iter = _policy_cxts.find(policy_name);
            if (iter != _policy_cxts.end()) {
                policy_ctx = iter->second;
            }
        }
        if (policy_ctx == nullptr) {
            if (!response.hint_msg.empty()) {
                response.hint_msg += "\n\t";
            }
            response.hint_msg += std::string("invalid policy_name " + policy_name);
            continue;
        }

        const compact_policy &cur_policy = policy_ctx->get_policy();
        compact_policy_records t_policy;
        t_policy.policy.__set_policy_name(cur_policy.policy_name);
        t_policy.policy.__set_interval_seconds(cur_policy.interval_seconds);
        t_policy.policy.__set_app_ids(cur_policy.app_ids);
        t_policy.policy.__set_start_time(cur_policy.start_time);
        t_policy.policy.__set_enable(cur_policy.enable);
        t_policy.policy.__set_opts(cur_policy.opts);
        const std::list<policy_record> &records =
            policy_ctx->get_compact_records();
        for (const auto &record : records) {
            compact_record t_record;
            t_record.id = record.id;
            t_record.start_time = record.start_time;
            t_record.end_time = record.end_time;
            t_record.app_ids = record.app_ids;
            t_policy.records.emplace_back(t_record);
        }
        response.policy_records.emplace_back(std::move(t_policy));
    }

    if (!response.hint_msg.empty()) {
        response.__isset.hint_msg = true;
    }
}

bool compact_service::is_valid_policy_name(const std::string &policy_name)
{
    return _policy_cxts.find(policy_name) == _policy_cxts.end();
}

std::string compact_service::get_policy_path(const std::string &policy_name)
{
    return _policy_root + "/" + policy_name;
}

std::string compact_service::get_record_path(const std::string &policy_name, int64_t id)
{
    return get_policy_path(policy_name) + "/" + std::to_string(id);
}

}
}
