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
#include "compact_common.h"
#include "dist/replication/meta_server/meta_service.h"
#include "dist/replication/meta_server/server_state.h"

#include <dsn/utility/chrono_literals.h>
#include <dsn/dist/fmt_logging.h>

namespace dsn {
namespace replication {

compact_service::compact_service(meta_service *meta_svc, const std::string &policy_root)
    : _meta_svc(meta_svc), _policy_root(policy_root)
{
    _svc_state = _meta_svc->get_server_state();
}

void compact_service::start()
{
    dsn::task_ptr sync_task =
        tasking::create_task(LPC_DEFAULT_CALLBACK, &_tracker, [this]() { start_sync_policies(); });
    create_policy_root(sync_task);
}

void compact_service::create_policy_root(dsn::task_ptr sync_task)
{
    dinfo_f("create policy root({}) on remote storage", _policy_root.c_str());
    _meta_svc->get_remote_storage()->create_node(
        _policy_root, LPC_DEFAULT_CALLBACK, [this, sync_task](dsn::error_code err) {
            if (err == dsn::ERR_OK || err == dsn::ERR_NODE_ALREADY_EXIST) {
                ddebug_f("create policy root({}) succeed, with err({})",
                         _policy_root.c_str(),
                         err.to_string());
                sync_task->enqueue();
            } else if (err == dsn::ERR_TIMEOUT) {
                derror_f("create policy root({}) timeout, try it later", _policy_root.c_str());
                tasking::enqueue(LPC_DEFAULT_CALLBACK,
                                 &_tracker,
                                 [this, sync_task]() { create_policy_root(sync_task); },
                                 0,
                                 rs_retry_delay);
            } else {
                dassert_f(false, "we can't handle this error({}) right now", err.to_string());
            }
        });
}

void compact_service::start_sync_policies()
{
    ddebug("start to sync policies from remote storage");
    dsn::error_code err = sync_policies_from_remote_storage();
    if (err == dsn::ERR_OK) {
        for (auto &policy_scheduler : _policy_schedulers) {
            ddebug_f("policy({}) start", policy_scheduler.first.c_str());
            policy_scheduler.second->start();
        }
    } else if (err == dsn::ERR_TIMEOUT) {
        derror("sync policies got timeout, retry it later");
        tasking::enqueue(LPC_DEFAULT_CALLBACK,
                         &_tracker,
                         [this]() { start_sync_policies(); },
                         0,
                         rs_retry_delay);
    } else {
        dassert(false, "sync policies from remote storage encounter error({})", err.to_string());
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

    auto parse_history_records = [this, &err](const std::string &policy_name) {
        auto add_history_record = [this, &err, policy_name](error_code ec, const dsn::blob &value) {
            if (ec == dsn::ERR_OK) {
                dinfo_f("sync a policy record string({}) from remote storage", value.data());
                ::dsn::json::string_tokenizer tokenizer(value);
                compact_record_json tcompact_record;
                tcompact_record.decode_json_state(tokenizer);

                std::shared_ptr<policy_deadline_checker> policy_scheduler = nullptr;
                {
                    zauto_lock l(_lock);
                    auto it = _policy_schedulers.find(policy_name);
                    dassert_f(it != _policy_schedulers.end(), "");
                    policy_scheduler = it->second;
                }
                policy_scheduler->add_record(compact_record(tcompact_record));
            } else {
                err = ec;
                ddebug_f("init compact_record_json from remote storage failed({})", ec.to_string());
            }
        };

        std::string specified_policy_path = get_policy_path(policy_name);
        _meta_svc->get_remote_storage()->get_children(
            specified_policy_path,
            LPC_DEFAULT_CALLBACK,
            [this, &err, policy_name, add_history_record](
                error_code ec, const std::vector<std::string> &record_ids) {
                if (ec == dsn::ERR_OK) {
                    if (!record_ids.empty()) {
                        for (const auto &record_id : record_ids) {
                            auto id = boost::lexical_cast<int64_t>(record_id);
                            std::string record_path = get_record_path(policy_name, id);
                            ddebug_f("start to acquire record({}.{})", policy_name.c_str(), id);
                            _meta_svc->get_remote_storage()->get_data(
                                record_path, TASK_CODE_EXEC_INLINED, add_history_record, &_tracker);
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
            &_tracker);
    };

    auto parse_one_policy = [this, &err, &parse_history_records](const std::string &policy_name) {
        ddebug_f("start to acquire the context of policy({})", policy_name.c_str());
        auto policy_path = get_policy_path(policy_name);
        _meta_svc->get_remote_storage()->get_data(
            policy_path,
            LPC_DEFAULT_CALLBACK,
            [this, &err, &parse_history_records, policy_name](error_code ec,
                                                              const dsn::blob &value) {
                if (ec == dsn::ERR_OK) {
                    ::dsn::json::string_tokenizer tokenizer(value);
                    compact_policy_json tpolicy;
                    tpolicy.decode_json_state(tokenizer);
                    tpolicy.enable_isset();
                    auto policy_scheduler =
                        std::make_shared<policy_deadline_checker>(this, compact_policy(tpolicy));

                    {
                        zauto_lock l(_lock);
                        _policy_schedulers.insert(std::make_pair(policy_name, policy_scheduler));
                    }
                    parse_history_records(policy_name);
                } else {
                    err = ec;
                    derror_f("parse policy({}) failed({})", policy_name.c_str(), ec.to_string());
                }
            },
            &_tracker);
    };

    _meta_svc->get_remote_storage()->get_children(
        _policy_root,
        LPC_DEFAULT_CALLBACK,
        [&err, &parse_one_policy](error_code ec, const std::vector<std::string> &policy_names) {
            if (ec == dsn::ERR_OK) {
                for (const auto &policy_name : policy_names) {
                    parse_one_policy(policy_name);
                }
            } else {
                err = ec;
                derror_f("get policy dirs from remote storage failed{}", ec.to_string());
            }
        },
        &_tracker);
    return err;
}

void compact_service::add_policy(add_compact_policy_rpc &add_rpc)
{
    auto &request = add_rpc.request();

    auto &policy = request.policy;
    std::set<int32_t> app_ids;
    for (const auto &app_id : policy.app_ids) {
        if (_svc_state->is_app_available(app_id)) {
            app_ids.insert(app_id);
        } else {
            derror_f("app_id({}) is not available, can't add it to policy({})",
                     app_id,
                     policy.policy_name.c_str());
            add_rpc.response().hint_message += "invalid app_id(" + std::to_string(app_id) + ")\n";
        }
    }

    bool valid_policy = false;
    if (!app_ids.empty()) {
        zauto_lock l(_lock);
        if (!is_valid_policy_name(policy.policy_name)) {
            ddebug_f("policy({}) is already exist", policy.policy_name.c_str());
        } else {
            valid_policy = true;
        }
    }

    if (valid_policy) {
        ddebug_f("add compact policy({})", policy.policy_name.c_str());
        compact_policy tmp(policy);
        tmp.__set_app_ids(app_ids);
        auto policy_scheduler = std::make_shared<policy_deadline_checker>(this, std::move(tmp));
        do_add_policy(add_rpc, policy_scheduler);
    } else {
        add_rpc.response().err = dsn::ERR_INVALID_PARAMETERS;
    }
}

void compact_service::do_add_policy(add_compact_policy_rpc &add_rpc,
                                    std::shared_ptr<policy_deadline_checker> policy_scheduler)
{
    const compact_policy &policy = policy_scheduler->get_policy();
    dsn::blob value =
        json::json_forwarder<compact_policy_json>::encode(compact_policy_json(policy));
    _meta_svc->get_remote_storage()->create_node(
        get_policy_path(policy.policy_name),
        LPC_DEFAULT_CALLBACK,
        [ this, add_rpc = std::move(add_rpc), policy_scheduler, policy_name = policy.policy_name ](
            error_code err) mutable {
            if (err == dsn::ERR_OK || err == dsn::ERR_NODE_ALREADY_EXIST) {
                ddebug_f("create compact policy({}) on remote storage succeed",
                         policy_name.c_str());

                add_rpc.response().err = dsn::ERR_OK;

                {
                    zauto_lock l(_lock);
                    _policy_schedulers.insert(std::make_pair(policy_name, policy_scheduler));
                }

                ddebug_f("policy({}) start", policy_name);
                policy_scheduler->start();
            } else if (err == dsn::ERR_TIMEOUT) {
                derror_f("create compact policy on remote storage timeout, retry it later");

                tasking::enqueue(
                    LPC_DEFAULT_CALLBACK,
                    &_tracker,
                    [ this, add_rpc = std::move(add_rpc), policy_scheduler ]() mutable {
                        do_add_policy(add_rpc, policy_scheduler);
                    },
                    0,
                    rs_retry_delay);
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

    const compact_policy &req_policy = request.policy;
    std::shared_ptr<policy_deadline_checker> policy_scheduler = nullptr;
    {
        zauto_lock(_lock);
        auto iter = _policy_schedulers.find(req_policy.policy_name);
        if (iter == _policy_schedulers.end()) {
            dwarn_f("policy_name({}) not found", req_policy.policy_name.c_str());
            response.err = dsn::ERR_INVALID_PARAMETERS;
        } else {
            policy_scheduler = iter->second;
        }
    }

    if (policy_scheduler == nullptr) {
        return;
    }

    compact_policy cur_policy = policy_scheduler->get_policy();

    bool have_modify_policy = false;

    // modify app_ids
    if (req_policy.__isset.app_ids) {
        std::set<int32_t> app_ids;
        for (const auto &app_id : req_policy.app_ids) {
            if (_svc_state->is_app_available(app_id)) {
                app_ids.insert(app_id);
                have_modify_policy = true;
            } else {
                derror_f("app_id({}) is not available, can't add it to policy({})",
                         app_id,
                         req_policy.policy_name.c_str());
            }
        }

        if (!app_ids.empty()) {
            std::stringstream sslog;
            sslog << "set policy(" << cur_policy.policy_name << ")'s app_ids as ("
                  << ::dsn::utils::sequence_container_to_string(app_ids, ",") << ")";
            ddebug_f("{}", sslog.str().c_str());

            cur_policy.__set_app_ids(app_ids);
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
                cur_policy.__set_enable(true);
                have_modify_policy = true;
            }
        } else {
            if (policy_scheduler->on_compacting()) {
                ddebug_f("policy({}) is under compacting, not allow to disabled",
                         cur_policy.policy_name.c_str());
                response.err = dsn::ERR_BUSY;
            } else if (cur_policy.enable) {
                ddebug_f("policy({}) is marked to disabled", cur_policy.policy_name.c_str());
                cur_policy.__set_enable(false);
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
            cur_policy.__set_interval_seconds(req_policy.interval_seconds);
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
        cur_policy.__set_start_time(req_policy.start_time);
        have_modify_policy = true;
    }

    if (req_policy.__isset.opts) {
        ddebug_f("policy({}) change opts from {} to {}",
                 cur_policy.policy_name.c_str(),
                 ::dsn::utils::kv_map_to_string(cur_policy.opts, ',', '=').c_str(),
                 ::dsn::utils::kv_map_to_string(req_policy.opts, ',', '=').c_str());
        cur_policy.__set_opts(req_policy.opts);
        have_modify_policy = true;
    }

    if (have_modify_policy) {
        modify_policy_on_remote_storage(modify_rpc, cur_policy, policy_scheduler);
    }
}

void compact_service::modify_policy_on_remote_storage(
    modify_compact_policy_rpc &modify_rpc,
    const compact_policy &policy,
    std::shared_ptr<policy_deadline_checker> policy_scheduler)
{
    std::string policy_path = get_policy_path(policy.policy_name);
    dsn::blob value =
        json::json_forwarder<compact_policy_json>::encode(compact_policy_json(policy));
    _meta_svc->get_remote_storage()->set_data(policy_path, value, LPC_DEFAULT_CALLBACK, [
        this,
        modify_rpc = std::move(modify_rpc),
        policy,
        policy_scheduler
    ](error_code err) mutable {
        if (err == dsn::ERR_OK) {
            ddebug_f("modify compact policy({}) to remote storage succeed",
                     policy.policy_name.c_str());

            policy_scheduler->set_policy(policy);
            modify_rpc.response().err = dsn::ERR_OK;
        } else if (err == dsn::ERR_TIMEOUT) {
            derror_f("modify compact policy({}) to remote storage failed, retry it later",
                     policy.policy_name.c_str(),
                     rs_retry_delay.count());

            tasking::enqueue(
                LPC_DEFAULT_CALLBACK,
                &_tracker,
                [ this, modify_rpc = std::move(modify_rpc), policy, policy_scheduler ]() mutable {
                    modify_policy_on_remote_storage(modify_rpc, policy, policy_scheduler);
                },
                0,
                rs_retry_delay);
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
        for (const auto &policy_scheduler : _policy_schedulers) {
            policy_names.emplace(policy_scheduler.first);
        }
    }

    for (const auto &policy_name : policy_names) {
        std::shared_ptr<policy_deadline_checker> policy_scheduler = nullptr;
        {
            zauto_lock l(_lock);
            auto iter = _policy_schedulers.find(policy_name);
            if (iter != _policy_schedulers.end()) {
                policy_scheduler = iter->second;
            }
        }
        if (policy_scheduler == nullptr) {
            if (!response.hint_msg.empty()) {
                response.hint_msg += "\n\t";
            }
            response.hint_msg += std::string("invalid policy_name " + policy_name);
            continue;
        }

        compact_policy_records t_policy;
        t_policy.policy = policy_scheduler->get_policy();
        t_policy.records = policy_scheduler->get_compact_records();
        response.policy_records.emplace_back(std::move(t_policy));
    }

    if (!response.hint_msg.empty()) {
        response.__isset.hint_msg = true;
    }
}

bool compact_service::is_valid_policy_name(const std::string &policy_name)
{
    return _policy_schedulers.find(policy_name) == _policy_schedulers.end();
}

std::string compact_service::get_policy_path(const std::string &policy_name)
{
    return _policy_root + "/" + policy_name;
}

std::string compact_service::get_record_path(const std::string &policy_name, int64_t id)
{
    return get_policy_path(policy_name) + "/" + std::to_string(id);
}

} // namespace replication
} // namespace dsn
