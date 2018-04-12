#include "meta_compact_service.h"

#include <dsn/utility/chrono_literals.h>
#include <dsn/dist/fmt_logging.h>
#include "dist/replication/meta_server/meta_service.h"
#include "dist/replication/meta_server/server_state.h"

namespace dsn {
namespace replication {

void compact_policy_context::start_compact_app_meta(int32_t app_id)
{
    bool app_available = false;
    {
        server_state *state = _compact_service->get_state();
        zauto_read_lock l;
        state->lock_read(l);
        const std::shared_ptr<app_state> &app = state->get_app(app_id);
        if (app != nullptr &&
            app->status == app_status::AS_AVAILABLE) {
            app_available = true;
        }
    }

    // if app is dropped when app start to compact, we just skip compact this app this time
    if (!app_available) {
        dwarn_f("{}: can't compact app({}), perhaps it has benn removed, treat it as compact finished",
                _compact_sig.c_str(),
                app_id);
        auto iter = _progress.app_unfinish_partition_count.find(app_id);
        dassert_f(iter != _progress.app_unfinish_partition_count.end(),
                  "{}: can't find app({}) in app_unfinish_partition_count",
                  _compact_sig.c_str(),
                  app_id);
        _progress.is_app_skipped[app_id] = true;
        for (int32_t pidx = 0; pidx < iter->second; ++pidx) {
            update_partition_progress(gpid(app_id, pidx),
                                      partition_compact_progress::kFinished,
                                      dsn::rpc_address());
        }
        return;
    }

    zauto_lock l(_lock);
    start_compact_app(app_id);
}

void compact_policy_context::start_compact_app(int32_t app_id)
{
    auto iter = _progress.app_unfinish_partition_count.find(app_id);
    dassert_f(iter != _progress.app_unfinish_partition_count.end(),
              "{}: can't find app({}) in unfinished apps",
              _compact_sig.c_str(),
              app_id);
    for (int32_t i = 0; i < iter->second; ++i) {
        start_compact_partition(gpid(app_id, i));
    }
}

void compact_policy_context::finish_compact_app(int32_t app_id)
{
    ddebug_f("{}: finish compact for app({})", _compact_sig.c_str(), app_id);

    // policy compact task is completed
    if (--_progress.unfinished_apps == 0) {
        ddebug_f("{}: finish current compact for all apps", _compact_sig.c_str());
        _cur_record.end_time = dsn_now_s();

        task_ptr compact_task =
            tasking::create_task(LPC_DEFAULT_CALLBACK,
                                 nullptr,
                                 [this]() {
                // store compact record into memory
                zauto_lock l(_lock);
                auto iter = _compact_history.emplace(_cur_record.id, _cur_record);
                dassert_f(iter.second,
                          "{}: compact_id({}) already in the compact_history",
                          _policy.policy_name.c_str(),
                          _cur_record.id);
                _cur_record.start_time = 0;
                _cur_record.end_time = 0;

                // start a new compact task
                ddebug_f("{}: finish an old compact, try to start a new one",
                         _compact_sig.c_str());
                issue_new_compact();
            });
        sync_record_to_remote_storage(_cur_record, compact_task, false);
    }
}

bool compact_policy_context::update_partition_progress(gpid pid,
                                                       int32_t progress,
                                                       const rpc_address &source)
{
    int32_t &recorded_progress = _progress.partition_progress[pid];
    if (recorded_progress == partition_compact_progress::kFinished) {
        dwarn_f("{}: recorded progress({}) is finished for pid({}.{}), ignore the progress "
                "from({}) to set as {}",
                _compact_sig.c_str(),
                recorded_progress,
                pid.get_app_id(),
                pid.get_partition_index(),
                source.to_string(),
                progress);
        return true;
    }

    if (progress < recorded_progress) {             // TODO how to compare?
        dwarn_f("{}: local progress({}) is larger than progress({}) from({}) for pid({}.{}), "
                "perhaps primary changed",
                _compact_sig.c_str(),
                recorded_progress,
                progress,
                source.to_string(),
                pid.get_app_id(),
                pid.get_partition_index());
    }

    recorded_progress = progress;
    dinfo_f("{}: set gpid({}.{})'s progress as ({}) from({})",
            _compact_sig.c_str(),
            pid.get_app_id(),
            pid.get_partition_index(),
            progress,
            source.to_string());
    if (recorded_progress == partition_compact_progress::kFinished) {
        ddebug_f("{}: finish compact for gpid({}.{}) from {}, app_progress({})",
                 _compact_sig.c_str(),
                 pid.get_app_id(),
                 pid.get_partition_index(),
                 source.to_string(),
                 _progress.app_unfinish_partition_count[pid.get_app_id()]);

        // app compact task is completed
        if (--_progress.app_unfinish_partition_count[pid.get_app_id()] == 0) {
            zauto_lock l(_lock);
            finish_compact_app(pid.get_app_id());
        }
    }

    return recorded_progress == partition_compact_progress::kFinished;
}

void compact_policy_context::start_compact_partition(gpid pid)
{
    dsn::rpc_address partition_primary;
    // check the partition status
    {
        zauto_read_lock l;
        _compact_service->get_state()->lock_read(l);
        const app_state *app = _compact_service->get_state()->get_app(pid.get_app_id()).get();

        // skip compact this app
        if (app == nullptr ||
            app->status == app_status::AS_DROPPED) {
            dwarn_f("{}: app({}) is not available, just ignore it",
                    _compact_sig.c_str(),
                    pid.get_app_id());
            _progress.is_app_skipped[pid.get_app_id()] = true;
            update_partition_progress(pid,
                                      partition_compact_progress::kFinished,
                                      dsn::rpc_address());
            return;
        }

        partition_primary = app->partitions[pid.get_partition_index()].primary;
    }

    // then start to compact partition
    {
        if (partition_primary.is_invalid()) {
            dwarn_f("{}: gpid({}.{}) don't have a primary right now, retry it later",
                    _compact_sig.c_str(),
                    pid.get_app_id(),
                    pid.get_partition_index());
            tasking::enqueue(LPC_DEFAULT_CALLBACK,
                             nullptr,
                             [this, pid]() {
                                 zauto_lock l(_lock);
                                 start_compact_partition(pid);
                             },
                             0,
                             _compact_service->get_option().reconfiguration_retry_delay);
        } else {
            // 执行compact
            // TODO compact 需要区分primay secondary吗?
            // _progress.compact_requests[pid] = rpc_callback;
        }
    }
}

void compact_policy_context::initialize_compact_progress()
{
    _progress.reset();

    zauto_read_lock l;
    _compact_service->get_state()->lock_read(l);

    // NOTICE: the unfinished_apps is initialized with the app-set's size
    // even if some apps are not available.
    _progress.unfinished_apps = _cur_record.app_ids.size();
    for (const int32_t &app_id : _cur_record.app_ids) {
        const std::shared_ptr<app_state> &app = _compact_service->get_state()->get_app(app_id);
        _progress.is_app_skipped[app_id] = true;
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
            // NOTICE: only available apps have entry in
            // app_unfinish_partition_count & partition_progress
            _progress.app_unfinish_partition_count[app_id] = app->partition_count;
            for (const auto &pc : app->partitions) {
                _progress.partition_progress[pc.pid] = 0;
            }
            _progress.is_app_skipped[app_id] = false;
        }
    }
}

void compact_policy_context::prepare_current_compact_on_new()
{
    _cur_record.id = _cur_record.start_time = static_cast<int64_t>(dsn_now_s());
    _cur_record.app_ids = _policy.app_ids;
    _cur_record.app_names = _policy.app_names;

    initialize_compact_progress();
    _compact_sig = _policy.policy_name
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
            ddebug_f("{}: synced policy_record({}) to remote storage successfully,"
                     " start real compact work, new_node_create({})",
                     _policy.policy_name.c_str(),
                     record.id,
                     create_new_node ? "true" : "false");
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
                      _compact_sig.c_str(),
                      err.to_string());
        }
    };

    std::string record_path = _compact_service->get_record_path(_policy.policy_name, record.id);
    dsn::blob record_data = dsn::json::json_forwarder<policy_record>::encode(record);
    if (create_new_node) {
        _compact_service->get_meta_service()->get_remote_storage()->create_node(record_path,
                                                                                LPC_DEFAULT_CALLBACK,
                                                                                callback,
                                                                                record_data,
                                                                                nullptr);
    } else {
        _compact_service->get_meta_service()->get_remote_storage()->set_data(record_path,
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
            start_compact_app_meta(app_id);
        } else {
            zauto_lock l(_lock);
            finish_compact_app(app_id);
        }
    }
}

bool compact_policy_context::should_start_compact()
{
    uint64_t now = dsn_now_s() % 86400;
    uint64_t last_compact_start_time = 0;
    if (!_compact_history.empty()) {
        last_compact_start_time = _compact_history.rbegin()->second.start_time;
    }

    if (last_compact_start_time == 0) {
        //  the first time to compact
        return _policy.start_time <= now && now <= _policy.start_time + 3600;
    } else {
        uint64_t next_compact_time =
                last_compact_start_time + _policy.interval_seconds;
        return next_compact_time <= now;
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
    if (_policy.is_disable) {
        ddebug_f("{}: policy is disable, just ignore compact, try it later",
                 _policy.policy_name.c_str());
        retry_issue_new_compact();
        return;
    }

    if (!should_start_compact()) {
        ddebug_f("{}: start issue new compact {}ms later",
                 _policy.policy_name.c_str(),
                 _compact_service->get_option().issue_new_op_interval.count());
        retry_issue_new_compact();
        return;
    }

    prepare_current_compact_on_new();

    // if all apps are dropped, we don't issue a new compact
    if (_progress.app_unfinish_partition_count.empty()) {
        dwarn_f("{}: all apps have been dropped, ignore this compact and retry it later",
                _compact_sig.c_str());
        retry_issue_new_compact();
    } else {
        task_ptr compact_task
            = tasking::create_task(LPC_DEFAULT_CALLBACK,
                                   nullptr,
                                   [this]() {
                                       zauto_lock l(_lock);
                                       continue_current_compact();
                                   });
        sync_record_to_remote_storage(_cur_record, compact_task, true);
    }
}

void compact_policy_context::start()
{
    zauto_lock l(_lock);

    if (_cur_record.start_time == 0) {
        issue_new_compact();
    } else {
        continue_current_compact();
    }

    // TODO counter
    std::string counter_name = _policy.policy_name + ".recent.compact.duration(ms)";
    _counter_recent_compact_duration.init_app_counter(
        "eon.meta.policy",
        counter_name.c_str(),
        COUNTER_TYPE_NUMBER,
        "policy recent compact duration time");

    issue_gc_policy_record_task();
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
        dassert_f(_compact_history.empty() || record.id > _compact_history.rbegin()->first,
                  "{}: id({}) in history larger than current({})",
                  _policy.policy_name.c_str(),
                  _compact_history.rbegin()->first,
                  record.id);
        _cur_record = record;
        initialize_compact_progress();
        _compact_sig = _policy.policy_name
                       + "@"
                       + boost::lexical_cast<std::string>(_cur_record.id);
    } else {
        ddebug_f("{}: add compact history, id({}), start_time({}), endtime({})",
                 _policy.policy_name.c_str(),
                 record.id,
                 record.start_time,
                 record.end_time);
        dassert_f(_cur_record.end_time == 0 || record.id < _cur_record.id,
                  "{}: id({}) in history larger than current({})",
                  _policy.policy_name.c_str(),
                  record.id,
                  _cur_record.id);

        auto result_pair = _compact_history.emplace(record.id, record);
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
    for (auto &it : _compact_history) {
        records.emplace_back(it.second);
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

void compact_policy_context::remove_local_record(int64_t id)
{
    zauto_lock l(_lock);
    _compact_history.erase(id);
}

void compact_policy_context::gc_policy_record(const policy_record &record)
{
    ddebug_f("{}: start to gc policy_record, id({}), start_time({}), end_time({})",
             _policy.policy_name.c_str(),
             record.id,
             ::dsn::utils::time_to_date(record.start_time).c_str(),
             ::dsn::utils::time_to_date(record.end_time).c_str());

    dsn::task_ptr sync_task =
        tasking::create_task(
            LPC_DEFAULT_CALLBACK,
            nullptr,
            [this, record]() {
                remove_local_record(record.id);
                issue_gc_policy_record_task();
            });
    sync_remove_compact_record(record, sync_task);
}

void compact_policy_context::update_compact_duration()
{
    uint64_t last_compact_duration_time = 0;
    if (_cur_record.start_time == 0) {
        if (!_compact_history.empty()) {
            const policy_record &record = _compact_history.rbegin()->second;
            last_compact_duration_time = (record.end_time - record.start_time);
        }
    } else {
        dassert(_cur_record.start_time > 0, "");
        if (_cur_record.end_time == 0) {
            last_compact_duration_time = (dsn_now_s() - _cur_record.start_time);
        } else {
            dassert(_cur_record.end_time > 0, "");
            last_compact_duration_time = (_cur_record.end_time - _cur_record.start_time);
        }
    }
    _counter_recent_compact_duration->set(last_compact_duration_time);
}

void compact_policy_context::issue_gc_policy_record_task()
{
    if (_compact_history.size() > _policy.history_count_to_keep) {
        const policy_record &record = _compact_history.begin()->second;
        ddebug_f("{}: start to gc compact record with id({})",
                 _policy.policy_name.c_str(),
                 record.id);

        tasking::create_task(LPC_DEFAULT_CALLBACK,
                             nullptr,
                             [this, record]() {
                                 gc_policy_record(record);
                             })->enqueue();
    } else {
        dinfo_f("{}: no need to gc compact record, start it later",
                _policy.policy_name.c_str());
        tasking::create_task(LPC_DEFAULT_CALLBACK,
                             nullptr,
                             [this]() {
                                 zauto_lock l(_lock);
                                 issue_gc_policy_record_task();
                             })->enqueue(std::chrono::minutes(3));
    }

    update_compact_duration();
}

void compact_policy_context::sync_remove_compact_record(const policy_record &record,
                                                        dsn::task_ptr sync_task)
{
    auto callback = [this, record, sync_task](dsn::error_code err) {
        if (err == dsn::ERR_OK ||
            err == dsn::ERR_OBJECT_NOT_FOUND) {
            ddebug_f("{}: sync remove policy_record on remote storage successfully, compact_id({})",
                     _policy.policy_name.c_str(),
                     record.id);
            if (sync_task != nullptr) {
                sync_task->enqueue();
            }
        } else if (err == dsn::ERR_TIMEOUT) {
            derror_f("{}: sync remove policy_record on remote storage got timeout, retry it later",
                     _policy.policy_name.c_str());
            tasking::enqueue(
                LPC_DEFAULT_CALLBACK,
                nullptr,
                [this, record, sync_task]() {
                    sync_remove_compact_record(record, sync_task);
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
                                 const std::string &policy_meta_root,
                                 const policy_factory &factory)
        : _factory(factory),
          _meta_svc(meta_svc),
          _policy_meta_root(policy_meta_root)
{
    _state = _meta_svc->get_server_state();

    _opt.meta_retry_delay = 10000_ms;
    _opt.reconfiguration_retry_delay = 15000_ms;
    _opt.issue_new_op_interval = 300000_ms;
}

void compact_service::start_create_policy_meta_root(dsn::task_ptr sync_task)
{
    dinfo_f("create policy meta root({}) on remote storage", _policy_meta_root.c_str());
    _meta_svc->get_remote_storage()->create_node(
        _policy_meta_root,
        LPC_DEFAULT_CALLBACK,
        [this, sync_task](dsn::error_code err) {
            if (err == dsn::ERR_OK ||
                err == dsn::ERR_NODE_ALREADY_EXIST) {
                ddebug_f("create policy meta root({}) succeed, with err({})",
                         _policy_meta_root.c_str(),
                         err.to_string());
                sync_task->enqueue();
            } else if (err == dsn::ERR_TIMEOUT) {
                derror_f("create policy meta root({}) timeout, try it later",
                         _policy_meta_root.c_str());
                dsn::tasking::enqueue(
                    LPC_DEFAULT_CALLBACK,
                    nullptr,
                    std::bind(&compact_service::start_create_policy_meta_root, this, sync_task),
                    0,
                    _opt.meta_retry_delay);
            } else {
                dassert_f(false, "we can't handle this error({}) right now", err.to_string());
            }
        }
    );
}

void compact_service::start_sync_policies()
{
    // TODO: make sync_policies_from_remote_storage function to async
    //       sync-api will lead to deadlock when the threadnum = 1 in default threadpool
    ddebug("compact service start to sync policies from remote storage");
    dsn::error_code err = sync_policies_from_remote_storage();
    if (err == dsn::ERR_OK) {
        for (auto &policy_ctx : _policy_cxts) {
            ddebug_f("policy({}) start to compact", policy_ctx.first.c_str());
            policy_ctx.second->start();
        }
        if (_policy_cxts.empty()) {
            dwarn("can't sync policies from remote storage, user should config some policies");
        }
    } else if (err == dsn::ERR_TIMEOUT) {
        derror("sync policies got timeout, retry it later");
        dsn::tasking::enqueue(
            LPC_DEFAULT_CALLBACK,
            nullptr,
            std::bind(&compact_service::start_sync_policies, this),
            0,
            _opt.meta_retry_delay);
    } else {
        dassert(false,
                "sync policies from remote storage encounter error(%s), we can't handle "
                "this right now");
    }
}

error_code compact_service::sync_policies_from_remote_storage()
{
    // policy on remote storage:
    //      -- _policy_meta_root/policy_name1/compact_id_1
    //      --                               /compact_id_2
    //                           policy_name2/compact_id_1
    //                                       /compact_id_2
    error_code err = dsn::ERR_OK;
    ::dsn::clientlet tracker(1);

    auto parse_history_records = [this, &err, &tracker](const std::string &policy_name) {
        auto after_get_history_record = [this, &err, policy_name](error_code ec, const dsn::blob &value) {
            if (ec == dsn::ERR_OK) {
                dinfo("sync a policy string(%s) from remote storage", value.data());
                ::dsn::json::string_tokenizer tokenizer(value);
                policy_record tcompact_record;
                tcompact_record.decode_json_state(tokenizer);

                compact_policy_context *ptr = nullptr;
                {
                    zauto_lock l(_lock);
                    auto it = _policy_cxts.find(policy_name);
                    dassert (it != _policy_cxts.end(),
                             "before initializing the policy_history, initialize the _policy_cxts first");
                    ptr = it->second.get();
                }
                ptr->add_record(tcompact_record);
            } else {
                err = ec;
                ddebug("init policy_record from remote storage fail, error_code = %s",
                       ec.to_string());
            }
        };

        std::string specified_policy_path = get_policy_path(policy_name);
        _meta_svc->get_remote_storage()->get_children(
            specified_policy_path,
            LPC_DEFAULT_CALLBACK,
            [this, &err, &tracker, policy_name, after_get_history_record](
                error_code ec,
                const std::vector<std::string> &record_ids) {
                if (ec == dsn::ERR_OK) {
                    if (record_ids.size() > 0) {
                        for (const auto &record_id : record_ids) {
                            int64_t id = boost::lexical_cast<int64_t>(record_id);
                            std::string record_path = get_record_path(policy_name, id);
                            ddebug_f("start to acquire policy_record({}) of policy({})",
                                     id,
                                     policy_name.c_str());
                            _meta_svc->get_remote_storage()->get_data(
                                record_path,
                                TASK_CODE_EXEC_INLINED,
                                std::move(after_get_history_record),
                                &tracker);
                        }
                    } else {    // have not compact
                        ddebug("policy has not started a compact process, policy_name = %s",
                               policy_name.c_str());
                    }
                } else {
                    err = ec;
                    derror("get compact record dirs fail from remote storage, policy_path = %s, "
                           "err = %s",
                           get_policy_path(policy_name).c_str(),
                           ec.to_string());
                }
            },
            &tracker);
    };

    auto parse_one_policy =
        [this, &err, &tracker, &parse_history_records](const std::string &policy_name) {
            ddebug("start to acquire the context of policy(%s)", policy_name.c_str());
            auto policy_path = get_policy_path(policy_name);
            _meta_svc->get_remote_storage()->get_data(
                policy_path,
                LPC_DEFAULT_CALLBACK,
                [this, &err, &parse_history_records, policy_path, policy_name](error_code ec,
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
                        derror("parse one policy fail, policy_path = %s, error_code = %s",
                               policy_path.c_str(),
                               ec.to_string());
                    }
                },
                &tracker);
        };

    _meta_svc->get_remote_storage()->get_children(
        _policy_meta_root,
        LPC_DEFAULT_CALLBACK,
        [&err, &tracker, &parse_one_policy](error_code ec,
                                            const std::vector<std::string> &policy_names) {
            if (ec == dsn::ERR_OK) {
                // children's name is name of each policy
                for (const auto &policy_name : policy_names) {
                    parse_one_policy(policy_name);
                }
            } else {
                err = ec;
                derror("get policy dirs from remote storage fail, error_code = %s", ec.to_string());
            }
        },
        &tracker);

    dsn_task_tracker_wait_all(tracker.tracker());
    return err;
}

void compact_service::start()
{
    dsn::task_ptr after_create_policy_meta_root = tasking::create_task(
            LPC_DEFAULT_CALLBACK,
            nullptr,
            [this]() {
                start_sync_policies();
            });
    start_create_policy_meta_root(after_create_policy_meta_root);
}

void compact_service::add_policy(dsn_message_t msg)
{
    configuration_add_compact_policy_request request;
    configuration_add_compact_policy_response response;

    ::dsn::unmarshall(msg, request);
    const compact_policy_entry &policy = request.policy;
    std::set<int32_t> app_ids;
    std::map<int32_t, std::string> app_names;
    {
        zauto_read_lock l;
        _state->lock_read(l);

        for (auto &app_id : policy.app_ids) {
            const std::shared_ptr<app_state> &app = _state->get_app(app_id);
            if (app == nullptr) {
                derror_f("app({}) doesn't exist, can't add it to policy {}",
                         app_id,
                         policy.policy_name.c_str());
                response.hint_message += "invalid app(" + std::to_string(app_id) + ")\n";
            } else {
                app_ids.insert(app_id);
                app_names.insert(std::make_pair(app_id, app->app_name));            // emplace
            }
        }
    }

    bool should_create_new_policy = true;
    std::shared_ptr<compact_policy_context> policy_cxt_ptr = nullptr;

    if (app_ids.size() > 0) {
        {
            zauto_lock l(_lock);
            if (!is_valid_policy_name(policy.policy_name)) {
                ddebug_f("policy({}) is already exist", policy.policy_name.c_str());
                response.err = dsn::ERR_INVALID_PARAMETERS;
                should_create_new_policy = false;
            } else {
                policy_cxt_ptr = _factory(this);
            }
        }

        if (should_create_new_policy) {
            ddebug_f("add compact policy, policy_name = {}", policy.policy_name.c_str());
            compact_policy tmp;
            tmp.policy_name = policy.policy_name;
            tmp.start_time = policy.start_time;
            tmp.interval_seconds = policy.interval_seconds;
            tmp.app_ids = app_ids;
            tmp.app_names = app_names;
            policy_cxt_ptr->set_policy(tmp);
        }
    } else {
        should_create_new_policy = false;
    }

    if (should_create_new_policy) {
        dassert_f(policy_cxt_ptr != nullptr,
                  "invalid compact_policy_context");
        do_add_policy(msg, policy_cxt_ptr, response.hint_message);
    } else {
        response.err = dsn::ERR_INVALID_PARAMETERS;
        _meta_svc->reply_data(msg, response);
        dsn_msg_release_ref(msg);
    }
}

void compact_service::do_add_policy(dsn_message_t req,
                                    std::shared_ptr<compact_policy_context> policy_cxt_ptr,
                                    const std::string &hint_msg)
{
    compact_policy cur_policy = policy_cxt_ptr->get_policy();
    std::string policy_path = get_policy_path(cur_policy.policy_name);
    dsn::blob value = json::json_forwarder<compact_policy>::encode(cur_policy);
    _meta_svc->get_remote_storage()->create_node(
        policy_path,
        LPC_DEFAULT_CALLBACK,
        [ this, req, policy_cxt_ptr, hint_msg, policy_name = cur_policy.policy_name ](error_code err) {
            if (err == dsn::ERR_OK ||
                err == dsn::ERR_NODE_ALREADY_EXIST) {
                ddebug_f("add compact policy({}) succeed", policy_name.c_str());

                configuration_add_compact_policy_response resp;
                resp.hint_message = hint_msg;
                resp.err = dsn::ERR_OK;
                _meta_svc->reply_data(req, resp);
                dsn_msg_release_ref(req);

                {
                    zauto_lock l(_lock);
                    _policy_cxts.insert(std::make_pair(policy_name, policy_cxt_ptr));
                }
                policy_cxt_ptr->start();
            } else if (err == dsn::ERR_TIMEOUT) {
                derror_f("create compact policy on remote storage timeout, retry after {}ms",
                         _opt.meta_retry_delay.count());
                tasking::enqueue(LPC_DEFAULT_CALLBACK,
                                 nullptr,
                                 std::bind(&compact_service::do_add_policy, this, req, policy_cxt_ptr, hint_msg),
                                 0,
                                 _opt.meta_retry_delay);
                return;
            } else {
                dassert_f(false,
                          "we can't handle this when create compact policy({}), err({})",
                          policy_name.c_str(),
                          err.to_string());
            }
        },
        value);
}

void compact_service::do_update_policy_to_remote_storage(dsn_message_t req,
                                                         const compact_policy &policy,
                                                         std::shared_ptr<compact_policy_context> &policy_cxt_ptr)
{
    std::string policy_path = get_policy_path(policy.policy_name);
    dsn::blob value = json::json_forwarder<compact_policy>::encode(policy);
    _meta_svc->get_remote_storage()->set_data(
        policy_path,
        value,
        LPC_DEFAULT_CALLBACK,
        [this, req, policy, policy_cxt_ptr](error_code err) {
            if (err == dsn::ERR_OK) {
                configuration_modify_compact_policy_response resp;
                resp.err = dsn::ERR_OK;
                ddebug_f("update compact policy to remote storage succeed, policy_name = {}",
                         policy.policy_name.c_str());
                policy_cxt_ptr->set_policy(policy);
                _meta_svc->reply_data(req, resp);
                dsn_msg_release_ref(req);
            } else if (err == dsn::ERR_TIMEOUT) {
                derror_f("update compact policy to remote storage failed, policy_name = {}, retry {}ms later",
                         policy.policy_name.c_str(),
                         _opt.meta_retry_delay.count());
                tasking::enqueue(
                    LPC_DEFAULT_CALLBACK,
                    nullptr,
                    std::bind(&compact_service::do_update_policy_to_remote_storage,
                              this,
                              req,
                              policy,
                              policy_cxt_ptr),
                    0,
                    _opt.meta_retry_delay);
            } else {
                dassert_f(false,
                          "we can't handle this when create compact policy, err({})",
                          err.to_string());
            }
        });
}

bool compact_service::is_valid_policy_name(const std::string &policy_name)
{
    return _policy_cxts.find(policy_name) == _policy_cxts.end();
}

void compact_service::query_policy(dsn_message_t msg)
{
    configuration_query_compact_policy_request request;
    configuration_query_compact_policy_response response;
    response.err = dsn::ERR_OK;

    ::dsn::unmarshall(msg, request);

    std::vector<std::string> policy_names = request.policy_names;
    if (policy_names.empty()) {
        // default all the policy
        zauto_lock l(_lock);
        for (const auto &name_cxt : _policy_cxts) {
            policy_names.emplace_back(name_cxt.first);
        }
    }

    for (const auto &policy_name : policy_names) {
        std::shared_ptr<compact_policy_context> policy_cxt_ptr = nullptr;
        {
            zauto_lock l(_lock);
            auto it = _policy_cxts.find(policy_name);
            if (it != _policy_cxts.end()) {
                policy_cxt_ptr = it->second;
            }
        }
        if (policy_cxt_ptr == nullptr) {
            if (!response.hint_msg.empty()) {
                response.hint_msg += "\n\t";
            }
            response.hint_msg += std::string("invalid policy_name " + policy_name);
            continue;
        }

        compact_policy cur_policy = policy_cxt_ptr->get_policy();
        compact_policy_records t_policy;
        t_policy.policy.__set_policy_name(cur_policy.policy_name);
        t_policy.policy.__set_interval_seconds(cur_policy.interval_seconds);
        t_policy.policy.__set_app_ids(cur_policy.app_ids);
        t_policy.policy.__set_start_time(cur_policy.start_time);
        t_policy.policy.__set_is_disable(cur_policy.is_disable);
        const std::list<policy_record> &records =
            policy_cxt_ptr->get_compact_records();
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

    _meta_svc->reply_data(msg, response);
    dsn_msg_release_ref(msg);
}

void compact_service::modify_policy(dsn_message_t msg)
{
    configuration_modify_compact_policy_request request;
    configuration_modify_compact_policy_response response;
    response.err = dsn::ERR_OK;

    ::dsn::unmarshall(msg, request);
    const compact_policy_entry &policy = request.policy;
    std::shared_ptr<compact_policy_context> policy_cxt_ptr;
    {
        zauto_lock (_lock);
        auto iter = _policy_cxts.find(policy.policy_name);
        if (iter == _policy_cxts.end()) {
            dwarn_f("policy_name({}) not found",
                    policy.policy_name.c_str());
            response.err = dsn::ERR_INVALID_PARAMETERS;
            policy_cxt_ptr = nullptr;
        } else {
            policy_cxt_ptr = iter->second;
        }
    }

    if (policy_cxt_ptr == nullptr) {
        _meta_svc->reply_data(msg, response);
        dsn_msg_release_ref(msg);
        return;
    }

    compact_policy cur_policy = policy_cxt_ptr->get_policy();

    bool have_modify_policy = false;
    std::map<int32_t, std::string> app_names;
    if (policy.__isset.app_ids) {
        zauto_read_lock l;
        _state->lock_read(l);

        for (const auto &app_id : policy.app_ids) {
            const auto &app = _state->get_app(app_id);
            if (app == nullptr) {
                dwarn_f("{}: add app({}) to policy failed, because it's invalid, ignore it",
                        cur_policy.policy_name.c_str(),
                        app_id);
            } else {
                app_names.emplace(app_id, app->app_name);
                have_modify_policy = true;
            }
        }
    }

    if (policy.__isset.is_disable) {
        if (policy.is_disable) {
            if (policy_cxt_ptr->is_under_compacting()) {
                ddebug_f("{}: policy is under compacting, not allow to disabled",
                         cur_policy.policy_name.c_str());
                response.err = dsn::ERR_BUSY;                                           // TODO could mark to drop
            } else if (!cur_policy.is_disable) {
                ddebug_f("{}: policy is marked to disabled", cur_policy.policy_name.c_str());
                cur_policy.is_disable = true;
                have_modify_policy = true;
            } else {
                ddebug_f("{}: policy has been disabled already", cur_policy.policy_name.c_str());
                response.err = dsn::ERR_OK;
                response.hint_message = std::string("policy has been disabled already");
            }
        } else {
            if (cur_policy.is_disable) {
                cur_policy.is_disable = false;
                ddebug_f("{}: policy is marked to enabled", cur_policy.policy_name.c_str());
                have_modify_policy = true;
            } else {
                ddebug_f("{}: policy has been enabled already", cur_policy.policy_name.c_str());
                response.err = dsn::ERR_OK;
                response.hint_message = std::string("policy has been enabled already");
            }
        }
    }

    if (policy.__isset.app_ids && !app_names.empty()) {
        cur_policy.app_ids.clear();
        cur_policy.app_names.clear();
        for (const auto &app_name : app_names) {
            ddebug_f("{}: policy include app id {}",
                     cur_policy.policy_name.c_str(),
                     app_name.first);
            cur_policy.app_ids.insert(app_name.first);
            cur_policy.app_names.insert(app_name);
            have_modify_policy = true;
        }
    }

    if (policy.__isset.interval_seconds) {
        if (policy.interval_seconds > 0) {
            ddebug_f("{}: policy will change compact interval from {}s to {}s",
                     cur_policy.policy_name.c_str(),
                     cur_policy.interval_seconds,
                     policy.interval_seconds);
            cur_policy.interval_seconds = policy.interval_seconds;
            have_modify_policy = true;
        } else {
            dwarn_f("{}: invalid interval_seconds({})",
                    cur_policy.policy_name.c_str(),
                    policy.interval_seconds);
        }
    }

    if (policy.__isset.start_time) {
        ddebug_f("{}: policy change start_time from {} to {}",
                 cur_policy.policy_name.c_str(),
                 ::dsn::utils::sec_of_day_to_hm(cur_policy.start_time).c_str(),
                 ::dsn::utils::sec_of_day_to_hm(policy.start_time).c_str());
        cur_policy.start_time = policy.start_time;
        have_modify_policy = true;
    }

    if (have_modify_policy) {
        do_update_policy_to_remote_storage(msg, cur_policy, policy_cxt_ptr);
    } else {
        _meta_svc->reply_data(msg, response);
        dsn_msg_release_ref(msg);
    }
}
    
std::string compact_service::get_policy_path(const std::string &policy_name)
{
    return _policy_meta_root +
            "/" +
            policy_name;
}

std::string compact_service::get_record_path(const std::string &policy_name, int64_t id)
{
    return get_policy_path(policy_name) +
            "/" +
            std::to_string(id);
}

}
}
