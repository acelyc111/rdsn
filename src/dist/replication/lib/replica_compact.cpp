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

#include "replica.h"

#include <dsn/dist/fmt_logging.h>
#include <dsn/dist/replication/replication_app_base.h>

namespace dsn {
namespace replication {

void replica::do_policy_compact(compact_status::type cs,
                                compact_context_ptr compact_context,
                                const std::map<std::string, std::string> &opts,
                                compact_response &response)
{
    if (cs == compact_status::COMPACT_STATUS_COMPACTING) {
        // do nothing
        ddebug_f("{}: compact is on going, compact_status = {}",
                 compact_context->name,
                 _compact_status_VALUES_TO_NAMES.at(cs));
        response.err = dsn::ERR_BUSY;
        response.finish = false;
    } else if (cs == compact_status::COMPACT_STATUS_INVALID) {
        // execute compact task async
        ddebug_f("{}: start check_and_compact", compact_context->name);
        compact_context->start_compact();
        tasking::enqueue(
            LPC_MANUAL_COMPACT,
            this,
            [this, compact_context, opts]() {
                check_and_compact(opts);
                compact_context->finish_compact();
        });
        response.err = dsn::ERR_BUSY;
        response.finish = false;
    } else if (cs == compact_status::COMPACT_STATUS_COMPACTED) {
        // finished
        response.err = dsn::ERR_OK;
        if (status() == partition_status::PS_SECONDARY) {
            ddebug_f("{}: compact completed",
                     compact_context->name);
            response.finish = true;
        } else {
            if (compact_context->secondary_status.size() ==
                _primary_states.membership.max_replica_count - 1) {
                ddebug_f("{}: primary and secondaries compact completed",
                         compact_context->name);
                response.finish = true;
            } else {
                ddebug_f("{}: primary compact completed but secondaries not",
                         compact_context->name);
                response.finish = false;
            }
        }
    } else {
        // bad case
        dfatal_f("{}: unhandled case, compact_status = {}",
                 compact_context->name,
                 _compact_status_VALUES_TO_NAMES.at(cs));
    }
}

void replica::on_policy_compact(const compact_request &request,
                                compact_response &response)
{
    const std::string &policy_name = request.policy_name;
    auto req_id = request.id;
    compact_context_ptr new_context(new compact_context(request));

    if (status() == partition_status::type::PS_PRIMARY ||
        status() == partition_status::type::PS_SECONDARY) {
        compact_context_ptr compact_context = nullptr;
        auto iter = _compact_contexts.find(policy_name);
        if (iter != _compact_contexts.end()) {
            compact_context = iter->second;
        } else {
            auto r = _compact_contexts.insert(std::make_pair(policy_name, new_context));
            dassert(r.second, "");
            compact_context = r.first->second;
        }

        dassert(compact_context != nullptr, "");
        compact_status::type cs = compact_context->status();

        // obsoleted compact exist
        if (compact_context->request.id < req_id) {
            ddebug_f("{}: obsoleted compact exist, old compact id = {}, compact_status = {}",
                     new_context->name,
                     compact_context->request.id,
                     _compact_status_VALUES_TO_NAMES.at(cs));
            _compact_contexts.erase(policy_name);
            on_policy_compact(request, response);
            return;
        }

        // outdated compact request
        if (compact_context->request.id > req_id) {
            // req_id is outdated
            derror_f("{}: outdated compact request, current compact id = {}, compact_status = {}",
                     new_context->name,
                     compact_context->request.id,
                     _compact_status_VALUES_TO_NAMES.at(cs));
            response.err = dsn::ERR_VERSION_OUTDATED;
            response.finish = false;
            return;
        }

        // request on primary
        if (status() == partition_status::PS_PRIMARY) {
            send_compact_request_to_secondary(request, compact_context);
        }

        do_policy_compact(cs, compact_context, request.opts, response);
    } else {
        derror_f("{}: invalid state for compaction, partition_status = {}",
                 new_context->name,
                 enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
        response.finish = false;
    }
}

void replica::send_compact_request_to_secondary(const compact_request &request,
                                                compact_context_ptr compact_context)
{
    for (const auto &secondary : _primary_states.membership.secondaries) {
        rpc::call(secondary,
                  RPC_POLICY_COMPACT,
                  request,
                  nullptr,
                  [this, compact_context, secondary](dsn::error_code err,
                                                     compact_response &&resp) {
                      if (err == dsn::ERR_OK && resp.finish) {
                          compact_context->secondary_status[secondary] = true;
                      }
                  });
    }
}

void replica::check_and_compact(const std::map<std::string, std::string> &opts)
{
    if (could_start_manual_compact()) {
        manual_compact(opts);
    }
}

bool replica::could_start_manual_compact()
{
    uint64_t not_start = 0;
    uint64_t now = dsn_now_ms();
    if (_options->manual_compact_min_interval_seconds > 0 &&
        (_manual_compact_last_finish_time_ms.load() == 0 ||
         now - _manual_compact_last_finish_time_ms.load() >
             (uint64_t)_options->manual_compact_min_interval_seconds * 1000)) {
        return _manual_compact_enqueue_time_ms.compare_exchange_strong(not_start, now);
    } else {
        return false;
    }
}

void replica::manual_compact(const std::map<std::string, std::string> &opts)
{
    if (_app != nullptr) {
        ddebug_replica("start to execute manual compaction");
        uint64_t start = dsn_now_ms();
        _manual_compact_start_time_ms.store(start);
        _app->manual_compact(opts);
        uint64_t finish = _app->last_compact_finish_time().count();
        ddebug_replica("finish to execute manual compaction, time_used = {}ms",
                       finish - start);
        _manual_compact_last_finish_time_ms.store(finish);
        _manual_compact_last_time_used_ms.store(finish - start);
        _manual_compact_start_time_ms.store(0);
        _manual_compact_enqueue_time_ms.store(0);
    }
}

std::string replica::get_compact_state()
{
    uint64_t enqueue_time_ms = _manual_compact_enqueue_time_ms.load();
    uint64_t start_time_ms = _manual_compact_start_time_ms.load();
    uint64_t last_finish_time_ms = _manual_compact_last_finish_time_ms.load();
    uint64_t last_time_used_ms = _manual_compact_last_time_used_ms.load();
    std::stringstream state;
    if (last_finish_time_ms > 0) {
        char str[24];
        utils::time_ms_to_string(last_finish_time_ms, str);
        state << "last finish at [" << str << "], last used ";
        if (last_time_used_ms == 0) {
            state << "-";
        } else {
            state << last_time_used_ms;
        }
        state << " ms";
    } else {
        state << "last finish at [-]";
    }
    if (enqueue_time_ms > 0) {
        char str[24];
        utils::time_ms_to_string(enqueue_time_ms, str);
        state << ", recent enqueue at [" << str << "]";
    }
    if (start_time_ms > 0) {
        char str[24];
        utils::time_ms_to_string(start_time_ms, str);
        state << ", recent start at [" << str << "]";
    }
    return state.str();
}

}   // namespace replication
}   // namespace dsn
