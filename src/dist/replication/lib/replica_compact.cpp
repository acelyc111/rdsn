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

#include <dsn/dist/replication/replication_app_base.h>

namespace dsn {
namespace replication {

void replica::do_policy_compact(compact_status::type cs,
                                compact_context_ptr context_ptr,
                                const std::map<std::string, std::string> &opts,
                                compact_response &response)
{
    if (cs == compact_status::COMPACT_STATUS_COMPACTING) {
        // do nothing
        ddebug_f("{}: compact is on going, compact_status = {}",
                 context_ptr->name,
                 _compact_status_VALUES_TO_NAMES.at(cs));
        response.err = dsn::ERR_BUSY;
        response.is_finished = false;
    } else if (cs == compact_status::COMPACT_STATUS_INVALID) {
        // execute compact task async
        ddebug_f("{}: start check_and_compact", context_ptr->name);
        context_ptr->start_compact();
        tasking::enqueue(
            LPC_MANUAL_COMPACT,
            &_tracker,
            [this, context_ptr, opts]() {
                check_and_compact(opts);
                context_ptr->finish_compact();
        });
        response.err = dsn::ERR_BUSY;
        response.is_finished = false;
    } else if (cs == compact_status::COMPACT_STATUS_COMPACTED) {
        // finished
        response.err = dsn::ERR_OK;
        if (status() == partition_status::PS_SECONDARY) {
            ddebug_f("{}: compact completed",
                     context_ptr->name);
            response.is_finished = true;
        } else {
            if (context_ptr->secondary_status.size() ==
                _primary_states.membership.max_replica_count - 1) {
                ddebug_f("{}: primary and secondaries compact completed",
                         context_ptr->name);
                response.is_finished = true;
            } else {
                ddebug_f("{}: primary compact completed but secondaries not",
                         context_ptr->name);
                response.is_finished = false;
            }
        }
    } else {
        // bad case
        dfatal_f("{}: unhandled case, compact_status = {}",
                 context_ptr->name,
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
        compact_context_ptr context_ptr = nullptr;
        auto iter = _compact_contexts.find(policy_name);
        if (iter != _compact_contexts.end()) {
            context_ptr = iter->second;
        } else {
            auto r = _compact_contexts.insert(std::make_pair(policy_name, new_context));
            dassert(r.second, "");
            context_ptr = r.first->second;
        }

        dassert(context_ptr != nullptr, "");
        compact_status::type cs = context_ptr->status();

        // obsoleted compact exist
        if (context_ptr->request.id < req_id) {
            ddebug_f("{}: obsoleted compact exist, old compact id = {}, compact_status = {}",
                     new_context->name,
                     context_ptr->request.id,
                     _compact_status_VALUES_TO_NAMES.at(cs));
            _compact_contexts.erase(policy_name);
            on_policy_compact(request, response);
            return;
        }

        // outdated compact request
        if (context_ptr->request.id > req_id) {
            // req_id is outdated
            derror_f("{}: outdated compact request, current compact id = {}, compact_status = {}",
                     new_context->name,
                     context_ptr->request.id,
                     _compact_status_VALUES_TO_NAMES.at(cs));
            response.err = dsn::ERR_VERSION_OUTDATED;
            response.is_finished = false;
            return;
        }

        // request on primary
        if (status() == partition_status::PS_PRIMARY) {
            send_compact_request_to_secondary(request, context_ptr);
        }

        do_policy_compact(cs, context_ptr, request.opts, response);
    } else {
        derror_f("{}: invalid state for compaction, partition_status = {}",
                 new_context->name,
                 enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
        response.is_finished = false;
    }
}

void replica::send_compact_request_to_secondary(const compact_request &request,
                                                compact_context_ptr context_ptr)
{
    for (const auto &secondary : _primary_states.membership.secondaries) {
        rpc::call(secondary,
                  RPC_POLICY_COMPACT,
                  request,
                  &_tracker,
                  [this, context_ptr, secondary](dsn::error_code err,
                                                     compact_response &&resp) {
                      if (err == dsn::ERR_OK && resp.is_finished) {
                          context_ptr->secondary_status[secondary] = true;
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
        uint64_t finish = static_cast<uint64_t>(_app->last_compact_finish_time().count());
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
            state << "unknown";
        } else {
            state << last_time_used_ms << " ms";
        }
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
