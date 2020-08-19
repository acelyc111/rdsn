// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <dsn/utility/utils.h>
#include <dsn/c/api_utilities.h>
#include <dsn/metrics.h>
#include <dsn/utility/process_utils.h>

#include "builtin_counters.h"

namespace dsn {

METRIC_DEFINE_gauge(server,
                    memused_virt_mb,
                    metric_unit::kMegaBytes,
                    "virtual memory usages in MB");

METRIC_DEFINE_gauge(server,
                    memused_res_mb,
                    metric_unit::kMegaBytes,
                    "physical memory usages in MB");

builtin_counters::builtin_counters()
{
    _memused_res = METRIC_memused_res_mb.instantiate(get_metric_entity_server());
    _memused_virt = METRIC_memused_virt_mb.instantiate(get_metric_entity_server());
}

builtin_counters::~builtin_counters() {}

void builtin_counters::update_counters()
{
    double vm_usage;
    double resident_set;
    utils::process_mem_usage(vm_usage, resident_set);
    uint64_t memused_virt = (uint64_t)vm_usage / 1024;
    uint64_t memused_res = (uint64_t)resident_set / 1024;
    _memused_virt->set(memused_virt);
    _memused_res->set(memused_res);
    ddebug("memused_virt = %" PRIu64 " MB, memused_res = %" PRIu64 "MB", memused_virt, memused_res);
}
} // namespace dsn
