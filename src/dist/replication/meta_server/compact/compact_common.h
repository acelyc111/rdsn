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

#include <dsn/cpp/json_helper.h>

namespace dsn {
namespace replication {

class compact_record_json : public compact_record
{
public:
    explicit compact_record_json() {}
    explicit compact_record_json(const compact_record &o) : compact_record(o) {}

    DEFINE_JSON_SERIALIZATION(id, start_time, end_time, app_ids)
};

class compact_policy_json : public compact_policy
{
public:
    explicit compact_policy_json() {}
    explicit compact_policy_json(const compact_policy &o) : compact_policy(o) {}
    explicit compact_policy_json(compact_policy &&o) : compact_policy(o) {}

    void enable_isset()
    {
        __isset.policy_name = true;
        __isset.enable = true;
        __isset.start_time = true;
        __isset.interval_seconds = true;
        __isset.app_ids = true;
        __isset.opts = true;
    }

    DEFINE_JSON_SERIALIZATION(policy_name, enable, start_time, interval_seconds, app_ids, opts)
};

} // namespace replication
} // namespace dsn
