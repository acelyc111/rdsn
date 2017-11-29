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

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <dsn/service_api_c.h>
#include <dsn/tool-api/perf_counter.h>
#include <dsn/tool-api/perf_counters.h>

namespace dsn {
/*!
@addtogroup perf-counter
@{
*/
class perf_counter_wrapper
{
public:
    perf_counter_wrapper() { _counter = nullptr; }

    perf_counter_wrapper(const perf_counter_wrapper &other) = delete;
    perf_counter_wrapper(perf_counter_wrapper &other) = delete;
    perf_counter_wrapper(perf_counter_wrapper &&other) = delete;
    perf_counter_wrapper &operator=(const perf_counter_wrapper &other) = delete;
    perf_counter_wrapper &operator=(perf_counter_wrapper &other) = delete;
    perf_counter_wrapper &operator=(perf_counter_wrapper &&other) = delete;

    ~perf_counter_wrapper()
    {
        if (nullptr != _counter) {
            dsn::perf_counters::instance().remove_counter(_counter->full_name());
            _counter = nullptr;
        }
    }

    void init_app_counter(const char *section,
                          const char *name,
                          dsn_perf_counter_type_t type,
                          const char *dsptr)
    {
        dsn::perf_counter_ptr c =
            dsn::perf_counters::instance().new_app_counter(section, name, type, dsptr);
        _counter = c.get();
    }

    void init_global_counter(const char *app,
                             const char *section,
                             const char *name,
                             dsn_perf_counter_type_t type,
                             const char *dsptr)
    {
        dsn::perf_counter_ptr c =
            dsn::perf_counters::instance().new_global_counter(app, section, name, type, dsptr);
        _counter = c.get();
    }

    dsn::perf_counter *get() const { return _counter; }
    dsn::perf_counter *operator->() const { return _counter; }
private:
    dsn::perf_counter *_counter;
};
/*@}*/
} // end namespace
