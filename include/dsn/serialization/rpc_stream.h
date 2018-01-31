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

#include <dsn/utility/utils.h>
#include <dsn/utility/mm.h>
#include <dsn/tool-api/auto_codes.h>
#include <dsn/tool-api/rpc_message.h>

namespace dsn {
/*!
@addtogroup rpc-msg
@{
*/
class rpc_read_stream : public binary_reader
{
public:
    rpc_read_stream(message_ex *msg) { set_read_msg(msg); }
    rpc_read_stream() : msg(nullptr) {}
    virtual ~rpc_read_stream()
    {
        if (msg) {
            msg->read_commit((size_t)(total_size() - get_remaining_size()));
        }
    }

    void set_read_msg(message_ex *m)
    {
        msg = m;

        void *ptr;
        size_t size;
        bool r = msg->read_next(&ptr, &size);
        dassert(r, "read msg must have one segment of buffer ready");

        blob bb((const char *)ptr, 0, (int)size);
        init(bb);
    }

private:
    message_ex *msg;
};

class rpc_write_stream : public binary_writer
{
public:
    // for response
    rpc_write_stream(message_ex *m) : msg(m)
    {
        _last_write_next_committed = true;
        _last_write_next_total_size = 0;
    }

    // for request
    rpc_write_stream(task_code code,
                     int timeout_ms = 0,
                     int thread_hash = 0,
                     uint64_t partition_hash = 0)
        : msg(message_ex::create_request(code, timeout_ms, thread_hash, partition_hash))
    {
        _last_write_next_committed = true;
        _last_write_next_total_size = 0;
    }

    // write buffer for rpc_write_stream is allocated from
    // a per-thread pool, and it is expected that
    // the per-thread pool cannot allocated two outstanding
    // buffers at the same time.
    // e.g., alloc1, commit1, alloc2, commit2 is ok
    // while alloc1, alloc2, commit2, commit 1 is invalid
    void commit_buffer()
    {
        if (!_last_write_next_committed) {
            msg->write_commit((size_t)(total_size() - _last_write_next_total_size));
            _last_write_next_committed = true;
        }
    }

    virtual ~rpc_write_stream() { flush(); }

    virtual void flush() override
    {
        binary_writer::flush();
        commit_buffer();
    }

private:
    virtual void create_new_buffer(size_t size, /*out*/ blob &bb) override
    {
        commit_buffer();

        void *ptr;
        size_t sz;
        msg->write_next(&ptr, &sz, size);
        dbg_dassert(sz >= size, "allocated buffer size must be not less than the required size");
        bb.assign((const char *)ptr, 0, (int)sz);

        _last_write_next_total_size = total_size();
        _last_write_next_committed = false;
    }

private:
    message_ex *msg;
    bool _last_write_next_committed;
    int _last_write_next_total_size;
};
/*@}*/
}
