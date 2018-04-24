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

#include "simple_kv.server.h"

namespace dsn {
namespace replication {
namespace test {

class simple_kv_service_impl : public simple_kv_service
{
public:
    static bool s_simple_kv_open_fail;
    static bool s_simple_kv_close_fail;
    static bool s_simple_kv_get_checkpoint_fail;
    static bool s_simple_kv_apply_checkpoint_fail;

    static void register_service()
    {
        replication_app_base::register_storage_engine(
            "simple_kv", replication_app_base::create<simple_kv_service_impl>);
        simple_kv_service::register_rpc_handlers();
    }

public:
    simple_kv_service_impl(replica *r);

    // RPC_SIMPLE_KV_READ
    virtual void on_read(const std::string &key, ::dsn::rpc_replier<std::string> &reply);
    // RPC_SIMPLE_KV_WRITE
    virtual void on_write(const kv_pair &pr, ::dsn::rpc_replier<int32_t> &reply);
    // RPC_SIMPLE_KV_APPEND
    virtual void on_append(const kv_pair &pr, ::dsn::rpc_replier<int32_t> &reply);

    virtual ::dsn::error_code start(int argc, char **argv) override;

    virtual ::dsn::error_code stop(bool cleanup = false) override;

    virtual int64_t last_durable_decree() const override { return _last_durable_decree; }

    virtual ::dsn::error_code sync_checkpoint() override;

    virtual ::dsn::error_code prepare_get_checkpoint(blob &learn_req) { return dsn::ERR_OK; }

    virtual ::dsn::error_code async_checkpoint(bool is_emergency) override;

    virtual ::dsn::error_code copy_checkpoint_to_dir(const char *checkpoint_dir,
                                                     int64_t *last_decree) override
    {
        return ERR_NOT_IMPLEMENTED;
    }

    virtual ::dsn::error_code get_checkpoint(int64_t learn_start,
                                             const dsn::blob &learn_request,
                                             /*out*/ learn_state &state) override;

    virtual ::dsn::error_code storage_apply_checkpoint(chkpt_apply_mode mode,
                                                       const learn_state &state) override;

    virtual uint64_t last_compact_finish_time() { return 0; }
    virtual void manual_compact(const std::map<std::string, std::string> &opts) {}
    virtual void update_app_envs(const std::map<std::string, std::string> &envs) {}
    virtual void query_app_envs(/*out*/ std::map<std::string, std::string> &envs) {}

private:
    void recover();
    void recover(const std::string &name, int64_t version);
    void set_last_durable_decree(int64_t d) { _last_durable_decree = d; }

    void reset_state();

private:
    typedef std::map<std::string, std::string> simple_kv;
    simple_kv _store;
    ::dsn::service::zlock _lock;
    bool _test_file_learning;

    int64_t _last_durable_decree;
};
}
}
}
