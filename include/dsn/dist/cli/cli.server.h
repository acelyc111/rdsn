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

#include <dsn/tool-api/rpc_address.h>
#include <dsn/tool-api/command_manager.h>
#include <dsn/c/api_common.h>
#include <dsn/cpp/serverlet.h>

namespace dsn {
namespace service {
class cli_server : serverlet<cli_server>
{
public:
    cli_server() : serverlet("cli_server"), _cmd_manager(command_manager::instance()) {}
    void open_service();

    void close_service();

    void set_cli_target_address(dsn_handle_t handle, dsn::rpc_address target);

    bool run_command(const std::string &cmdline, /*out*/ std::string &output);
    bool run_command(const std::string &cmd,
                     const std::vector<std::string> &args,
                     /*out*/ std::string &output);

private:
    void on_remote_cli(message_ex *req);

private:
    command_manager &_cmd_manager;
};
}
}
