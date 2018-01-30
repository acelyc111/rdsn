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

#include <dsn/dist/cli/cli.h>
#include <dsn/dist/cli/cli.server.h>

#include "core/core/service_engine.h"
#include "core/core/rpc_engine.h"

namespace dsn {
namespace service {
void cli_server::open_service()
{
    register_rpc_handler(RPC_CLI_CLI_CALL, "dsn.cli", &cli_server::on_remote_cli);
}

void cli_server::close_service()
{
    ddebug("start to unregister RPC_CLI_CLI_CALL");
    unregister_rpc_handler(RPC_CLI_CLI_CALL);
}

void cli_server::set_cli_target_address(dsn_handle_t handle, rpc_address target)
{
    reinterpret_cast<command_instance *>(handle)->address = target;
}

void cli_server::on_remote_cli(dsn::message_ex *req)
{
    ::dsn::command cmd;
    std::string result;

    ::dsn::unmarshall(req, cmd);
    run_command(cmd.cmd, cmd.arguments, result);

    reply(req, result);
}

bool cli_server::run_command(const std::string &cmdline, std::string &output)
{
    auto cnode = ::dsn::task::get_current_node2();

    if (cnode == nullptr) {
        auto &all_nodes = ::dsn::service_engine::fast_instance().get_all_nodes();
        dassert(!all_nodes.empty(), "no node to mimic!");
        dsn_mimic_app(all_nodes.begin()->second->spec().role_name.c_str(), 1);
    }
    std::string scmd = cmdline;
    std::vector<std::string> args;

    utils::split_args(scmd.c_str(), args, ' ');

    if (args.size() < 1)
        return false;

    std::vector<std::string> args2;
    for (size_t i = 1; i < args.size(); i++) {
        args2.push_back(args[i]);
    }

    return run_command(args[0], args2, output);
}

bool cli_server::run_command(const std::string &cmd,
                             const std::vector<std::string> &args,
                             std::string &output)
{
    command_instance *cmd_instance = _cmd_manager.get_command_instance(cmd);

    if (cmd_instance == nullptr) {
        output = std::string("unknown command '") + cmd + "'";
        return false;
    } else {
        if (cmd_instance->address.is_invalid() ||
            cmd_instance->address == dsn::task::get_current_rpc()->primary_address()) {
            output = cmd_instance->handler(args);
            return true;
        } else {
            dsn::message_ex *msg = dsn::message_ex::create_request(RPC_CLI_CLI_CALL);
            ::dsn::command rcmd;
            rcmd.cmd = cmd;
            rcmd.arguments = args;
            ::dsn::marshall(msg, rcmd);
            auto resp = dsn_rpc_call_wait(cmd_instance->address, msg);
            if (resp != nullptr) {
                ::dsn::unmarshall(resp, output);
                return true;
            } else {
                dwarn("cli run for %s is too long, timeout", cmd.c_str());
                return false;
            }
        }
    }
}
}
}
