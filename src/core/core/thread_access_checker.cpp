#include <dsn/utility/utils.h>
#include <dsn/tool-api/thread_access_checker.h>
#include <dsn/tool-api/logging.h>

namespace dsn {

thread_access_checker::thread_access_checker() { _access_thread_id_inited = false; }

thread_access_checker::~thread_access_checker() { _access_thread_id_inited = false; }

void thread_access_checker::only_one_thread_access()
{
    if (_access_thread_id_inited) {
        dassert(::dsn::utils::get_current_tid() == _access_thread_id,
                "the service is assumed to be accessed by one thread only!");
    } else {
        _access_thread_id = ::dsn::utils::get_current_tid();
        _access_thread_id_inited = true;
    }
}
}
