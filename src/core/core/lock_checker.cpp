#include <dsn/tool-api/lock_checker.h>
#include <dsn/tool-api/task.h>
#include "service_engine.h"
#include "task_engine.h"

namespace dsn {
__thread int lock_checker::zlock_exclusive_count = 0;
__thread int lock_checker::zlock_shared_count = 0;

void lock_checker::check_wait_safety()
{
    if (zlock_exclusive_count + zlock_shared_count > 0) {
        dwarn("wait inside locks may lead to deadlocks - current thread owns %u exclusive locks "
              "and %u shared locks now.",
              zlock_exclusive_count,
              zlock_shared_count);
    }
}

void lock_checker::check_dangling_lock()
{
    if (zlock_exclusive_count + zlock_shared_count > 0) {
        dwarn("locks should not be hold at this point - current thread owns %u exclusive locks and "
              "%u shared locks now.",
              zlock_exclusive_count,
              zlock_shared_count);
    }
}

void lock_checker::check_wait_task(task *waitee)
{
    check_wait_safety();

    // not in worker thread
    if (task::get_current_worker() == nullptr)
        return;

    // caller and callee don't share the same thread pool,
    if (waitee->spec().type != TASK_TYPE_RPC_RESPONSE &&
        (waitee->spec().pool_code != task::get_current_worker()->pool_spec().pool_code))
        return;

    // callee is empty
    if (waitee->is_empty())
        return;

    // there are enough concurrency
    if (!task::get_current_worker()->pool_spec().partitioned &&
        task::get_current_worker()->pool_spec().worker_count > 1)
        return;

    dwarn("task %s waits for another task %s sharing the same thread pool "
          "- will lead to deadlocks easily (e.g., when worker_count = 1 or when the pool "
          "is partitioned)",
          task::get_current_task()->spec().code.to_string(),
          waitee->spec().code.to_string());
}
}
