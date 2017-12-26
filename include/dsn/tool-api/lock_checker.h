#pragma once

namespace dsn {
class task;
class lock_checker
{
public:
    static __thread int zlock_exclusive_count;
    static __thread int zlock_shared_count;
    static void check_wait_safety();
    static void check_dangling_lock();
    static void check_wait_task(task *waitee);
};
}
