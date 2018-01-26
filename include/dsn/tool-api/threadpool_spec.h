#pragma once

#include <string>
#include <list>

#include <dsn/utility/enum_helper.h>
#include <dsn/utility/config_helper.h>
#include <dsn/tool-api/threadpool_code.h>

namespace dsn {

enum worker_priority_t
{
    THREAD_xPRIORITY_LOWEST,
    THREAD_xPRIORITY_BELOW_NORMAL,
    THREAD_xPRIORITY_NORMAL,
    THREAD_xPRIORITY_ABOVE_NORMAL,
    THREAD_xPRIORITY_HIGHEST,
    THREAD_xPRIORITY_COUNT,
    THREAD_xPRIORITY_INVALID
};

ENUM_BEGIN(worker_priority_t, THREAD_xPRIORITY_INVALID)
ENUM_REG(THREAD_xPRIORITY_LOWEST)
ENUM_REG(THREAD_xPRIORITY_BELOW_NORMAL)
ENUM_REG(THREAD_xPRIORITY_NORMAL)
ENUM_REG(THREAD_xPRIORITY_ABOVE_NORMAL)
ENUM_REG(THREAD_xPRIORITY_HIGHEST)
ENUM_END(worker_priority_t)

struct threadpool_spec
{
    std::string name;
    dsn::threadpool_code pool_code;
    int worker_count;
    worker_priority_t worker_priority;
    bool worker_share_core;
    uint64_t worker_affinity_mask;
    int dequeue_batch_size;
    bool partitioned; // false by default
    std::string queue_factory_name;
    std::string worker_factory_name;
    std::list<std::string> queue_aspects;
    std::list<std::string> worker_aspects;
    int queue_length_throttling_threshold;
    bool enable_virtual_queue_throttling;
    std::string admission_controller_factory_name;
    std::string admission_controller_arguments;

    threadpool_spec(const dsn::threadpool_code &code) : name(code.to_string()), pool_code(code) {}
    threadpool_spec(const threadpool_spec &source) = default;
    threadpool_spec &operator=(const threadpool_spec &source) = default;

    static bool init(/*out*/ std::vector<threadpool_spec> &specs);
};

CONFIG_BEGIN(threadpool_spec)
CONFIG_FLD_STRING(name, "", "thread pool name")
CONFIG_FLD(int, uint64, worker_count, 2, "thread/worker count")
CONFIG_FLD(int,
           uint64,
           dequeue_batch_size,
           5,
           "how many tasks (if available) should be returned "
           "for one dequeue call for best batching performance")
CONFIG_FLD_ENUM(worker_priority_t,
                worker_priority,
                THREAD_xPRIORITY_NORMAL,
                THREAD_xPRIORITY_INVALID,
                false,
                "thread priority")
CONFIG_FLD(bool, bool, worker_share_core, true, "whether the threads share all assigned cores")
CONFIG_FLD(uint64_t,
           uint64,
           worker_affinity_mask,
           0,
           "what CPU cores are assigned to this pool, 0 for all")
CONFIG_FLD(bool,
           bool,
           partitioned,
           false,
           "whethe the threads share a single "
           "queue(partitioned=false) or not; the latter is usually "
           "for workload hash partitioning for avoiding locking")
CONFIG_FLD_STRING(queue_factory_name, "", "task queue provider name")
CONFIG_FLD_STRING(worker_factory_name, "", "task worker provider name")
CONFIG_FLD_STRING_LIST(queue_aspects, "task queue aspects names, usually for tooling purpose")
CONFIG_FLD_STRING_LIST(worker_aspects, "task aspects names, usually for tooling purpose")
CONFIG_FLD(int,
           uint64,
           queue_length_throttling_threshold,
           1000000,
           "throttling: throttling threshold above which rpc requests will be dropped")
CONFIG_FLD(bool,
           bool,
           enable_virtual_queue_throttling,
           false,
           "throttling: whether to enable throttling with virtual queues")
CONFIG_FLD_STRING(admission_controller_factory_name,
                  "",
                  "customized admission controller for the task queues")
CONFIG_FLD_STRING(admission_controller_arguments,
                  "",
                  "arguments for the cusotmized admission controller")
CONFIG_END
}
