#pragma once

#include <dsn/utility/ports.h>
#include <dsn/utility/enum_helper.h>
#include <dsn/tool-api/threadpool_code.h>

#ifdef DSN_USE_THRIFT_SERIALIZATION
#include <thrift/protocol/TProtocol.h>
#endif

typedef enum dsn_task_type_t {
    TASK_TYPE_RPC_REQUEST,  ///< task handling rpc request
    TASK_TYPE_RPC_RESPONSE, ///< task handling rpc response or timeout
    TASK_TYPE_COMPUTE,      ///< async calls or timers
    TASK_TYPE_AIO,          ///< callback for file read and write
    TASK_TYPE_CONTINUATION, ///< above tasks are seperated into several continuation
                            ///< tasks by thread-synchronization operations.
                            ///< so that each "task" is non-blocking
    TASK_TYPE_COUNT,
    TASK_TYPE_INVALID
} dsn_task_type_t;

ENUM_BEGIN(dsn_task_type_t, TASK_TYPE_INVALID)
ENUM_REG(TASK_TYPE_RPC_REQUEST)
ENUM_REG(TASK_TYPE_RPC_RESPONSE)
ENUM_REG(TASK_TYPE_COMPUTE)
ENUM_REG(TASK_TYPE_AIO)
ENUM_REG(TASK_TYPE_CONTINUATION)
ENUM_END(dsn_task_type_t)

typedef enum dsn_task_priority_t {
    TASK_PRIORITY_LOW,
    TASK_PRIORITY_COMMON,
    TASK_PRIORITY_HIGH,
    TASK_PRIORITY_COUNT,
    TASK_PRIORITY_INVALID
} dsn_task_priority_t;

ENUM_BEGIN(dsn_task_priority_t, TASK_PRIORITY_INVALID)
ENUM_REG(TASK_PRIORITY_LOW)
ENUM_REG(TASK_PRIORITY_COMMON)
ENUM_REG(TASK_PRIORITY_HIGH)
ENUM_END(dsn_task_priority_t)

namespace dsn {
class task_code
{
public:
    task_code(const char *name,
              dsn_task_type_t tt,
              dsn_task_priority_t pri,
              dsn::threadpool_code pool);
    task_code() { _internal_code = 0; }
    task_code(const task_code &r) { _internal_code = r._internal_code; }
    explicit task_code(int code) { _internal_code = code; }

    const char *to_string() const;

    task_code &operator=(const task_code &source)
    {
        _internal_code = source._internal_code;
        return *this;
    }
    bool operator==(const task_code &r) { return _internal_code == r._internal_code; }
    bool operator!=(const task_code &r) { return !(*this == r); }
    operator int() const { return _internal_code; }
    int code() const { return _internal_code; }
#ifdef DSN_USE_THRIFT_SERIALIZATION
    uint32_t read(::apache::thrift::protocol::TProtocol *iprot);
    uint32_t write(::apache::thrift::protocol::TProtocol *oprot) const;
#endif

    static int max();
    static bool is_exist(const char *name);
    static task_code try_get(const char *name, task_code default_value);

private:
    task_code(const char *name);
    int _internal_code;
};

#define DEFINE_NAMED_TASK_CODE(x, name, pri, pool)                                                 \
    __selectany const ::dsn::task_code x(#name, TASK_TYPE_COMPUTE, pri, pool);

#define DEFINE_NAMED_TASK_CODE_AIO(x, name, pri, pool)                                             \
    __selectany const ::dsn::task_code x(#name, TASK_TYPE_AIO, pri, pool);

#define DEFINE_NAMED_TASK_CODE_RPC(x, name, pri, pool)                                             \
    __selectany const ::dsn::task_code x(#name, TASK_TYPE_RPC_REQUEST, pri, pool);                 \
    __selectany const ::dsn::task_code x##_ACK(#name "_ACK", TASK_TYPE_RPC_RESPONSE, pri, pool);

/*! define a new task code with TASK_TYPE_COMPUTATION */
#define DEFINE_TASK_CODE(x, pri, pool) DEFINE_NAMED_TASK_CODE(x, x, pri, pool)
#define DEFINE_TASK_CODE_AIO(x, pri, pool) DEFINE_NAMED_TASK_CODE_AIO(x, x, pri, pool)
#define DEFINE_TASK_CODE_RPC(x, pri, pool) DEFINE_NAMED_TASK_CODE_RPC(x, x, pri, pool)

DEFINE_TASK_CODE(TASK_CODE_INVALID, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
}
