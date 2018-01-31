#include <dsn/utility/customizable_id.h>
#include <dsn/tool-api/task_code.h>
#include <dsn/tool-api/task_spec.h>

#ifdef DSN_USE_THRIFT_SERIALIZATION
#include <dsn/utility/char_ptr.h>

#include <thrift/Thrift.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>
#include <thrift/protocol/TVirtualProtocol.h>
#include <thrift/transport/TVirtualTransport.h>
#include <thrift/TApplicationException.h>
#endif

namespace dsn {

typedef dsn::utils::customized_id_mgr<dsn::task_code> task_code_mgr;

/*static*/
int task_code::max() { return task_code_mgr::instance().max_value(); }

/*static*/
bool task_code::is_exist(const char *name) { return task_code_mgr::instance().get_id(name) != -1; }

/*static*/
task_code task_code::try_get(const char *name, task_code default_value)
{
    int code = task_code_mgr::instance().get_id(name);
    if (code == -1)
        return default_value;
    return task_code(code);
}

task_code::task_code(const char *name) : _internal_code(task_code_mgr::instance().register_id(name))
{
}

task_code::task_code(const char *name,
                     dsn_task_type_t tt,
                     dsn_task_priority_t pri,
                     dsn::threadpool_code pool)
    : task_code(name)
{
    task_spec::register_task_code(*this, tt, pri, pool);
}

task_code::task_code(const char *name,
                     dsn_task_type_t tt,
                     dsn_task_priority_t pri,
                     dsn::threadpool_code pool,
                     bool is_storage_write,
                     bool allow_batch)
    : task_code(name)
{
    task_spec::register_storage_task_code(*this, tt, pri, pool, is_storage_write, allow_batch);
}

const char *task_code::to_string() const
{
    return task_code_mgr::instance().get_name(_internal_code);
}

#ifdef DSN_USE_THRIFT_SERIALIZATION
uint32_t task_code::read(apache::thrift::protocol::TProtocol *iprot)
{
    std::string task_code_string;
    uint32_t xfer = 0;
    apache::thrift::protocol::TBinaryProtocol *binary_proto =
        dynamic_cast<apache::thrift::protocol::TBinaryProtocol *>(iprot);
    if (binary_proto != nullptr) {
        // the protocol is binary protocol
        xfer += iprot->readString(task_code_string);
    } else {
        // the protocol is json protocol
        std::string fname;
        ::apache::thrift::protocol::TType ftype;
        int16_t fid;

        xfer += iprot->readStructBegin(fname);

        using ::apache::thrift::protocol::TProtocolException;

        while (true) {
            xfer += iprot->readFieldBegin(fname, ftype, fid);
            if (ftype == ::apache::thrift::protocol::T_STOP) {
                break;
            }
            switch (fid) {
            case 1:
                if (ftype == ::apache::thrift::protocol::T_STRING) {
                    xfer += iprot->readString(task_code_string);
                } else {
                    xfer += iprot->skip(ftype);
                }
                break;
            default:
                xfer += iprot->skip(ftype);
                break;
            }
            xfer += iprot->readFieldEnd();
        }

        xfer += iprot->readStructEnd();
    }
    _internal_code = try_get(task_code_string.c_str(), TASK_CODE_INVALID);
    return xfer;
}

uint32_t task_code::write(apache::thrift::protocol::TProtocol *oprot) const
{
    const char *name = to_string();
    apache::thrift::protocol::TBinaryProtocol *binary_proto =
        dynamic_cast<apache::thrift::protocol::TBinaryProtocol *>(oprot);
    if (binary_proto != nullptr) {
        // the protocol is binary protocol
        return binary_proto->writeString<char_ptr>(char_ptr(name, static_cast<int>(strlen(name))));
    } else {
        // the protocol is json protocol
        uint32_t xfer = 0;
        xfer += oprot->writeStructBegin("task_code");

        xfer += oprot->writeFieldBegin("code", ::apache::thrift::protocol::T_STRING, 1);
        xfer += oprot->writeString(std::string(name));
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
}
#endif
}
