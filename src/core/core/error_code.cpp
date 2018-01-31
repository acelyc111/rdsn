#include <dsn/utility/error_code.h>
#include <dsn/utility/char_ptr.h>

#include <thrift/Thrift.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>
#include <thrift/protocol/TVirtualProtocol.h>
#include <thrift/transport/TVirtualTransport.h>
#include <thrift/TApplicationException.h>

namespace dsn {
/*static*/
int error_code::max()
{
    return dsn::utils::customized_id_mgr<dsn::error_code>::instance().max_value();
}
/*static*/
bool error_code::is_exist(const char *name)
{
    return dsn::utils::customized_id_mgr<dsn::error_code>::instance().get_id(name) != -1;
}
/*static*/
error_code error_code::try_get(const char *name, error_code default_value)
{
    int ans = dsn::utils::customized_id_mgr<dsn::error_code>::instance().get_id(name);
    if (ans == -1)
        return default_value;
    return error_code(ans);
}

error_code::error_code(const char *name)
{
    _internal_code = dsn::utils::customized_id_mgr<dsn::error_code>::instance().register_id(name);
}

const char *error_code::to_string() const
{
    return dsn::utils::customized_id_mgr<dsn::error_code>::instance().get_name(_internal_code);
}

uint32_t error_code::read(apache::thrift::protocol::TProtocol *iprot)
{
    std::string ec_string;
    apache::thrift::protocol::TBinaryProtocol *binary_proto =
        dynamic_cast<apache::thrift::protocol::TBinaryProtocol *>(iprot);
    uint32_t xfer = 0;
    if (binary_proto != nullptr) {
        // the protocol is binary protocol
        xfer += iprot->readString(ec_string);
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
                    xfer += iprot->readString(ec_string);
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
    *this = error_code::try_get(ec_string.c_str(), ERR_UNKNOWN);
    return xfer;
}

uint32_t error_code::write(apache::thrift::protocol::TProtocol *oprot) const
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
        xfer += oprot->writeStructBegin("error_code");

        xfer += oprot->writeFieldBegin("code", ::apache::thrift::protocol::T_STRING, 1);
        xfer += oprot->writeString(std::string(name));
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
}
}
