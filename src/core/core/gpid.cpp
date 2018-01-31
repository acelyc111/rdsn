#include <dsn/tool-api/gpid.h>
#include <cstring>

#ifdef DSN_USE_THRIFT_SERIALIZATION
#include <thrift/Thrift.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>
#include <thrift/protocol/TVirtualProtocol.h>
#include <thrift/transport/TVirtualTransport.h>
#include <thrift/TApplicationException.h>
#endif

namespace dsn {

bool gpid::parse_from(const char *str)
{
    return sscanf(str, "%d.%d", &_value.u.app_id, &_value.u.partition_index) == 2;
}

char __thread buffer[8][64];
unsigned int __thread index;

const char *gpid::to_string() const
{
    char *b = buffer[(++index) % 8];
    snprintf(b, 64, "%d.%d", _value.u.app_id, _value.u.partition_index);
    return b;
}

#ifdef DSN_USE_THRIFT_SERIALIZATION
uint32_t gpid::read(apache::thrift::protocol::TProtocol *iprot)
{
    apache::thrift::protocol::TBinaryProtocol *binary_proto =
        dynamic_cast<apache::thrift::protocol::TBinaryProtocol *>(iprot);
    if (binary_proto != nullptr) {
        // the protocol is binary protocol
        return iprot->readI64(reinterpret_cast<int64_t &>(_value.value));
    } else {
        // the protocol is json protocol
        uint32_t xfer = 0;
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
                if (ftype == ::apache::thrift::protocol::T_I64) {
                    xfer += iprot->readI64(reinterpret_cast<int64_t &>(_value.value));
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

        return xfer;
    }
}

uint32_t gpid::write(apache::thrift::protocol::TProtocol *oprot) const
{
    apache::thrift::protocol::TBinaryProtocol *binary_proto =
        dynamic_cast<apache::thrift::protocol::TBinaryProtocol *>(oprot);
    if (binary_proto != nullptr) {
        // the protocol is binary protocol
        return oprot->writeI64((int64_t)_value.value);
    } else {
        // the protocol is json protocol
        uint32_t xfer = 0;

        xfer += oprot->writeStructBegin("gpid");

        xfer += oprot->writeFieldBegin("id", ::apache::thrift::protocol::T_I64, 1);
        xfer += oprot->writeI64((int64_t)_value.value);
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
}
#endif
}
