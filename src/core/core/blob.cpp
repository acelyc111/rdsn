#include <dsn/utility/blob.h>

#ifdef DSN_USE_THRIFT_SERIALIZATION
#include <dsn/utility/mm.h>

#include <thrift/Thrift.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>
#include <thrift/protocol/TVirtualProtocol.h>
#include <thrift/transport/TVirtualTransport.h>
#include <thrift/TApplicationException.h>
#endif

namespace dsn {

#ifdef DSN_USE_THRIFT_SERIALIZATION
class blob_string
{
private:
    blob &_buffer;

public:
    blob_string(blob &bb) : _buffer(bb) {}

    void clear() { _buffer.assign(std::shared_ptr<char>(nullptr), 0, 0); }
    void resize(std::size_t new_size)
    {
        std::shared_ptr<char> b(dsn::make_shared_array<char>(new_size));
        _buffer.assign(b, 0, static_cast<int>(new_size));
    }
    void assign(const char *ptr, std::size_t size)
    {
        std::shared_ptr<char> b(dsn::make_shared_array<char>(size));
        memcpy(b.get(), ptr, size);
        _buffer.assign(b, 0, static_cast<int>(size));
    }
    const char *data() const { return _buffer.data(); }
    size_t size() const { return _buffer.length(); }

    char &operator[](int pos) { return const_cast<char *>(_buffer.data())[pos]; }
};

uint32_t blob::read(apache::thrift::protocol::TProtocol *iprot)
{
    // for optimization, it is dangerous if the oprot is not a binary proto
    apache::thrift::protocol::TBinaryProtocol *binary_proto =
        static_cast<apache::thrift::protocol::TBinaryProtocol *>(iprot);
    blob_string str(*this);
    return binary_proto->readString<blob_string>(str);
}

uint32_t blob::write(apache::thrift::protocol::TProtocol *oprot) const
{
    apache::thrift::protocol::TBinaryProtocol *binary_proto =
        static_cast<apache::thrift::protocol::TBinaryProtocol *>(oprot);
    return binary_proto->writeString<blob_string>(blob_string(const_cast<blob &>(*this)));
}
#endif
}
