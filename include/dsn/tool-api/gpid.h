#pragma once

#include <cstdint>

#ifdef DSN_USE_THRIFT_SERIALIZATION
#include <thrift/protocol/TProtocol.h>
#endif

namespace dsn {
class gpid
{
public:
    gpid(int app_id, int pidx)
    {
        _value.u.app_id = app_id;
        _value.u.partition_index = pidx;
    }
    gpid(const gpid &gd) { _value.value = gd._value.value; }
    gpid() { _value.value = 0; }
    uint64_t value() const { return _value.value; }

    bool operator<(const gpid &r) const
    {
        return _value.u.app_id < r._value.u.app_id ||
               (_value.u.app_id == r._value.u.app_id &&
                _value.u.partition_index < r._value.u.partition_index);
    }
    bool operator==(const gpid &r) const { return value() == r.value(); }
    bool operator!=(const gpid &r) const { return value() != r.value(); }

    int32_t get_app_id() const { return _value.u.app_id; }
    int32_t get_partition_index() const { return _value.u.partition_index; }
    void set_app_id(int32_t v) { _value.u.app_id = v; }
    void set_partition_index(int32_t v) { _value.u.partition_index = v; }
    void set_value(uint64_t v) { _value.value = v; }

#ifdef DSN_USE_THRIFT_SERIALIZATION
    uint32_t read(::apache::thrift::protocol::TProtocol *iprot);
    uint32_t write(::apache::thrift::protocol::TProtocol *oprot) const;
#endif

    int thread_hash() const { return _value.u.app_id * 7919 + _value.u.partition_index; }
private:
    union
    {
        struct
        {
            int32_t app_id;          ///< 1-based app id (0 for invalid)
            int32_t partition_index; ///< zero-based partition index
        } u;
        uint64_t value;
    } _value;
};
}

namespace std {
template <>
struct hash<::dsn::gpid>
{
    size_t operator()(const ::dsn::gpid &pid) const
    {
        return static_cast<std::size_t>(pid.thread_hash());
    }
};
}
