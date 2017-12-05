#pragma once

#include <dsn/utility/blob.h>
#include <cstring>

namespace dsn {

template <typename T>
std::shared_ptr<T> make_shared_array(size_t size)
{
    return std::shared_ptr<T>(new T[size], std::default_delete<T[]>());
}

class binary_reader
{
public:
    // given bb on ctor
    binary_reader(const blob &blob);

    // or delayed init
    binary_reader() {}

    virtual ~binary_reader() {}

    void init(const blob &bb);

    template <typename T>
    int read_pod(/*out*/ T &val);
    template <typename T>
    int read(/*out*/ T &val)
    {
        // read of this type is not implemented
        assert(false);
        return 0;
    }
    int read(/*out*/ int8_t &val) { return read_pod(val); }
    int read(/*out*/ uint8_t &val) { return read_pod(val); }
    int read(/*out*/ int16_t &val) { return read_pod(val); }
    int read(/*out*/ uint16_t &val) { return read_pod(val); }
    int read(/*out*/ int32_t &val) { return read_pod(val); }
    int read(/*out*/ uint32_t &val) { return read_pod(val); }
    int read(/*out*/ int64_t &val) { return read_pod(val); }
    int read(/*out*/ uint64_t &val) { return read_pod(val); }
    int read(/*out*/ bool &val) { return read_pod(val); }

    int read(/*out*/ std::string &s);
    int read(char *buffer, int sz);
    int read(blob &blob);

    bool next(const void **data, int *size);
    bool skip(int count);
    bool backup(int count);

    blob get_buffer() const { return _blob; }
    blob get_remaining_buffer() const { return _blob.range(static_cast<int>(_ptr - _blob.data())); }
    bool is_eof() const { return _ptr >= _blob.data() + _size; }
    int total_size() const { return _size; }
    int get_remaining_size() const { return _remaining_size; }

private:
    blob _blob;
    int _size;
    const char *_ptr;
    int _remaining_size;
};

class binary_writer
{
public:
    binary_writer(int reserved_buffer_size = 0);
    binary_writer(blob &buffer);
    virtual ~binary_writer();

    virtual void flush();

    template <typename T>
    void write_pod(const T &val);
    template <typename T>
    void write(const T &val)
    {
        // write of this type is not implemented
        assert(false);
    }
    void write(const int8_t &val) { write_pod(val); }
    void write(const uint8_t &val) { write_pod(val); }
    void write(const int16_t &val) { write_pod(val); }
    void write(const uint16_t &val) { write_pod(val); }
    void write(const int32_t &val) { write_pod(val); }
    void write(const uint32_t &val) { write_pod(val); }
    void write(const int64_t &val) { write_pod(val); }
    void write(const uint64_t &val) { write_pod(val); }
    void write(const bool &val) { write_pod(val); }

    void write(const std::string &val);
    void write(const char *buffer, int sz);
    void write(const blob &val);
    void write_empty(int sz);

    bool next(void **data, int *size);
    bool backup(int count);

    void get_buffers(/*out*/ std::vector<blob> &buffers);
    int get_buffer_count() const { return static_cast<int>(_buffers.size()); }
    blob get_buffer();
    blob get_current_buffer(); // without commit, write can be continued on the last buffer
    blob get_first_buffer() const;

    int total_size() const { return _total_size; }

protected:
    // bb may have large space than size
    void create_buffer(size_t size);
    void commit();
    virtual void create_new_buffer(size_t size, /*out*/ blob &bb);

private:
    std::vector<blob> _buffers;

    char *_current_buffer;
    int _current_offset;
    int _current_buffer_length;

    int _total_size;
    int _reserved_size_per_buffer;
    static int _reserved_size_per_buffer_static;
};

//--------------- inline implementation -------------------
template <typename T>
inline int binary_reader::read_pod(/*out*/ T &val)
{
    if (sizeof(T) <= get_remaining_size()) {
        memcpy((void *)&val, _ptr, sizeof(T));
        _ptr += sizeof(T);
        _remaining_size -= sizeof(T);
        return static_cast<int>(sizeof(T));
    } else {
        // read beyond the end of buffer
        assert(false);
        return 0;
    }
}

template <typename T>
inline void binary_writer::write_pod(const T &val)
{
    write((char *)&val, static_cast<int>(sizeof(T)));
}

inline void binary_writer::get_buffers(/*out*/ std::vector<blob> &buffers)
{
    commit();
    buffers = _buffers;
}

inline blob binary_writer::get_first_buffer() const { return _buffers[0]; }

inline void binary_writer::write(const std::string &val)
{
    int len = static_cast<int>(val.length());
    write((const char *)&len, sizeof(int));
    if (len > 0)
        write((const char *)&val[0], len);
}

inline void binary_writer::write(const blob &val)
{
    // TODO: optimization by not memcpy
    int len = val.length();
    write((const char *)&len, sizeof(int));
    if (len > 0)
        write((const char *)val.data(), len);
}
}
