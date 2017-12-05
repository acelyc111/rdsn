#include <dsn/utility/mm.h>

namespace dsn {

binary_reader::binary_reader(const blob &blob) { init(blob); }

void binary_reader::init(const blob &bb)
{
    _blob = bb;
    _size = bb.length();
    _ptr = bb.data();
    _remaining_size = _size;
}

int binary_reader::read(/*out*/ std::string &s)
{
    int len;
    if (0 == read(len))
        return 0;

    s.resize(len, 0);

    if (len > 0) {
        int x = read((char *)&s[0], len);
        return x == 0 ? x : (x + sizeof(len));
    } else {
        return static_cast<int>(sizeof(len));
    }
}

int binary_reader::read(blob &blob)
{
    int len;
    if (0 == read(len))
        return 0;

    if (len <= get_remaining_size()) {
        blob = _blob.range(static_cast<int>(_ptr - _blob.data()), len);

        // optimization: zero-copy
        if (!blob.buffer_ptr()) {
            std::shared_ptr<char> buffer(::dsn::make_shared_array<char>(len));
            memcpy(buffer.get(), blob.data(), blob.length());
            blob = ::dsn::blob(buffer, 0, blob.length());
        }

        _ptr += len;
        _remaining_size -= len;
        return len + sizeof(len);
    } else {
        assert(false);
        return 0;
    }
}

int binary_reader::read(char *buffer, int sz)
{
    if (sz <= get_remaining_size()) {
        memcpy((void *)buffer, _ptr, sz);
        _ptr += sz;
        _remaining_size -= sz;
        return sz;
    } else {
        assert(false);
        return 0;
    }
}

bool binary_reader::next(const void **data, int *size)
{
    if (get_remaining_size() > 0) {
        *data = (const void *)_ptr;
        *size = _remaining_size;

        _ptr += _remaining_size;
        _remaining_size = 0;
        return true;
    } else
        return false;
}

bool binary_reader::backup(int count)
{
    if (count <= static_cast<int>(_ptr - _blob.data())) {
        _ptr -= count;
        _remaining_size += count;
        return true;
    } else
        return false;
}

bool binary_reader::skip(int count)
{
    if (count <= get_remaining_size()) {
        _ptr += count;
        _remaining_size -= count;
        return true;
    } else {
        assert(false);
        return false;
    }
}

int binary_writer::_reserved_size_per_buffer_static = 256;

binary_writer::binary_writer(int reserveBufferSize)
{
    _total_size = 0;
    _buffers.reserve(1);
    _reserved_size_per_buffer =
        (reserveBufferSize == 0) ? _reserved_size_per_buffer_static : reserveBufferSize;
    _current_buffer = nullptr;
    _current_offset = 0;
    _current_buffer_length = 0;
}

binary_writer::binary_writer(blob &buffer)
{
    _total_size = 0;
    _buffers.reserve(1);
    _reserved_size_per_buffer = _reserved_size_per_buffer_static;

    _buffers.push_back(buffer);
    _current_buffer = (char *)buffer.data();
    _current_offset = 0;
    _current_buffer_length = buffer.length();
}

binary_writer::~binary_writer() {}

void binary_writer::flush() { commit(); }

void binary_writer::create_buffer(size_t size)
{
    commit();

    blob bb;
    create_new_buffer(size, bb);
    _buffers.push_back(bb);

    _current_buffer = (char *)bb.data();
    _current_buffer_length = bb.length();
}

void binary_writer::create_new_buffer(size_t size, /*out*/ blob &bb)
{
    bb.assign(::dsn::make_shared_array<char>(size), 0, (int)size);
}

void binary_writer::commit()
{
    if (_current_offset > 0) {
        *_buffers.rbegin() = _buffers.rbegin()->range(0, _current_offset);

        _current_offset = 0;
        _current_buffer_length = 0;
    }
}

blob binary_writer::get_buffer()
{
    commit();

    if (_buffers.size() == 1) {
        return _buffers[0];
    } else if (_total_size == 0) {
        return blob();
    } else {
        std::shared_ptr<char> bptr(::dsn::make_shared_array<char>(_total_size));
        blob bb(bptr, _total_size);
        const char *ptr = bb.data();

        for (int i = 0; i < static_cast<int>(_buffers.size()); i++) {
            memcpy((void *)ptr, (const void *)_buffers[i].data(), (size_t)_buffers[i].length());
            ptr += _buffers[i].length();
        }
        return bb;
    }
}

blob binary_writer::get_current_buffer()
{
    if (_buffers.size() == 1) {
        return _current_offset > 0 ? _buffers[0].range(0, _current_offset) : _buffers[0];
    } else {
        std::shared_ptr<char> bptr(::dsn::make_shared_array<char>(_total_size));
        blob bb(bptr, _total_size);
        const char *ptr = bb.data();

        for (int i = 0; i < static_cast<int>(_buffers.size()); i++) {
            size_t len = (size_t)_buffers[i].length();
            if (_current_offset > 0 && i + 1 == (int)_buffers.size()) {
                len = _current_offset;
            }

            memcpy((void *)ptr, (const void *)_buffers[i].data(), len);
            ptr += _buffers[i].length();
        }
        return bb;
    }
}

void binary_writer::write_empty(int sz)
{
    int sz0 = sz;
    int rem_size = _current_buffer_length - _current_offset;
    if (rem_size >= sz) {
        _current_offset += sz;
    } else {
        _current_offset += rem_size;
        sz -= rem_size;

        int allocSize = _reserved_size_per_buffer;
        if (sz > allocSize)
            allocSize = sz;

        create_buffer(allocSize);
        _current_offset += sz;
    }

    _total_size += sz0;
}

void binary_writer::write(const char *buffer, int sz)
{
    int rem_size = _current_buffer_length - _current_offset;
    if (rem_size >= sz) {
        memcpy((void *)(_current_buffer + _current_offset), buffer, (size_t)sz);
        _current_offset += sz;
        _total_size += sz;
    } else {
        if (rem_size > 0) {
            memcpy((void *)(_current_buffer + _current_offset), buffer, (size_t)rem_size);
            _current_offset += rem_size;
            _total_size += rem_size;
            sz -= rem_size;
        }

        int allocSize = _reserved_size_per_buffer;
        if (sz > allocSize)
            allocSize = sz;

        create_buffer(allocSize);
        memcpy((void *)(_current_buffer + _current_offset), buffer + rem_size, (size_t)sz);
        _current_offset += sz;
        _total_size += sz;
    }
}

bool binary_writer::next(void **data, int *size)
{
    int rem_size = _current_buffer_length - _current_offset;
    if (rem_size == 0) {
        create_buffer(_reserved_size_per_buffer);
        rem_size = _current_buffer_length;
    }

    *size = rem_size;
    *data = (void *)(_current_buffer + _current_offset);
    _current_offset = _current_buffer_length;
    _total_size += rem_size;
    return true;
}

bool binary_writer::backup(int count)
{
    assert(count <= _current_offset);
    _current_offset -= count;
    _total_size -= count;
    return true;
}
}
