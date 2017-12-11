#pragma once

#include <dsn/utility/autoref_ptr.h>

namespace dsn {
typedef void (*safe_handle_release)(void *);

template <safe_handle_release releaser>
class safe_handle : public ::dsn::ref_counter
{
public:
    safe_handle(void *handle, bool is_owner)
    {
        _handle = handle;
        _is_owner = is_owner;
    }

    safe_handle()
    {
        _handle = nullptr;
        _is_owner = false;
    }

    void assign(void *handle, bool is_owner)
    {
        clear();

        _handle = handle;
        _is_owner = is_owner;
    }

    void set_owner(bool owner = true) { _is_owner = owner; }

    ~safe_handle() { clear(); }

    void *native_handle() const { return _handle; }

private:
    void clear()
    {
        if (_is_owner && nullptr != _handle) {
            releaser(_handle);
            _handle = nullptr;
        }
    }

private:
    void *_handle;
    bool _is_owner;
};
}
