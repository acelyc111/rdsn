#pragma once

#include <cstdint>

class char_ptr
{
private:
    const char *ptr;
    int length;

public:
    char_ptr(const char *p, int len) : ptr(p), length(len) {}
    std::size_t size() const { return length; }
    const char *data() const { return ptr; }
};
