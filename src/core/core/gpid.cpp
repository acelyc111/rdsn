#include <dsn/tool-api/gpid.h>
#include <cstring>

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
}
