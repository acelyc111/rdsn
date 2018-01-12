#pragma once

namespace dsn {
class thread_access_checker
{
public:
    thread_access_checker();
    ~thread_access_checker();

    void only_one_thread_access();

private:
    int _access_thread_id;
    bool _access_thread_id_inited;
};
}
