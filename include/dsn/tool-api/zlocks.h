/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/*
 * Description:
 *     lock implementation atop c service api
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <boost/noncopyable.hpp>
#include <dsn/utility/utils.h>
#include <dsn/tool-api/zlock_provider.h>

namespace dsn {
namespace service {

/*!
@addtogroup sync-exlock
@{
*/
class zlock : private boost::noncopyable
{
public:
    zlock(bool recursive = false);
    ~zlock();

    void lock();
    bool try_lock();
    void unlock();

private:
    mutex_base *_h;
};
/*@}*/

/*!
@addtogroup sync-rwlock
@{
*/
class zrwlock_nr : private boost::noncopyable
{
public:
    zrwlock_nr();
    ~zrwlock_nr();

    void lock_read();
    void unlock_read();
    bool try_lock_read();

    void lock_write();
    void unlock_write();
    bool try_lock_write();

private:
    rwlock_nr_provider *_h;
};
/*@}*/

/*!
@addtogroup sync-sema
@{
*/
class zsemaphore : private boost::noncopyable
{
public:
    zsemaphore(int initial_count = 0);
    ~zsemaphore();

public:
    void signal(int count = 1);
    bool wait(int timeout_milliseconds = TIME_MS_MAX);

private:
    semaphore_provider *_h;
};

class zevent : private boost::noncopyable
{
public:
    zevent(bool manualReset, bool initState = false);
    ~zevent();

public:
    void set();
    void reset();
    bool wait(int timeout_milliseconds = TIME_MS_MAX);

private:
    zsemaphore _sema;
    std::atomic<bool> _signaled;
    bool _manualReset;
};
/*@}*/

class zauto_lock
{
public:
    zauto_lock(zlock &lock) : _lock(&lock) { _lock->lock(); }
    ~zauto_lock() { _lock->unlock(); }

private:
    zlock *_lock;
};

class zauto_read_lock
{
public:
    zauto_read_lock(zrwlock_nr &lock) : _lock(&lock) { _lock->lock_read(); }
    ~zauto_read_lock() { _lock->unlock_read(); }

private:
    zrwlock_nr *_lock;
};

class zauto_write_lock
{
public:
    zauto_write_lock(zrwlock_nr &lock) : _lock(&lock) { _lock->lock_write(); }
    ~zauto_write_lock() { _lock->unlock_write(); }

private:
    zrwlock_nr *_lock;
};
}
} // end namespace dsn::service
