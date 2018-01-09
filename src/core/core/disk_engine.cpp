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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "disk_engine.h"
#include <dsn/tool-api/perf_counter.h>
#include <dsn/tool-api/aio_provider.h>
#include <dsn/utility/utils.h>
#include <dsn/utility/transient_memory.h>

#ifdef __TITLE__
#undef __TITLE__
#endif
#define __TITLE__ "disk_engine"

using namespace dsn::utils;

namespace dsn {

DEFINE_TASK_CODE_AIO(LPC_AIO_BATCH_WRITE, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

//----------------- disk_file ------------------------
aio_task *disk_write_queue::unlink_next_workload(void *plength)
{
    uint64_t next_offset;
    uint32_t &sz = *(uint32_t *)plength;
    sz = 0;

    aio_task *first = _hdr._first, *current = first, *last = first;
    while (nullptr != current) {
        auto io = current->aio();
        if (sz == 0) {
            sz = io->buffer_size;
            next_offset = io->file_offset + sz;
        } else {
            // batch condition
            if (next_offset == io->file_offset && sz + io->buffer_size <= _max_batch_bytes) {
                sz += io->buffer_size;
                next_offset += io->buffer_size;
            }

            // no batch is possible
            else {
                break;
            }
        }

        // continue next
        last = current;
        current = (aio_task *)current->next;
    }

    // unlink [first, last] -> current
    if (last) {
        _hdr._first = current;
        if (last == _hdr._last)
            _hdr._last = nullptr;
        last->next = nullptr;
    }

    return first;
}

disk_file::disk_file(dsn_handle_t handle) : _handle(handle) {}

void disk_file::ctrl(dsn_ctrl_code_t code, int param)
{
    // TODO:
    dassert(false, "NOT IMPLEMENTED");
}

aio_task_ptr disk_file::read(const aio_task_ptr &tsk)
{
    // because we use aio, so we must ensure that releasing the task after the aio-task is completed
    // so, we should add_ref here and release_ref after completion
    // if you want erase this model, you should modify the _read_queue, make it can hold the
    // life-cycle of tsk(now, _read_queue is implemented through slist, it just a raw point and
    // can't control life-cycle)
    aio_task *raw_ptr = tsk.get();
    raw_ptr->add_ref(); // release on completion
    return _read_queue.add_work(raw_ptr, nullptr);
}

aio_task_ptr disk_file::write(const aio_task_ptr &tsk, uint32_t &sz)
{
    aio_task *raw_ptr = tsk.get();
    raw_ptr->add_ref(); // release on completion
    return _write_queue.add_work(raw_ptr, (void *)&sz);
}

aio_task_ptr disk_file::on_read_completed(const aio_task_ptr &wk, error_code err, size_t size)
{
    aio_task *raw_ptr = wk.get();
    dassert(raw_ptr->next == nullptr, "");
    aio_task_ptr ret = _read_queue.on_work_completed(raw_ptr, nullptr);
    raw_ptr->enqueue_aio(err, size);
    raw_ptr->release_ref(); // added in above read

    return ret;
}

aio_task_ptr
disk_file::on_write_completed(const aio_task_ptr &wk, void *ctx, error_code err, size_t size)
{
    aio_task *raw_ptr = wk.get();
    aio_task_ptr ret = _write_queue.on_work_completed(raw_ptr, ctx);

    while (raw_ptr) {
        aio_task *next = (aio_task *)raw_ptr->next;
        raw_ptr->next = nullptr;

        if (err == ERR_OK) {
            size_t this_size = (size_t)raw_ptr->aio()->buffer_size;
            dassert(size >= this_size,
                    "written buffer size does not equal to input buffer's size: %d vs %d",
                    (int)size,
                    (int)this_size);

            raw_ptr->enqueue_aio(err, this_size);
            size -= this_size;
        } else {
            raw_ptr->enqueue_aio(err, size);
        }

        raw_ptr->release_ref(); // added in above write

        raw_ptr = next;
    }

    if (err == ERR_OK) {
        dassert(size == 0, "written buffer size does not equal to input buffer's size");
    }

    return ret;
}

//----------------- disk_engine ------------------------
disk_engine::disk_engine(service_node *node)
{
    _is_running = false;
    _provider = nullptr;
    _node = node;
}

disk_engine::~disk_engine() {}

void disk_engine::start(aio_provider *provider)
{
    if (_is_running)
        return;

    _provider = provider;
    _provider->start();
    _is_running = true;
}

void disk_engine::ctrl(dsn_handle_t fh, dsn_ctrl_code_t code, int param)
{
    if (nullptr == fh)
        return;

    auto df = (disk_file *)fh;
    df->ctrl(code, param);
}

dsn_handle_t disk_engine::open(const char *file_name, int flag, int pmode)
{
    dsn_handle_t nh = _provider->open(file_name, flag, pmode);
    if (nh != DSN_INVALID_FILE_HANDLE) {
        return new disk_file(nh);
    } else {
        return nullptr;
    }
}

error_code disk_engine::close(dsn_handle_t fh)
{
    if (nullptr != fh) {
        auto df = (disk_file *)fh;
        auto ret = _provider->close(df->native_handle());
        delete df;
        return ret;
    } else {
        return ERR_INVALID_HANDLE;
    }
}

error_code disk_engine::flush(dsn_handle_t fh)
{
    if (nullptr != fh) {
        auto df = (disk_file *)fh;
        return _provider->flush(df->native_handle());
    } else {
        return ERR_INVALID_HANDLE;
    }
}

void disk_engine::read(const aio_task_ptr &aio)
{
    if (!_is_running) {
        aio->enqueue_aio(ERR_SERVICE_NOT_FOUND, 0);
        return;
    }

    if (!aio->spec().on_aio_call.execute(task::get_current_task(), aio, true)) {
        aio->enqueue_aio(ERR_FILE_OPERATION_FAILED, 0);
        return;
    }

    auto dio = aio->aio();
    auto df = (disk_file *)dio->file;
    dio->file = df->native_handle();
    dio->file_object = df;
    dio->engine = this;
    dio->type = AIO_Read;

    aio_task_ptr wk = df->read(aio);
    if (wk) {
        _provider->aio(wk);
    }
}

class batch_write_io_task : public aio_task
{
public:
    batch_write_io_task(aio_task *tasks, blob &buffer)
        : aio_task(LPC_AIO_BATCH_WRITE, nullptr), _tasks(tasks), _buffer(buffer)
    {
    }

    virtual void exec() override
    {
        auto df = (disk_file *)_tasks->aio()->file_object;
        uint32_t sz;

        auto wk = df->on_write_completed(_tasks, (void *)&sz, error(), _transferred_size);
        if (wk) {
            wk->aio()->engine->process_write(wk, sz);
        }
    }

public:
    aio_task *_tasks;
    blob _buffer;
};
typedef ::dsn::ref_ptr<batch_write_io_task> batch_write_io_task_ptr;

void disk_engine::write(const aio_task_ptr &aio)
{
    if (!_is_running) {
        aio->enqueue_aio(ERR_SERVICE_NOT_FOUND, 0);
        return;
    }

    if (!aio->spec().on_aio_call.execute(task::get_current_task(), aio, true)) {
        aio->enqueue_aio(ERR_FILE_OPERATION_FAILED, 0);
        return;
    }

    auto dio = aio->aio();
    auto df = (disk_file *)dio->file;
    dio->file = df->native_handle();
    dio->file_object = df;
    dio->engine = this;
    dio->type = AIO_Write;

    uint32_t sz = 0;
    aio_task_ptr wk = df->write(aio, sz);
    if (wk) {
        process_write(wk, sz);
    }
}

void disk_engine::process_write(const aio_task_ptr &aio, uint32_t sz)
{
    // no batching
    if (aio->aio()->buffer_size == sz) {
        aio->collapse();
        return _provider->aio(aio);
    }

    // batching
    else {
        // merge the buffers
        auto bb = tls_trans_mem_alloc_blob((size_t)sz);
        char *ptr = (char *)bb.data();
        auto current_wk = aio.get();
        do {
            current_wk->copy_to(ptr);
            ptr += current_wk->aio()->buffer_size;
            current_wk = (aio_task *)current_wk->next;
        } while (current_wk);

        dassert(ptr == (char *)bb.data() + bb.length(),
                "ptr = %" PRIu64 ", bb.data() = %" PRIu64 ", bb.length = %u",
                (uint64_t)(ptr),
                (uint64_t)(bb.data()),
                bb.length());

        // setup io task
        batch_write_io_task_ptr new_task(new batch_write_io_task(aio, bb));
        auto dio = new_task->aio();
        dio->buffer = (void *)bb.data();
        dio->buffer_size = sz;
        dio->file_offset = aio->aio()->file_offset;

        dio->file = aio->aio()->file;
        dio->file_object = aio->aio()->file_object;
        dio->engine = aio->aio()->engine;
        dio->type = AIO_Write;

        new_task->add_ref(); // released in complete_io
        return _provider->aio(new_task);
    }
}

void disk_engine::complete_io(const aio_task_ptr &aio,
                              error_code err,
                              uint32_t bytes,
                              int delay_milliseconds)
{
    if (err != ERR_OK) {
        dinfo("disk operation failure with code %s, err = %s, aio_task_id = %016" PRIx64,
              aio->spec().name.c_str(),
              err.to_string(),
              aio->id());
    }

    // batching
    if (aio->code() == LPC_AIO_BATCH_WRITE) {
        aio->enqueue_aio(err, (size_t)bytes);
        aio->release_ref(); // added in process_write
    }

    // no batching
    else {
        auto df = (disk_file *)(aio->aio()->file_object);
        if (aio->aio()->type == AIO_Read) {
            auto wk = df->on_read_completed(aio, err, (size_t)bytes);
            if (wk) {
                _provider->aio(wk);
            }
        }

        // write
        else {
            uint32_t sz;
            aio_task_ptr wk = df->on_write_completed(aio, (void *)&sz, err, (size_t)bytes);
            if (wk) {
                process_write(wk, sz);
            }
        }
    }
}

} // end namespace
