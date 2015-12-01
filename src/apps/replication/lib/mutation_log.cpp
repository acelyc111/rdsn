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


#include "mutation_log.h"
#ifdef _WIN32
#include <io.h>
#endif

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "mutation_log"

namespace dsn { namespace replication {

    using namespace ::dsn::service;

mutation_log::mutation_log(
    const std::string& dir,
    bool is_private,
    uint32_t batch_buffer_size_kb,
    uint32_t max_log_file_mb
    )
{
    _dir = dir;
    _max_log_file_size_in_bytes = static_cast<int64_t>(max_log_file_mb) * 1024L * 1024L;
    _batch_buffer_bytes = batch_buffer_size_kb * 1024;
    _is_private = is_private;

    init_states();
}

void mutation_log::init_states()
{
    // memory states
    _is_opened = false;

    // logs
    _global_start_offset = 0;
    _global_end_offset = 0;
    _last_file_number = 0;
    _log_files.clear();
    _current_log_file = nullptr;

    // buffering and replica states
    _pending_write = nullptr;
    _pending_write_callbacks = nullptr;
    _private_gpid.app_id = 0;
    _private_gpid.pidx = 0;
    _private_max_decree = 0;
    _private_valid_start_offset = 0;
}

mutation_log::~mutation_log()
{
    close();
}

void mutation_log::set_valid_log_offset_before_open(global_partition_id gpid, int64_t valid_start_offset)
{
    zauto_lock l(_lock);

    if (_is_private)
    {
        _private_gpid = gpid;
        _private_valid_start_offset = valid_start_offset;
    }
    else
    {
        _shared_max_decrees[gpid] = log_replica_info(0, valid_start_offset);
    }
}

error_code mutation_log::open(replay_callback callback)
{
    error_code err;
    dassert(!_is_opened, "cannot open a opened mutation_log");
    dassert(nullptr == _current_log_file,
        "the current log file must be null at this point");


    // create dir if necessary
    if (!dsn::utils::filesystem::path_exists(_dir))
    {
        if (!dsn::utils::filesystem::create_directory(_dir))
        {
            derror("open mutation_log: create log path failed");
            return ERR_FILE_OPERATION_FAILED;
        }
    }
    
    // load the existing logs
    _log_files.clear();

    std::vector<std::string> file_list;
    if (!dsn::utils::filesystem::get_subfiles(_dir, file_list, false))
    {
        derror("open mutation_log: get files failed.");
        return ERR_FILE_OPERATION_FAILED;
    }

    if (nullptr == callback)
    {
        dassert(file_list.size() == 0, "log must be empty if callback is not present");
    }

    std::sort(file_list.begin(), file_list.end());

    for (auto& fpath : file_list)
    {
        log_file_ptr log = log_file::open_read(fpath.c_str(), err);
        if (log == nullptr)
        {
            if (err == ERR_HANDLE_EOF || err == ERR_INCOMPLETE_DATA || err == ERR_INVALID_PARAMETERS)
            {
                dwarn("Skip file %s during log init", fpath.c_str());
                continue;
            }
            else
            {
                return err;
            }
        }

        ddebug("open log file %s succeed", fpath.c_str());

        dassert(_log_files.find(log->index()) == _log_files.end(), "");
        _log_files[log->index()] = log;
    }
    file_list.clear();

    // replay with the found files
    int64_t offset = 0;
    err = replay(
        _log_files,
        [this, callback](mutation_ptr& mu)
        {
            bool ret = true;

            if (callback)
            {
                ret = callback(mu);
            }

            if (ret)
            {
                this->update_max_decrees(mu->data.header.gpid, mu->data.header.decree);
            }
            return ret;
        },
        offset
        );

    if (ERR_OK == err)
    {
        _global_start_offset = _log_files.size() > 0 ? _log_files.begin()->second->start_offset() : 0;
        _global_end_offset = offset;
        _last_file_number = _log_files.size() > 0 ? _log_files.rbegin()->first : 0;
    }

    _is_opened = (err == ERR_OK);
    return err;
}

error_code mutation_log::open(
    global_partition_id gpid,
    replay_callback callback,
    decree max_decree /*= invalid_decree*/)
{
    dassert(_is_private, "this is only valid for private mutation log");
    if (_private_gpid.app_id != 0)
    {
        dassert(_private_gpid == gpid, "invalid gpid");
    }
    else
        _private_gpid = gpid;

    if (max_decree != invalid_decree)
        _private_max_decree = max_decree;

    return open(callback);
}

void mutation_log::close(bool clear_all)
{
    {
        zauto_lock l(_lock);
        // event it is not opened, we still need to
        // go through the following logics, e.g., 
        // clear all logs as open failed.

       /* if (!_is_opened)
            return;*/

        _is_opened = false;

        if (_pending_write)
        {
            write_pending_mutations(false);
            _pending_write = nullptr;
        }
    }

    dinfo("close mutation log %s, clear_all = %s",
        dir().c_str(),
        clear_all ? "true" : "false"
        );

    // make sure all issued aios are completed
    dsn_task_tracker_wait_all(tracker());

    // close last log file
    if (nullptr != _current_log_file)
    {
        _current_log_file->close();
        _current_log_file = nullptr;
    }

    if (clear_all)
    {
        for (auto& kv : _log_files)
        {
            kv.second->close();
        }
        _log_files.clear();

        auto lerr = utils::filesystem::remove_path(dir());
        dassert(lerr, "remove log %s failed", dir().c_str());
    }

    // reset all states
    init_states();
}


error_code mutation_log::create_new_log_file()
{
    if (_current_log_file != nullptr)
    {
        dassert (_current_log_file->end_offset() == _global_end_offset, "");
    }

    log_file_ptr logf = log_file::create_write(
        _dir.c_str(),
        _last_file_number + 1,
        _global_end_offset
        );

    if (logf == nullptr)
    {
        derror ("cannot create log file with index %d", _last_file_number);
        return ERR_FILE_OPERATION_FAILED;
    }

    ddebug ("create new log file %s succeed", logf->path().c_str());

    _last_file_number++;
    dassert (_log_files.find(_last_file_number) == _log_files.end(), "");
    _log_files[_last_file_number] = logf;

    dassert (logf->end_offset() == logf->start_offset(), "");
    dassert (_global_end_offset == logf->end_offset(), "");

    _current_log_file = logf;

    create_new_pending_buffer();

    int len = 0;
    if (_is_private)
    {
        multi_partition_decrees_ex ds;
        ds[_private_gpid] = log_replica_info(_private_max_decree, _private_valid_start_offset);
        binary_writer temp_writer;
        len = logf->write_header(
            temp_writer,
            ds,
            static_cast<int>(_batch_buffer_bytes)
            );
        _pending_write->add(temp_writer.get_buffer());
    }
    else
    {
        binary_writer temp_writer;
        len = logf->write_header(
            temp_writer,
            _shared_max_decrees,
            static_cast<int>(_batch_buffer_bytes)
            );
        _pending_write->add(temp_writer.get_buffer());
    }

    _global_end_offset += len;
    dassert (_pending_write->size() == len + sizeof(log_block_header), "");
    return ERR_OK;
}

void mutation_log::create_new_pending_buffer()
{
    dassert (_pending_write == nullptr, "");
    dassert (_pending_write_callbacks == nullptr, "");

    _pending_write_callbacks.reset(new std::list< ::dsn::task_ptr>);
    _pending_write = _current_log_file->prepare_log_block();
    _global_end_offset += _pending_write->data().front().length();
}

error_code mutation_log::write_pending_mutations(bool create_new_log_when_necessary)
{
    dassert (_pending_write != nullptr, "");
    dassert (_pending_write_callbacks != nullptr, "");

    // write block header

    uint64_t offset = _global_end_offset - _pending_write->size();
    bool new_log_file = create_new_log_when_necessary
        && (_global_end_offset - _current_log_file->start_offset()
        >= _max_log_file_size_in_bytes)
        ;

    auto aio = _current_log_file->commit_log_block(
        *_pending_write,
        offset,
        LPC_WRITE_REPLICATION_LOG,
        this,
        std::bind(
            &mutation_log::internal_write_callback,
            std::placeholders::_1,
            std::placeholders::_2,
            _pending_write_callbacks, _pending_write, _current_log_file->crc32()),
        -1
        );

    if (aio == nullptr)
    {
        internal_write_callback(ERR_FILE_OPERATION_FAILED,
            0, _pending_write_callbacks, _pending_write, _current_log_file->crc32());
    }
    else
    {
        dassert (_global_end_offset == _current_log_file->end_offset(), "");
    }

    _pending_write = nullptr;
    _pending_write_callbacks = nullptr;

    dassert(nullptr != aio, "");

    if (new_log_file)
    {
        error_code ret = create_new_log_file();
        if (ret != ERR_OK)
        {
            dinfo ("create new log file failed, err = %s", ret.to_string());
        }
        return ret;
    }

    return ERR_OK;
}

void mutation_log::internal_write_callback(
    error_code err,
    size_t size,
    mutation_log::pending_callbacks_ptr callbacks,
    std::shared_ptr<log_block> block,
    uint32_t init_crc32
    )
{
    auto hdr = (log_block_header*)block->front().data();
    dassert(hdr->magic == 0xdeadbeef, "header magic is changed: 0x%x", hdr->magic);


    for (auto log_iter = std::next(block->data().begin()); log_iter != block->data().end(); ++log_iter)
    {
        init_crc32 = dsn_crc32_compute(
            static_cast<const void*>(log_iter->data()),
            static_cast<size_t>(log_iter->length()), init_crc32
            );
    }
    dassert(init_crc32 == hdr->body_crc, "");

    if (err == ERR_OK)
    {
        dassert(static_cast<int>(size) == block->size(),
            "log write size must equal to the given size: %d vs %d",
            (int)size,
            block->size()
            );
    }

    for (auto& cb : *callbacks)
    {
        cb->enqueue_aio(err, size);
    }
}

/*static*/ error_code mutation_log::replay(
    log_file_ptr log,
    replay_callback callback,
    /*out*/ int64_t& offset
    )
{
    offset = log->start_offset();
    dinfo("replay mutation log %s: offset = [%" PRId64 ", %" PRId64 ")",
        log->path().c_str(),
        log->start_offset(),
        log->end_offset()
        );

    ::dsn::blob bb;
    log->crc32() = 0;
    error_code err = log->read_next_log_block(0, bb);
    if (err != ERR_OK)
    {
        return err;
    }

    std::shared_ptr<binary_reader> reader(new binary_reader(bb));
    offset += sizeof(log_block_header);
    offset += log->read_header(*reader);
    if (!log->is_right_header())
    {
        return ERR_INVALID_DATA;
    }

    while (true)
    {
        while (!reader->is_eof())
        {
            auto old_size = reader->get_remaining_size();
            mutation_ptr mu = mutation::read_from(*reader, nullptr);
            dassert(nullptr != mu, "");
            mu->set_logged();

            if (mu->data.header.log_offset != offset)
            {
                derror("offset mismatch in log entry and mutation %" PRId64 " vs %" PRId64,
                    offset, mu->data.header.log_offset);
                err = ERR_INVALID_DATA;
                break;
            }

            callback(mu);

            offset += old_size - reader->get_remaining_size();
        }

        /*
         * if an error in an log mutation block, then the replay log is
         * stopped
         */
        err = log->read_next_log_block(offset - log->start_offset(), bb);
        if (err != ERR_OK)
        {
            break;
        }

        reader.reset(new binary_reader(bb));
        offset += sizeof(log_block_header);
    }

    return err;
}

/*static*/ error_code mutation_log::replay(
    std::vector<std::string>& log_files,
    replay_callback callback,
    /*out*/ int64_t& offset
    )
{
    std::map<int, log_file_ptr> logs;
    for (auto& fpath : log_files)
    {
        error_code err;
        log_file_ptr log = log_file::open_read(fpath.c_str(), err);
        if (log == nullptr)
        {
            if (err == ERR_HANDLE_EOF || err == ERR_INCOMPLETE_DATA || err == ERR_INVALID_PARAMETERS)
            {
                dinfo("Skip file %s during log replay", fpath.c_str());
                continue;
            }
            else
            {
                return err;
            }
        }

        dassert(logs.find(log->index()) == logs.end(), "");
        logs[log->index()] = log;
    }

    return replay(logs, callback, offset);
}

/*static*/ error_code mutation_log::replay(
    std::map<int, log_file_ptr>& logs,
    replay_callback callback,
    /*out*/ int64_t& offset
    )
{
    int64_t g_start_offset = 0, g_end_offset = 0;
    error_code err = ERR_OK;
    std::shared_ptr<binary_reader> reader;
    log_file_ptr last;
    int last_file_number = 0;

    if (logs.size() > 0)
    {
        g_start_offset = logs.begin()->second->start_offset();
        g_end_offset = logs.rbegin()->second->end_offset();
        last_file_number = logs.begin()->first - 1;
    }

    for (auto& kv : logs)
    {
        if (++last_file_number != kv.first)
        {
            derror("log file missing with index %u", last_file_number);
            return ERR_OBJECT_NOT_FOUND;
        }
    }

    offset = g_start_offset;

    for (auto& kv : logs)
    {
        log_file_ptr& log = kv.second;

        if (log->start_offset() != offset)
        {
            derror("offset mismatch in log file offset and global offset %" PRId64 " vs %" PRId64,
                log->start_offset(), offset);
            return ERR_INVALID_DATA;
        }

        last = log;
        err = mutation_log::replay(log, callback, offset);

        log->close();

        if (err == ERR_INVALID_DATA || err == ERR_FILE_OPERATION_FAILED)
            break;
        else
        {
            // for other error codes, they are all checked by next loop or after loop
            /*
             * If the file is not corrupted, it may also return the value of ERR_INCOMPLETE_DATA
             * or ERR_HANDLE_EOF.
             * In this case, the correctness is relying on the check of start_offset.
             */
            dassert(err == ERR_OK
                || err == ERR_INCOMPLETE_DATA
                || err == ERR_HANDLE_EOF
                ,
                "unhandled error code: %s",
                err.to_string()
                );
        }
    }

    if (err == ERR_OK)
    {
        dassert(g_end_offset == offset,
            "make sure the global end offset is correct: %" PRId64 " vs %" PRId64,
            g_end_offset,
            offset
            );
    }
    else if (err == ERR_HANDLE_EOF || err == ERR_INCOMPLETE_DATA)
    {
        err = ERR_OK;
        // fix end offset so later log writing can be continued
        g_end_offset = offset;
    }
    else
    {
        // data failure
        derror("replay mutation log failed: %s", err.to_string());
    }

    return err;
}

decree mutation_log::max_decree(global_partition_id gpid) const
{
    zauto_lock l(_lock);
    if (_is_private)
        return _private_max_decree;
    else
    {
        auto it = _shared_max_decrees.find(gpid);
        if (it != _shared_max_decrees.end())
            return it->second.decree;
        else
            return 0;
    }
}

void mutation_log::check_log_start_offset(global_partition_id gpid, int64_t valid_start_offset) const
{
    zauto_lock l(_lock);

    if (_is_private)
    {
        dassert(valid_start_offset == _private_valid_start_offset,
            "valid start offset mismatch: %" PRId64 " vs %" PRId64,
            valid_start_offset,
            _private_valid_start_offset
            );
    }
    else
    {
        auto it = _shared_max_decrees.find(gpid);
        if (it != _shared_max_decrees.end())
        {
            dassert(valid_start_offset == it->second.log_start_offset,
                "valid start offset mismatch: %" PRId64 " vs %" PRId64,
                valid_start_offset,
                it->second.log_start_offset
                );
        }
    }
}

decree mutation_log::max_gced_decree(global_partition_id gpid, int64_t valid_start_offset) const
{
    check_log_start_offset(gpid, valid_start_offset);

    zauto_lock l(_lock);

    if (_log_files.size() == 0)
    {
        if (_is_private)
            return _private_max_decree;
        else
        {
            auto it = _shared_max_decrees.find(gpid);
            if (it != _shared_max_decrees.end())
                return it->second.decree;
            else
                return 0;
        }
    }
    else
    {
        for (auto& log : _log_files)
        {
            // when invalid log exits, all new logs are preserved (not gced)
            if (valid_start_offset > log.second->start_offset())
                return 0;

            auto it = log.second->previous_log_max_decrees().find(gpid);
            if (it != log.second->previous_log_max_decrees().end())
            {
                return it->second.decree;
            }
            else
                return 0;
        }
        return 0;
    }
}

void mutation_log::update_max_decrees(global_partition_id gpid, decree d)
{
    if (!_is_private)
    {
        auto it = _shared_max_decrees.find(gpid);
        if (it != _shared_max_decrees.end())
        {
            if (it->second.decree < d)
            {
                it->second.decree = d;
            }
        }
        /*else if (!_is_opened)
        {
            _shared_max_decrees[gpid] = log_replica_info(d, _global_end_offset);
        }*/
        else
        {
            dassert(false, "replica has not been registered in the log before");
        }
    }
    else
    {
        dassert(gpid == _private_gpid, "replica id does not match");
        if (d > _private_max_decree)
            _private_max_decree = d;
    }
}

::dsn::task_ptr mutation_log::append(
                        mutation_ptr& mu,
                        dsn_task_code_t callback_code,
                        clientlet* callback_host,
                        aio_handler callback,
                        int hash
                        )
{
    auto d = mu->data.header.decree;
    error_code err = ERR_OK;

    dinfo("write replication log for mutation %s", mu->name());

    zauto_lock l(_lock);
    dassert(_is_opened, "");

    if (nullptr == _current_log_file)
    {
        err = create_new_log_file();
        dassert(
            ERR_OK == err,
            "create write log file failed, err = %s",
            err.to_string()
            );
    }

    // update meta data
    update_max_decrees(mu->data.header.gpid, d);

    //
    // write to buffer
    //
    if (_pending_write == nullptr)
    {
        create_new_pending_buffer();
    }

    mu->data.header.log_offset = _global_end_offset;
    mu->write_to_scatter([this](blob bb)
    {
        _pending_write->add(bb);
        _global_end_offset += bb.length();
    });

    task_ptr tsk = nullptr;
    if (callback)
    {
        tsk = new safe_task<aio_handler>(callback);
        tsk->add_ref(); // released in exec_aio
        dsn_task_t t = dsn_file_create_aio_task(callback_code,
            safe_task<aio_handler>::exec_aio, tsk, hash, callback_host->tracker());
        tsk->set_task_info(t);
        _pending_write_callbacks->push_back(tsk);
    }

    //
    // start to write
    //
    //err = write_pending_mutations();
    // not batching
    
    if (_batch_buffer_bytes == 0)
    {
        err = write_pending_mutations();
    }
    // batching
    else
    {
        if (static_cast<uint32_t>(_pending_write->size()) >= _batch_buffer_bytes)
        {
            err = write_pending_mutations();
        }
        else
        {
            // waiting for batch write later
        }
    }
    

    dassert(
        err == ERR_OK,
        "write pending mutation failed, err = %s",
        err.to_string()
        );

    return tsk;
}

void mutation_log::on_partition_removed(global_partition_id gpid)
{
    dassert(!_is_private, "this method is only valid for shared logs");

    zauto_lock l(_lock);
    _shared_max_decrees.erase(gpid);
}

int64_t mutation_log::on_partition_reset(global_partition_id gpid, decree max_d)
{
    zauto_lock l(_lock);
    if (_is_private)
    {
        dassert(_private_gpid == gpid || _private_gpid.app_id == 0, "invalid gpid parameter");
        _private_gpid = gpid;
        _private_max_decree = max_d;
        _private_valid_start_offset = _global_end_offset;
    }
    else
    {
        log_replica_info info(max_d, _global_end_offset);

        auto it = _shared_max_decrees.insert(multi_partition_decrees_ex::value_type(gpid, info));
        if (!it.second)
        {
            dwarn("replica %d.%d has shifted its max decree from %" PRId64 " to %" PRId64 ", valid_start_offset from %" PRId64 " to %" PRId64,
                gpid.app_id, gpid.pidx,
                it.first->second.decree, info.decree,
                it.first->second.log_start_offset, info.log_start_offset
                );
            _shared_max_decrees[gpid] = info;
        }
    }
    return _global_end_offset;
}

void mutation_log::get_checkpoint(
    global_partition_id gpid,
    ::dsn::replication::decree start,
    /*out*/ ::dsn::replication::learn_state& state
    ) const
{
    dassert(_is_private, "this method is only valid for private logs");

    std::map<int, log_file_ptr> files;
    std::map<int, log_file_ptr>::reverse_iterator itr;
    log_file_ptr cfile = nullptr;

    {
        zauto_lock l(_lock);
        if (start > _private_max_decree)
            return;

        files = _log_files;
        cfile = _current_log_file;

        // also learn pending buffers
        if (_pending_write)
        {
            binary_writer temp_writer;
            for (auto& bb : _pending_write->data())
            {
                temp_writer.write(bb.data(), bb.length());
            }
            auto bb = temp_writer.get_buffer();
            auto hdr = (log_block_header*)bb.data();

            if (hdr->local_offset == 0)
            {
                bb = bb.range(cfile->get_file_header_size() + sizeof(log_block_header));
            }
            else
            {
                bb = bb.range(sizeof(log_block_header));
            }

            state.meta.push_back(bb);
        }
    }

    // flush last file so learning can learn the on-disk state
    if (nullptr != cfile) cfile->flush();

    // find all applicable files
    bool skip_next = false;
    std::list<std::string> learn_files;
    for (itr = files.rbegin(); itr != files.rend(); ++itr)
    {
        log_file_ptr& log = itr->second;
        if (log->end_offset() <= _private_valid_start_offset)
            break;

        if (skip_next)
        {
            skip_next = (log->previous_log_max_decrees().size() == 0);
            continue;
        }

        if (log->end_offset() > log->start_offset())
            learn_files.push_back(log->path());

        skip_next = (log->previous_log_max_decrees().size() == 0);
        if (skip_next)
            continue;

        decree last_max_decree = log->previous_log_max_decrees().begin()->second.decree;

        // when all possible decress are not needed
        if (last_max_decree < start)
        {
            // skip all older logs
            break;
        }
    }

    // reverse the order
    for (auto it = learn_files.rbegin(); it != learn_files.rend(); ++it)
    {
        state.files.push_back(*it);
    }
}

int mutation_log::garbage_collection(
    global_partition_id gpid,
    decree durable_d,
    int64_t valid_start_offset
    )
{
    dassert(_is_private, "this method is only valid for private logs");

    std::map<int, log_file_ptr> files;
    std::map<int, log_file_ptr>::reverse_iterator itr;
    decree max_decree;
    int current_file_index = -1;

    {
        zauto_lock l(_lock);
        files = _log_files;
        if (_current_log_file != nullptr)
            current_file_index = _current_log_file->index();
        max_decree = _private_max_decree;
    }

    for (itr = files.rbegin(); itr != files.rend(); ++itr)
    {
        // log is invalid, ok to delete
        if (valid_start_offset >= itr->second->end_offset())
        {
            dinfo("gc @ %d.%d: max_offset for %s is %" PRId64 " vs %" PRId64 " as app.valid_start_offset.private,"
                " safe to delete this and all older logs",
                _private_gpid.app_id,
                _private_gpid.pidx,
                itr->second->path().c_str(),
                itr->second->end_offset(),
                valid_start_offset
                );
            break;
        }

        // all decrees are durable, ok to delete
        else if (durable_d >= max_decree)
        {
            dinfo("gc @ %d.%d: max_decree for %s is %" PRId64 " vs %" PRId64 " as app.durable decree,"
                " safe to delete this and all older logs",
                _private_gpid.app_id,
                _private_gpid.pidx,
                itr->second->path().c_str(),
                max_decree,
                durable_d
                );
            break;
        }

        // update max decree for next log file
        log_file_ptr& log = itr->second;
        auto it3 = log->previous_log_max_decrees().find(gpid);
        dassert(it3 != log->previous_log_max_decrees().end(),
            "this is impossible for private logs");
        max_decree = it3->second.decree;
    }

    int count = 0;
    for (; itr != files.rend(); ++itr)
    {
        // skip current file
        if (current_file_index == itr->second->index())
            continue;

        itr->second->close();

        auto& fpath = itr->second->path();
        if (!dsn::utils::filesystem::remove_path(fpath))
        {
            derror("gc: fail to remove %s, stop current gc cycle ...", fpath.c_str());
            break;
        }

        ddebug("gc: log segment %s is removed", fpath.c_str());
        count++;
        {
            zauto_lock l(_lock);
            _log_files.erase(itr->first);
        }
    }

    return count;
}

int mutation_log::garbage_collection(multi_partition_decrees_ex& durable_decrees)
{
    dassert(!_is_private,
        "please call garbage_collection instead");

    std::map<int, log_file_ptr> files;
    std::map<int, log_file_ptr>::reverse_iterator itr;
    multi_partition_decrees_ex max_decrees;
    int current_file_index = -1;

    {
        zauto_lock l(_lock);
        files = _log_files;
        if (_current_log_file != nullptr)
            current_file_index = _current_log_file->index();
        max_decrees = _shared_max_decrees;
    }

    for (itr = files.rbegin(); itr != files.rend(); ++itr)
    {
        log_file_ptr& log = itr->second;

        // check for gc ok for?
        bool delete_ok = true;
        for (auto& kv : durable_decrees)
        {
            global_partition_id gpid = kv.first;
            decree durable_decree = kv.second.decree;
            int64_t valid_start_offset = kv.second.log_start_offset;

            bool delete_ok_for_this_replica;
            auto it3 = max_decrees.find(gpid);
            if (it3 == max_decrees.end())
            {
                dassert(valid_start_offset >= log->end_offset(),
                    "valid start offset must be greater than the end of this log file");

                dinfo("gc @ %d.%d: max_decree for %s is missing vs %" PRId64 " as app.durable decree,"
                    " safe to delete this and all older logs for this replica",
                    gpid.app_id,
                    gpid.pidx,
                    itr->second->path().c_str(),
                    durable_decree
                    );

                delete_ok_for_this_replica = true;
            }
            else
            {
                // log is valid, and all decrees are durable, ok to delete
                if (valid_start_offset >= log->end_offset())
                {
                    delete_ok_for_this_replica = true;

                    dinfo("gc @ %d.%d: log is invalid for %s, as"
                        " valid start offset vs log end offset = %" PRId64 " vs %" PRId64 ", "
                        " it is therefore safe to delete this and all older logs for this replica",
                        gpid.app_id,
                        gpid.pidx,
                        itr->second->path().c_str(),
                        valid_start_offset,
                        log->end_offset()
                        );
                }
                else if (durable_decree >= it3->second.decree)
                {
                    delete_ok_for_this_replica = true;

                    dinfo("gc @ %d.%d: max_decree for %s is %" PRId64 " vs %" PRId64 " as app.durable decree,"
                        " it is therefore safe to delete this and all older logs for this replica",
                        gpid.app_id,
                        gpid.pidx,
                        itr->second->path().c_str(),
                        it3->second,
                        durable_decree
                        );
                }
                else
                    delete_ok_for_this_replica = false;
            }

            if (delete_ok_for_this_replica == false)
            {
                delete_ok = false;
                break;
            }
        }

        if (delete_ok)
        {
            dinfo("gc %s and older files is ok as all replica agrees",
                itr->second->path().c_str()
                );
            break;
        }

        // update max_decrees for the next log file
        for (auto it = max_decrees.begin(); it != max_decrees.end();)
        {
            auto it3 = log->previous_log_max_decrees().find(it->first);
            if (it3 != log->previous_log_max_decrees().end())
            {
                it->second = it3->second;
                ++it;
            }
            else
            {
                it = max_decrees.erase(it);
            }
        }
    }

    int count = 0;
    for (; itr != files.rend(); ++itr)
    {
        // skip current file
        if (current_file_index == itr->second->index())
            continue;

        itr->second->close();

        auto& fpath = itr->second->path();
        if (!dsn::utils::filesystem::remove_path(fpath))
        {
            derror("gc: fail to remove %s, stop current gc cycle ...", fpath.c_str());
            break;
        }

        ddebug("gc: log segment %s is removed", fpath.c_str());
        count++;
        {
            zauto_lock l(_lock);
            _log_files.erase(itr->first);
        }
    }

    return count;
}

//------------------- log_file --------------------------
/*static */log_file_ptr log_file::open_read(const char* path, /*out*/ error_code& err)
{
    std::string pt = std::string(path);
    char splitters[] = { '\\', '/', 0 };
    std::string name = utils::get_last_component(pt, splitters);

    // log.index.start_offset
    if (name.length() < strlen("log.")
        || name.substr(0, strlen("log.")) != std::string("log."))
    {
        err = ERR_INVALID_PARAMETERS;
        dwarn("invalid log path %s", path);
        return nullptr;
    }

    auto pos = name.find_first_of('.');
    dassert(pos != std::string::npos, "");
    auto pos2 = name.find_first_of('.', pos + 1);
    if (pos2 == std::string::npos)
    {
        err = ERR_INVALID_PARAMETERS;
        dwarn("invalid log path %s", path);
        return nullptr;
    }

    /* so the log file format is log.index_str.start_offset_str */
    std::string index_str = name.substr(pos + 1, pos2 - pos - 1);
    std::string start_offset_str = name.substr(pos2 + 1);
    if (index_str.empty() || start_offset_str.empty())
    {
        err = ERR_INVALID_PARAMETERS;
        dwarn("invalid log path %s", path);
        return nullptr;
    }

    char* p = nullptr;
    int index = static_cast<int>(strtol(index_str.c_str(), &p, 10));
    if (*p != 0)
    {
        err = ERR_INVALID_PARAMETERS;
        dwarn("invalid log path %s", path);
        return nullptr;
    }
    int64_t start_offset = static_cast<int64_t>(strtoll(start_offset_str.c_str(), &p, 10));
    if (*p != 0)
    {
        err = ERR_INVALID_PARAMETERS;
        dwarn("invalid log path %s", path);
        return nullptr;
    }

    dsn_handle_t hfile = dsn_file_open(path, O_RDONLY | O_BINARY, 0);
    if (!hfile)
    {
        err = ERR_FILE_OPERATION_FAILED;
        dwarn("open log file %s failed", path);
        return nullptr;
    }

    /*
     * so log file is structured with sequences of log_blocks
     * each block consists of the log_block_header + log_content
     * and the first block is the log_file_header
     */
    auto lf = new log_file(path, hfile, index, start_offset, true);
    blob hdr_blob;
    err = lf->read_next_log_block(0, hdr_blob);
    if (err == ERR_INVALID_DATA || err == ERR_INCOMPLETE_DATA || err == ERR_HANDLE_EOF)
    {
        std::string removed = std::string(path) + ".removed";
        derror("read first log entry of file %s failed, err = %s. Rename the file to %s", path, err.to_string(), removed.c_str());
        delete lf;
        dsn::utils::filesystem::rename_path(path, removed);
        return nullptr;
    }
    else if (err != ERR_OK)
    {
        dassert(false, "unexpected error type");
    }

    binary_reader reader(hdr_blob);
    lf->read_header(reader);
    if (!lf->is_right_header())
    {
        err = ERR_INVALID_DATA;
        std::string removed = std::string(path) + ".removed";
        derror("invalid log file header of file %s. Rename the file to %s", path, removed.c_str());
        delete lf;
        dsn::utils::filesystem::rename_path(path, removed);
        return nullptr;
    }

    err = ERR_OK;
    return lf;
}

/*static*/ log_file_ptr log_file::create_write(
    const char* dir,
    int index,
    int64_t start_offset
    )
{
    char path[512]; 
    sprintf (path, "%s/log.%d.%" PRId64, dir, index, start_offset);

    if (dsn::utils::filesystem::path_exists(std::string(path)))
    {
        dwarn("log file %s already exist", path);
        return nullptr;
    }

    dsn_handle_t hfile = dsn_file_open(path, O_RDWR | O_CREAT | O_BINARY, 0666);
    if (!hfile)
    {
        dwarn("create log %s failed", path);
        return nullptr;
    }

    return new log_file(path, hfile, index, start_offset, false);
}

log_file::log_file(
    const char* path,
    dsn_handle_t handle,
    int index,
    int64_t start_offset,
    bool is_read
    )
{
    _start_offset = start_offset;
    _end_offset = start_offset;
    _handle = handle;
    _is_read = is_read;
    _path = path;
    _index = index;
    _crc32 = 0;
    memset(&_header, 0, sizeof(_header));

    if (is_read)
    {
        int64_t sz;
        if (!dsn::utils::filesystem::file_size(_path, sz))
        {
            dassert(false, "fail to get file size of %s.", _path.c_str());
        }
        _end_offset += sz;
    }
}

void log_file::close()
{
    if (_handle)
    {
        error_code err = dsn_file_close(_handle);
        dassert(
            err == ERR_OK,
            "dsn_file_close failed, err = %s",
            err.to_string()
            );

        _handle = nullptr;
    }
}

void log_file::flush() const
{
    dassert (!_is_read, "log file must be of write mode");

    if (_handle)
    {
        error_code err = dsn_file_flush(_handle);
        dassert(
            err == ERR_OK,
            "dsn_file_flush failed, err = %s",
            err.to_string()
            );
    }
}

error_code log_file::read_next_log_block(int64_t local_offset, /*out*/::dsn::blob& bb)
{
    /*
     * so the structure of log block: log_block_header + log_content
     * and the length_of(log_content) == log_block_header.length
     * and log_block_header.crc is stored for the check of lock_content
     */
    dassert (_is_read, "log file must be of read mode");

    log_block_header hdr;
    auto tsk = file::read(_handle, reinterpret_cast<char*>(&hdr), static_cast<int>(sizeof(log_block_header)),
        local_offset, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr);
    tsk->wait();

    auto read_count = tsk->io_size();
    auto err = tsk->error();
    if (err != ERR_OK || read_count != sizeof(log_block_header))
    {
        if (err == ERR_OK || err == ERR_HANDLE_EOF)
        {
            err = read_count == 0 ? ERR_HANDLE_EOF : ERR_INCOMPLETE_DATA;
        }
        else
        {
            derror("read data block header failed, size = %d vs %d, err = %s, local_offset = %" PRId64,
                read_count, (int)sizeof(log_block_header), err.to_string(), local_offset);
        }

        return err;
    }

    if (hdr.magic != 0xdeadbeef)
    {
        derror("invalid data header magic: 0x%x", hdr.magic);
        return ERR_INVALID_DATA;
    }

    std::shared_ptr<char> data(new char[hdr.length]);
    bb.assign(data, 0, hdr.length);

    tsk = file::read(_handle, const_cast<char*>(bb.data()), static_cast<int>(hdr.length),
        local_offset + read_count, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr);
    tsk->wait();

    read_count = tsk->io_size();
    err = tsk->error();
    if (err != ERR_OK || hdr.length != read_count)
    {
        derror("read data block body failed, size = %d vs %d, err = %s, local_offset = %" PRId64,
            read_count, (int)hdr.length, err.to_string(), local_offset);

        if (err == ERR_OK || err == ERR_HANDLE_EOF)
            err = ERR_INCOMPLETE_DATA;

        return err;
    }

    auto crc = dsn_crc32_compute(static_cast<const void*>(bb.data()), static_cast<size_t>(hdr.length), _crc32);
    if (crc != hdr.body_crc)
    {
        derror("crc checking failed");
        return ERR_INVALID_DATA;
    }
    _crc32 = crc;
    return ERR_OK;
}

std::shared_ptr<log_block> log_file::prepare_log_block() const
{
    log_block_header hdr;
    hdr.magic = 0xdeadbeef;
    hdr.length = 0;
    hdr.body_crc = 0;
    hdr.local_offset = static_cast<uint32_t>(_end_offset - _start_offset);
    
    binary_writer temp_writer;
    temp_writer.write_pod(hdr);
    return std::shared_ptr<log_block>(new log_block(temp_writer.get_buffer()));
}

::dsn::task_ptr log_file::commit_log_block(
                log_block& block,
                int64_t offset,
                dsn_task_code_t evt,
                clientlet* callback_host,
                aio_handler callback,
                int hash
                )
{
    dassert (!_is_read, "log file must be of write mode");
    dassert (offset == end_offset(), "offset should match with file's end offset");

    auto hdr = reinterpret_cast<log_block_header*>(const_cast<char*>(block.front().data()));

    dassert(hdr->magic == 0xdeadbeef, "");
    dassert(hdr->local_offset == static_cast<uint32_t>(offset - start_offset()), "");
    dassert(block.size() != 0, "log_block cannot be empty");

    hdr->length = block.size() - sizeof(log_block_header);
    hdr->body_crc = _crc32;
    for (auto log_iter = std::next(block.data().begin()) ; log_iter != block.data().end(); ++ log_iter)
    {
        hdr->body_crc = dsn_crc32_compute(
            static_cast<const void*>(log_iter->data()),
            static_cast<size_t>(log_iter->length()), hdr->body_crc
            );
    }
    _crc32 = hdr->body_crc;

    std::unique_ptr<dsn_file_buffer_t> buffer_vector(new dsn_file_buffer_t[block.data().size()]);
    std::transform(block.data().begin(), block.data().end(), buffer_vector.get(), [](const blob& bb)
    {
        return dsn_file_buffer_t
            {
                reinterpret_cast<void*>(const_cast<char*>(bb.data())),
                bb.length()
            };
    });

    auto task = file::write_vector(
        _handle,
        buffer_vector.get(),
        block.data().size(),
        static_cast<int>(offset - start_offset()),
        evt,
        callback_host,
        callback,
        hash
        );

    _end_offset += block.size();
    return task;
}

int log_file::read_header(binary_reader& reader)
{
    /*
     * and the log file header structure:
     * log_file_header +
     * count + [gpid + prev_log_max_decree]-->array_size_is_count
     */
    reader.read_pod(_header);

    int count;
    reader.read(count);
    for (int i = 0; i < count; i++)
    {
        global_partition_id gpid;
        log_replica_info info;

        reader.read_pod(gpid);
        reader.read_pod(info);

        _previous_log_max_decrees[gpid] = info;
    }

    return get_file_header_size();
}

int log_file::get_file_header_size() const
{
    int count = static_cast<int>(_previous_log_max_decrees.size());
    return static_cast<int>(
        sizeof(log_file_header) + sizeof(count)
        + (sizeof(global_partition_id) + sizeof(log_replica_info))*count
        );
}

bool log_file::is_right_header() const
{
    return _header.magic == 0xdeadbeef &&
        _header.start_global_offset == _start_offset;
}

int log_file::write_header(
    binary_writer& writer,
    multi_partition_decrees_ex& init_max_decrees,
    int buffer_bytes
    )
{
    _previous_log_max_decrees = init_max_decrees;

    _header.magic = 0xdeadbeef;
    _header.version = 0x1;
    _header.start_global_offset = start_offset();
    // staleness already set in the constructor

    writer.write_pod(_header);

    int count = static_cast<int>(_previous_log_max_decrees.size());
    writer.write(count);
    for (auto& kv : _previous_log_max_decrees)
    {
        writer.write_pod(kv.first);
        writer.write_pod(kv.second);
    }

    return get_file_header_size();
}

}} // end namespace
