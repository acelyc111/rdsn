#include <dsn/dist/nfs/nfs.h>
#include <dsn/dist/serverlet.h>
#include <dsn/tool-api/service_app.h>
#include <dsn/utility/filesystem.h>
#include <gtest/gtest.h>

#ifdef __TITLE__
#undef __TITLE__
#endif

#define __TITLE__ "fd.test"

using namespace dsn;

static std::unique_ptr<::dsn::nfs_node> nfs_handler;

// please ensure dir and dir_copy doesn't exist under ./
static std::string dir = "nfs_file_dir";
static std::string dir_copy = "nfs_file_copy_dir";

// two file that already exist under ./
static std::string file1 = "nfs_test_file1";
static std::string file2 = "nfs_test_file2";

struct aio_result
{
    dsn::error_code err;
    size_t sz;
};

class nfs_server_app : public service_app, public serverlet<nfs_server_app>
{
public:
    nfs_server_app(const service_app_info *info) : service_app(info), serverlet("nfs_server_app") {}

    error_code start(const std::vector<std::string> &args) override
    {
        nfs_handler.reset(nfs_node::create_new());
        return nfs_handler->start();
    }

    error_code stop(bool) override { return nfs_handler->stop(); }
};

//::dsn::nfs_node *nfs;
void nfs_init() { dsn::service_app::register_factory<nfs_server_app>("nfs_server"); }

DEFINE_THREAD_POOL_CODE(THREAD_POOL_NFS_TEST)
DEFINE_TASK_CODE_AIO(LPC_NFS_TEST, TASK_PRIORITY_COMMON, THREAD_POOL_NFS_TEST)

TEST(nfs, copy_file_test)
{
    ASSERT_TRUE(nfs_handler != nullptr);

    utils::filesystem::remove_path(dir);
    utils::filesystem::remove_path(dir_copy);

    ASSERT_FALSE(utils::filesystem::directory_exists(dir));
    ASSERT_FALSE(utils::filesystem::directory_exists(dir_copy));

    ASSERT_TRUE(utils::filesystem::create_directory(dir));
    ASSERT_TRUE(utils::filesystem::directory_exists(dir));

    {
        std::cout << "test copy file to dir" << std::endl;
        ASSERT_FALSE(utils::filesystem::file_exists(utils::filesystem::path_combine(dir, file1)));
        ASSERT_FALSE(utils::filesystem::file_exists(utils::filesystem::path_combine(dir, file2)));

        std::vector<std::string> files = {file1, file2};

        aio_result r;
        aio_task_ptr tsk = nfs_handler->copy_remote_files(dsn::rpc_address("localhost", 50001),
                                                          ".",
                                                          files,
                                                          dir,
                                                          false,
                                                          false,
                                                          LPC_NFS_TEST,
                                                          nullptr,
                                                          [&r](dsn::error_code err, size_t sz) {
                                                              r.err = err;
                                                              r.sz = sz;
                                                          });
        tsk->wait(20000);
        ASSERT_EQ(ERR_OK, r.err);
        ASSERT_EQ(r.err, tsk->error());
        ASSERT_EQ(r.sz, tsk->get_transferred_size());

        int64_t sz1 = 0, sz2 = 0;
        ASSERT_TRUE(utils::filesystem::file_size(file1, sz1));
        ASSERT_TRUE(utils::filesystem::file_size(file2, sz2));

        ASSERT_EQ(r.sz, sz1 + sz2);

        ASSERT_TRUE(utils::filesystem::file_exists(utils::filesystem::path_combine(dir, file1)));
        ASSERT_TRUE(utils::filesystem::file_exists(utils::filesystem::path_combine(dir, file2)));

        sz1 = sz2 = 0;
        ASSERT_TRUE(utils::filesystem::file_size(file1, sz1));
        ASSERT_TRUE(utils::filesystem::file_size(utils::filesystem::path_combine(dir, file1), sz2));
        ASSERT_EQ(sz1, sz2);

        sz1 = sz2 = 0;
        ASSERT_TRUE(utils::filesystem::file_size(file2, sz1));
        ASSERT_TRUE(utils::filesystem::file_size(utils::filesystem::path_combine(dir, file2), sz2));
        ASSERT_EQ(sz1, sz2);
    }

    {
        std::cout << "test copy file to dir again, overwrite the file that exist" << std::endl;
        ASSERT_TRUE(utils::filesystem::file_exists(utils::filesystem::path_combine(dir, file1)));
        ASSERT_TRUE(utils::filesystem::file_exists(utils::filesystem::path_combine(dir, file2)));

        std::vector<std::string> files = {file1, file2};

        aio_result r;
        aio_task_ptr tsk = nfs_handler->copy_remote_files(dsn::rpc_address("localhost", 50001),
                                                          ".",
                                                          files,
                                                          dir,
                                                          false,
                                                          false,
                                                          LPC_NFS_TEST,
                                                          nullptr,
                                                          [&r](dsn::error_code err, size_t sz) {
                                                              r.err = err;
                                                              r.sz = sz;
                                                          });
        tsk->wait(20000);
        ASSERT_EQ(ERR_OK, r.err);
        ASSERT_EQ(r.err, tsk->error());
        ASSERT_EQ(r.sz, tsk->get_transferred_size());

        int64_t sz1 = 0, sz2 = 0;
        ASSERT_TRUE(utils::filesystem::file_size(file1, sz1));
        ASSERT_TRUE(utils::filesystem::file_size(file2, sz2));

        ASSERT_EQ(r.sz, sz1 + sz2);

        ASSERT_TRUE(utils::filesystem::file_exists(utils::filesystem::path_combine(dir, file1)));
        ASSERT_TRUE(utils::filesystem::file_exists(utils::filesystem::path_combine(dir, file2)));

        sz1 = sz2 = 0;
        ASSERT_TRUE(utils::filesystem::file_size(file1, sz1));
        ASSERT_TRUE(utils::filesystem::file_size(utils::filesystem::path_combine(dir, file1), sz2));
        ASSERT_EQ(sz1, sz2);

        sz1 = sz2 = 0;
        ASSERT_TRUE(utils::filesystem::file_size(file2, sz1));
        ASSERT_TRUE(utils::filesystem::file_size(utils::filesystem::path_combine(dir, file2), sz2));
        ASSERT_EQ(sz1, sz2);
    }

    {
        std::cout << "copy dir to dir_copy" << std::endl;
        ASSERT_FALSE(utils::filesystem::directory_exists(dir_copy));

        aio_result r;
        aio_task_ptr tsk = nfs_handler->copy_remote_directory(dsn::rpc_address("localhost", 50001),
                                                              dir,
                                                              dir_copy,
                                                              false,
                                                              false,
                                                              LPC_NFS_TEST,
                                                              nullptr,
                                                              [&r](dsn::error_code err, size_t sz) {
                                                                  r.err = err;
                                                                  r.sz = sz;
                                                              });
        tsk->wait(20000);
        ASSERT_EQ(ERR_OK, r.err);
        ASSERT_EQ(r.err, tsk->error());
        ASSERT_EQ(r.sz, tsk->get_transferred_size());

        int64_t sz1 = 0, sz2 = 0;
        ASSERT_TRUE(utils::filesystem::file_size(file1, sz1));
        ASSERT_TRUE(utils::filesystem::file_size(file2, sz2));

        ASSERT_EQ(r.sz, sz1 + sz2);

        ASSERT_TRUE(utils::filesystem::directory_exists(dir_copy));
        ASSERT_TRUE(
            utils::filesystem::file_exists(utils::filesystem::path_combine(dir_copy, file1)));
        ASSERT_TRUE(
            utils::filesystem::file_exists(utils::filesystem::path_combine(dir_copy, file2)));

        std::vector<std::string> sub1, sub2;
        ASSERT_TRUE(utils::filesystem::get_subfiles(dir, sub1, true));
        ASSERT_TRUE(utils::filesystem::get_subfiles(dir_copy, sub2, true));
        ASSERT_EQ(sub1.size(), sub2.size());

        ASSERT_TRUE(utils::filesystem::file_size(utils::filesystem::path_combine(dir, file1), sz1));
        ASSERT_TRUE(
            utils::filesystem::file_size(utils::filesystem::path_combine(dir_copy, file1), sz2));
        ASSERT_EQ(sz1, sz2);
        ASSERT_TRUE(utils::filesystem::file_size(utils::filesystem::path_combine(dir, file2), sz1));
        ASSERT_TRUE(
            utils::filesystem::file_size(utils::filesystem::path_combine(dir_copy, file2), sz2));
        ASSERT_EQ(sz1, sz2);
    }
}
