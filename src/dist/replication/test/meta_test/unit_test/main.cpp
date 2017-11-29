#include <cmath>
#include <fstream>
#include <iostream>

#include <gtest/gtest.h>
#include <dsn/service_api_cpp.h>

#include "dist/replication/meta_server/meta_data.h"

#include "meta_service_test_app.h"

int gtest_flags = 0;
int gtest_ret = 0;
meta_service_test_app *g_app;

// as it is not easy to clean test environment in some cases, we simply run these tests in several
// commands,
// please check the script "run.sh" to modify the GTEST_FILTER
// currently, three filters are used to run these tests:
//   1. a test only run "meta.data_definition", coz it use different config-file
//   2. a test only run "meta.apply_balancer", coz it modify the global state of remote-storage,
//   this conflicts meta.state_sync
//   3. all others tests
//
// If adding a test which doesn't modify the global state, you should simple add your test to the
// case3.
TEST(meta, state_sync) { g_app->state_sync_test(); }

TEST(meta, data_definition) { g_app->data_definition_op_test(); }

TEST(meta, update_configuration) { g_app->update_configuration_test(); }

TEST(meta, balancer_validator) { g_app->balancer_validator(); }

TEST(meta, apply_balancer) { g_app->apply_balancer_test(); }

TEST(meta, cannot_run_balancer_test) { g_app->cannot_run_balancer_test(); }

TEST(meta, construct_apps_test) { g_app->construct_apps_test(); }

TEST(meta, balance_config_file) { g_app->balance_config_file(); }

TEST(meta, simple_lb_balanced_cure) { g_app->simple_lb_balanced_cure(); }

TEST(meta, simple_lb_cure_test) { g_app->simple_lb_cure_test(); }

TEST(meta, simple_lb_from_proposal_test) { g_app->simple_lb_from_proposal_test(); }

TEST(meta, simple_lb_collect_replica) { g_app->simple_lb_collect_replica(); }

TEST(meta, simple_lb_construct_replica) { g_app->simple_lb_construct_replica(); }

TEST(meta, json_compacity) { g_app->json_compacity(); }

TEST(meta, adjust_dropped_size) { g_app->adjust_dropped_size(); }

dsn::error_code meta_service_test_app::start(int argc, char **argv)
{
    uint32_t seed =
        (uint32_t)dsn_config_get_value_uint64("tools.simulator", "random_seed", 0, "random seed");
    if (seed == 0) {
        seed = time(0);
        derror("initial seed: %u", seed);
    }
    srand(seed);
    testing::InitGoogleTest(&argc, argv);
    g_app = this;
    gtest_ret = RUN_ALL_TESTS();
    gtest_flags = 1;
    return dsn::ERR_OK;
}

GTEST_API_ int main(int argc, char **argv)
{
    dsn::register_app<meta_service_test_app>("test_meta");
    dsn_meta_server_bridge(0, nullptr);
    if (argc < 2)
        dassert(dsn_run_config("config-test.ini", false), "");
    else
        dassert(dsn_run_config(argv[1], false), "");

    while (gtest_flags == 0) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

#ifndef ENABLE_GCOV
    dsn_exit(gtest_ret);
#endif
    return gtest_ret;
}
