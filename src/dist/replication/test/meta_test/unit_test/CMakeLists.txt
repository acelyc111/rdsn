set(MY_PROJ_NAME dsn.meta.test)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC ../misc/misc.cpp
                ../../../meta_server/compact/meta_compact_service.cpp
                ../../../meta_server/compact/compact_policy_worker.cpp
                ../../../meta_server/compact/policy_deadline_checker.cpp)

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

ms_find_source_files("CXX" "${CMAKE_CURRENT_SOURCE_DIR}/../../../meta_server" "GLOB" MY_PROJ_SRC)

set(MY_PROJ_INC_PATH "")

set(MY_PROJ_LIBS dsn.replication.zookeeper_provider
                 dsn.replication.clientlib
                 dsn.block_service.local
                 dsn.block_service.fds
                 dsn.failure_detector
                 zookeeper_mt
                 galaxy-fds-sdk-cpp
                 PocoNet
                 PocoFoundation
                 PocoNetSSL
                 PocoJSON
                 crypto
                 fmt
                 gtest)

set(MY_PROJ_LIB_PATH "")

set(MY_BOOST_PACKAGES system filesystem)

# Extra files that will be installed
set(MY_BINPLACES clear.sh run.sh config-ddl-test.ini config-test.ini suite1 suite2)

add_definitions(-DDSN_MOCK_TEST)
dsn_add_executable()
