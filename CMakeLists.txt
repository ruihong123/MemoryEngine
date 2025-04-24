cmake_minimum_required(VERSION 3.10)
project(SELCC)

set(CMAKE_CXX_STANDARD 17)

# Disable all warnings
 if (MSVC)
     # Visual Studio
     add_compile_options(/w)
 else()
     # GCC/Clang
     add_compile_options(-w)
 endif()

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DROCKSDB_SUPPORT_THREAD_LOCAL -DTIMESTAMP -DBACKOFF -Wall -Wextra  -pthread -ggdb") #-fsanitize=address -fsanitize=leak -fstack-protector
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused -Wno-format -Wno-missing-field-initializers -Wno-sign-compare -Wno-type-limits ")
#-DLOCK
#add_compile_options(-fsanitize=address)
#add_link_options(-fsanitize=address)
#set(CMAKE_CXX_FLAGS_RELEASE  "${CMAKE_CXX_FLAGS_RELEASE} -O3 -Wall -Wextra -pthread")
#set(CMAKE_CXX_FLAGS_DEBUG   "${CMAKE_CXX_FLAGS_DEBUG} -O2 -ggdb -Wall -Wextra -pthread")
include_directories(
        "${PROJECT_SOURCE_DIR}"
        "${PROJECT_SOURCE_DIR}/txn"
        "${PROJECT_SOURCE_DIR}/storage"
        "${PROJECT_SOURCE_DIR}/tpcc"
        "${PROJECT_SOURCE_DIR}/bench"
        "${PROJECT_SOURCE_DIR}/utils"
        )

add_library(DSMEngine utils/Local_opt_locks.cpp utils/Local_opt_locks.h btr/btr_itr.cpp)
add_library(stx_btree btr/stx_btree.h)
target_sources(DSMEngine
        PRIVATE
        "btr/Btr.cpp"
        "utils/ThreadPool.cpp"
        "utils/ThreadPool.h"
        "utils/allocator.h"
        "port/port_posix.h"
        "port/port_posix.cc"
        "port/port.h"
        "utils/arena.cc"
        "utils/arena.h"
        "utils/autovector.h"
        "utils/concurrent_arena.cc"
        "utils/concurrent_arena.h"
        "utils/core_local.h"
        "storage/cache.cc"
        "utils/coding.cc"
        "utils/coding.h"
        "storage/Common.cpp"
        "utils/comparator.cc"
#        "utils/crc32c.cc"
#        "utils/crc32c.h"
        "utils/fastrange.h"
#        "utils/filter_policy.cc"
        "utils/hash.cc"
        "utils/hash.h"
        "utils/logging.cc"
        "utils/logging.h"
        "utils/mutexlock.h"
        "utils/mutexlock.cpp"
        "storage/page.cpp"
        "storage/page.h"
        "storage/Record.h"
        "storage/DDSM.cpp"
        "utils/no_destructor.h"
        "utils/options.cc"
        "utils/random.cc"
        "utils/random.h"
        "storage/rdma.cc"
        "storage/rdma.h"
        "utils/thread_local.cc"
        "utils/thread_local.h"
        "utils/status.cc"
        "utils/env_posix.h"
        "utils/env_posix.cc"
        "utils/env.cc"
        "utils/status.h"
        "test/zipf.h"
        "include/Tools/env.h"
        "txn/TransactionManagerMVOCC.cpp"
        "txn/TransactionManagerOCC.cpp"
        "txn/TransactionManager2PL.cpp"
        "txn/TxnManagerTo.cpp"
        "txn/DeltaSection.cpp"
        "txn/DeltaRecord.cpp"


        # Only CMake 3.3+ supports PUBLIC sources in targets exported by "install".
        $<$<VERSION_GREATER:CMAKE_VERSION,3.2>:PUBLIC>
        "include/Btr.h"
        "include/Common.h"
        "include/Config.h"
        "include/HugePageAlloc.h"
        "include/Timer.h"
        "include/DDSM.h"
        "include/EngineInitiator.h"
        "txn/TransactionManager.h"
        "txn/DeltaRecord.h"
        "txn/DeltaSection.h"
        )
# add more target sources for DSMEngine here
#        "include/ThreadPool.h"

target_include_directories(DSMEngine
        PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>

        )
enable_testing()
#message(STATUS "cmake flag is release ${CMAKE_CXX_FLAGS_RELEASE}")
# This project is tested using GoogleTest.
#add_subdirectory("third_party/googletest")
find_package(ibverbs REQUIRED)
target_link_libraries(DSMEngine ibverbs::ibverbs -lcityhash -lboost_system -lboost_coroutine -lboost_thread -lmemcached)
target_link_libraries(stx_btree DSMEngine)
if(WITH_NUMA)
    find_package(NUMA REQUIRED)
    add_definitions(-DNUMA)
    include_directories(${NUMA_INCLUDE_DIR})
    target_link_libraries(DSMEngine NUMA::NUMA)
endif()
#add_executable(TestTrial trial_test.cpp)
add_executable(memory_server "server.cc"
        "memory_node/memory_node_keeper.h"
        "memory_node/memory_node_keeper.cpp")
add_executable(memory_server_term "server_termination.cc"
        "memory_node/memory_node_keeper.h"
        "memory_node/memory_node_keeper.cpp")
add_executable(memory_server_tpcc "server_termination_tpcc.cc"
        "memory_node/memory_node_keeper.h"
        "memory_node/memory_node_keeper.cpp")
add_executable(cpu_scale_test "CPU_cache_scalability.cpp")
add_executable(btree_bench test/benchmark_btree.cpp)
add_executable(second_btree_bench test/benchmark_btree_secondary.cpp)
add_executable(micro_bench test/benchmark_micro.cpp)

file(GLOB TPCC_DEPENDENCY
        "${PROJECT_SOURCE_DIR}/storage/*.h"
        "${PROJECT_SOURCE_DIR}/storage/*.cpp"
        "${PROJECT_SOURCE_DIR}/bench/*.h"
        "${PROJECT_SOURCE_DIR}/bench/*.cpp"
        "${PROJECT_SOURCE_DIR}/tpcc/*.h"
        "${PROJECT_SOURCE_DIR}/tpcc/*.cpp"
        "${PROJECT_SOURCE_DIR}/utils/*.h"
        "${PROJECT_SOURCE_DIR}/txn/*.cpp"
        "${PROJECT_SOURCE_DIR}/txn/*.h"
)
add_executable(tpcc ${TPCC_DEPENDENCY})
target_link_libraries(tpcc DSMEngine)
target_link_libraries(memory_server DSMEngine)
target_link_libraries(memory_server_term DSMEngine)
target_link_libraries(memory_server_tpcc DSMEngine)
target_link_libraries(btree_bench DSMEngine)
target_link_libraries(second_btree_bench DSMEngine)
target_link_libraries(micro_bench DSMEngine)
target_link_libraries(cpu_scale_test -lnuma)

add_subdirectory(Rtree)

