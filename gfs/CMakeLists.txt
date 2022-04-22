cmake_minimum_required(VERSION 2.8.12)
project(gfs CXX)

# 外部依赖：需要先手动安装fmt-4.1.0和sparsehash-2.0.4到系统中
find_package(FMT 4.1.0 REQUIRED)

# Include directories
include_directories(${PROJECT_SOURCE_DIR})

# Generic compilation options
set(CMAKE_CXX_FLAGS "-g -std=c++11 -Wall -pthread -DSKG_EDGE_DATA_COLUMN_STOAGE -DSKG_PROPERTIES_SUPPORT_NULL -D_FILE_OFFSET_BITS=64 -DDB_ADAPTER -DSKG_DISABLE_COMPRESSION -DSKG_PREPROCESS_DYNAMIC_EDGE -DUSE_STL_PRIORITY_QUEUE -DSKG_SUPPORT_THREAD_LOCAL -DSKG_QUERY_USE_MT -DSKG_REQ_VAR_PROP -D_SKGNET_STANDALONE_ -DROCKSDB_USING_THREAD_STATUS ${CMAKE_CXX_FLAGS}")

# initial variables
set(LIBGFS_PATH ${CMAKE_CURRENT_BINARY_DIR}/libgfs.a)
set(DEP_LIBS ${DEP_LIBS} env fmt fs metrics monitoring util threadpool)

# Source file lists
file(GLOB ENV_SRCS env/*.cc)
file(GLOB FMT_SRCS third_party/fmt/fmt/*.cc)
file(GLOB FS_SRCS fs/*.cc)
file(GLOB METRICS_SRCS metrics/*.cc)
file(GLOB MONITORING_SRCS monitoring/*.cc)
file(GLOB UTIL_SRCS util/*.cc)
file(GLOB THREADPOOL_SRCS util/threadpool/*.cc)

# build lib
add_library(env STATIC ${ENV_SRCS})
add_library(fmt STATIC ${FMT_SRCS})
add_library(fs STATIC ${FS_SRCS})
add_library(metrics STATIC ${METRICS_SRCS})
add_library(monitoring STATIC ${MONITORING_SRCS})
add_library(util STATIC ${UTIL_SRCS})
add_library(threadpool STATIC ${THREADPOOL_SRCS})
target_link_libraries(threadpool monitoring)

####### use ar put all static lib to libgfs.a, include libfmt.a #######
add_custom_command(OUTPUT libgfs.a
    COMMAND ar x $<TARGET_FILE:env>
    COMMAND ar x $<TARGET_FILE:fmt>
    COMMAND ar x $<TARGET_FILE:fs>
    COMMAND ar x $<TARGET_FILE:metrics>
    COMMAND ar x $<TARGET_FILE:monitoring>
    COMMAND ar x $<TARGET_FILE:util>
    COMMAND ar x $<TARGET_FILE:threadpool>
    COMMAND ar rc ${LIBGFS_PATH} *.o
    COMMAND rm -f ${CMAKE_CURRENT_BINARY_DIR}/*.o
    DEPENDS ${DEP_LIBS}
)
add_custom_target(_any ALL DEPENDS libgfs.a)
add_library(libgfs STATIC IMPORTED GLOBAL)
set_target_properties(libgfs
    PROPERTIES
    IMPORTED_LOCATION ${LIBGFS_PATH}
)
####### finish build libgfs.a #######

# build executable artifact for test
add_executable(newg test/newg.cc)

# 因make多线程编译时，没有正确处理libgfs依赖关系，暂时只能逐个link所有小lib
# target_link_libraries(newg libgfs)
target_link_libraries(newg ${DEP_LIBS})

