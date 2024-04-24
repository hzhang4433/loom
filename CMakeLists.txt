set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

cmake_minimum_required(VERSION 3.12)
project(minWAbort)

# 指定c++标准
set(CMAKE_CXX_STANDARD 11)

# 找gtest
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

# 找tbb
set(TBB_INCLUDE_DIRS /usr/include/tbb)
set(TBB_LIBRARIES /usr/lib/x86_64-linux-gnu/libtbb.so)


file(GLOB SRC_LIST "*.cpp")
file(GLOB HEADERS "*.h")


add_executable(minWAbort ${SRC_LIST} ${HEADERS})
target_link_libraries(minWAbort ${GTEST_LIBRARIES} ${TBB_LIBRARIES} pthread)
target_compile_options(minWAbort PRIVATE -Wpedantic -Wno-unused-variable -Wno-unused-parameter -Wno-comment -Wno-sign-compare -Wno-pedantic -Wno-return-type -Wno-uninitialized)