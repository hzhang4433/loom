set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

cmake_minimum_required(VERSION 3.12)
project(loom LANGUAGES CXX C)

# 设置C和C++编译器为GCC 11
set(CMAKE_C_COMPILER "/usr/bin/gcc-11")
set(CMAKE_CXX_COMPILER "/usr/bin/g++-11")

# 指定c++标准
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 找gtest
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

# 找tbb
set(TBB_INCLUDE_DIRS /usr/include/tbb)
set(TBB_LIBRARIES /usr/lib/x86_64-linux-gnu/libtbb.so)

# 找boost
find_package(Boost REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})

# 找glog
set(GLOG_DIR /usr/include/glog)
set(GLOG_LIBRARIES /usr/lib/x86_64-linux-gnu/libglog.so)

# 找fmt
set(FMT_DIR /usr/include/fmt)
set(FMT_LIBRARIES /usr/lib/x86_64-linux-gnu/libfmt.a)
include_directories(${FMT_INCLUDE_DIRS})


include_directories(${CMAKE_SOURCE_DIR})

file(GLOB_RECURSE SRC_LIST "*.cpp")
list(FILTER SRC_LIST EXCLUDE REGEX ".*/test/.*")
list(FILTER SRC_LIST EXCLUDE REGEX ".*/build/.*")
file(GLOB_RECURSE HEADERS "*.h" "*.hpp")

# 禁止tbb的deprecated警告
add_definitions(-DTBB_SUPPRESS_DEPRECATED_MESSAGES=1)

add_executable(loom ${SRC_LIST} ${HEADERS})
target_link_libraries(loom ${GTEST_LIBRARIES} ${TBB_LIBRARIES} ${Boost_LIBRARIES} ${GLOG_LIBRARIES} ${FMT_LIBRARIES} pthread)
target_compile_options(loom PRIVATE -Wno-deprecated -Wpedantic -Wno-unused-variable -Wno-unused-parameter -Wno-comment -Wno-sign-compare -Wno-pedantic -Wno-return-type -Wno-uninitialized)
# add project to include path
# message(STATUS "Project source directory: ${PROJECT_SOURCE_DIR}")
target_include_directories(loom PRIVATE ${PROJECT_SOURCE_DIR}/..)