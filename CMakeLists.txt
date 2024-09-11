set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -funroll-loops -march=native")
# set(CMAKE_CXX_FLAGS_RELEASE "-O3")
# set(CMAKE_CXX_FLAGS_DEBUG "-O0")

cmake_minimum_required(VERSION 3.12)
project(loom LANGUAGES CXX C)

# 设置C和C++编译器为GCC 11
set(CMAKE_C_COMPILER "/usr/bin/gcc-11")
set(CMAKE_CXX_COMPILER "/usr/bin/g++-11")

# 指定c++标准
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(ENABLE_NDEBUG "Enable NDEBUG" OFF)

# find gtest
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

# find tbb
set(TBB_INCLUDE_DIRS /usr/include/tbb)
set(TBB_LIBRARIES /usr/lib/x86_64-linux-gnu/libtbb.so)

# find boost
find_package(Boost REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})

# find glog
set(GLOG_INCLUDE_DIRS /usr/include/glog)
set(GLOG_LIBRARIES /usr/lib/x86_64-linux-gnu/libglog.so)
include_directories(${GLOG_INCLUDE_DIRS})

# find gflags
find_package(gflags REQUIRED)
set(GFLAGS_LIBRARIES /usr/lib/x86_64-linux-gnu/libgflags.so)
include_directories(${GFLAGS_INCLUDE_DIRS})

# find fmt
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
target_link_libraries(
    loom 
    PUBLIC ${GTEST_LIBRARIES} ${TBB_LIBRARIES} ${Boost_LIBRARIES} ${GLOG_LIBRARIES} ${GFLAGS_LIBRARIES} ${FMT_LIBRARIES} pthread)

# Manage Compile Options
if(ENABLE_NDEBUG)
    message(STATUS "NDEBUG is enabled")
    target_compile_definitions(loom PRIVATE -DNDEBUG=1)
    target_compile_options(loom PRIVATE
        -O3 -funroll-loops -march=native
    )
else()
    message(STATUS "NDEBUG is disabled")
    # target_compile_definitions(loom PRIVATE -DNDEBUG=0)
    target_compile_definitions(loom PRIVATE UNDEBUG)
    target_compile_options(loom PRIVATE
        -O0 -ggdb
    )
endif()

target_compile_options(loom PRIVATE -Wno-deprecated -Wpedantic -Wno-unused-variable -Wno-unused-parameter -Wno-comment -Wno-sign-compare -Wno-pedantic -Wno-return-type -Wno-uninitialized)
# add project to include path
message(STATUS "Project source directory: ${PROJECT_SOURCE_DIR}")
target_include_directories(loom PRIVATE ${PROJECT_SOURCE_DIR}/..)