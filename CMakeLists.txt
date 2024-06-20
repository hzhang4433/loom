set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

cmake_minimum_required(VERSION 3.12)
project(minWRollback)

# 指定c++标准
set(CMAKE_CXX_STANDARD 17)
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


include_directories(${CMAKE_SOURCE_DIR})

file(GLOB_RECURSE SRC_LIST "*.cpp")
list(FILTER SRC_LIST EXCLUDE REGEX ".*/test/.*")
list(FILTER SRC_LIST EXCLUDE REGEX ".*/build/.*")
file(GLOB_RECURSE HEADERS "*.h" "*.hpp")

# 禁止tbb的deprecated警告
add_definitions(-DTBB_SUPPRESS_DEPRECATED_MESSAGES=1)

add_executable(minWRollback ${SRC_LIST} ${HEADERS})
target_link_libraries(minWRollback ${GTEST_LIBRARIES} ${TBB_LIBRARIES} ${Boost_LIBRARIES} pthread)
target_compile_options(minWRollback PRIVATE -Wno-deprecated -Wpedantic -Wno-unused-variable -Wno-unused-parameter -Wno-comment -Wno-sign-compare -Wno-pedantic -Wno-return-type -Wno-uninitialized)