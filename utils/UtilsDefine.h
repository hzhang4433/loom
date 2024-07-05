/***************************
@Author: Chunel
@Contact: chunel@foxmail.com
@File: UtilsDefine.h
@Time: 2021/4/30 8:52 下午
@Desc: 
***************************/

#ifndef UTIL_UTILSDEFINE_H
#define UTIL_UTILSDEFINE_H

#include <iostream>
#include <string>

    #if __cplusplus >= 201703L
#include <shared_mutex>
    #else
#include <mutex>
    #endif

#include "CBasic/CBasicInclude.h"
#include "UAllocator.h"
#include "UtilsFunction.h"

UTIL_NAMESPACE_BEGIN

#ifdef _ENABLE_LIKELY_
    #define likely(x)   __builtin_expect(!!(x), 1)
    #define unlikely(x) __builtin_expect(!!(x), 0)
#else
    #define likely
    #define unlikely
#endif

using UTIL_LOCK_GUARD = std::lock_guard<std::mutex>;
using UTIL_UNIQUE_LOCK = std::unique_lock<std::mutex>;

/* 判断函数流程是否可以继续 */
UTIL_INTERNAL_NAMESPACE_BEGIN
    static std::mutex g_check_status_mtx;
    static std::mutex g_echo_mtx;
UTIL_INTERNAL_NAMESPACE_END

#if __cplusplus >= 201703L
    using UTIL_READ_LOCK = std::shared_lock<std::shared_mutex>;
    using UTIL_WRITE_LOCK = std::unique_lock<std::shared_mutex>;
#else
    using UTIL_READ_LOCK = UTIL_LOCK_GUARD;    // C++14不支持读写锁，使用mutex替代
    using UTIL_WRITE_LOCK = UTIL_LOCK_GUARD;
#endif


template<typename T>
CStatus __ASSERT_NOT_NULL(T t) {
    return (unlikely(nullptr == t))
           ? CErrStatus(UTIL_INPUT_IS_NULL)
           : CStatus();
}

template<typename T, typename... Args>
CStatus __ASSERT_NOT_NULL(T t, Args... args) {
    if (unlikely(t == nullptr)) {
        return __ASSERT_NOT_NULL(t);
    }

    return __ASSERT_NOT_NULL(args...);
}

template<typename T>
CVoid __ASSERT_NOT_NULL_THROW_EXCEPTION(T t) {
    if (unlikely(nullptr == t)) {
        UTIL_THROW_EXCEPTION("[CException] " + std::string(UTIL_INPUT_IS_NULL))
    }
}

template<typename T, typename... Args>
CVoid __ASSERT_NOT_NULL_THROW_EXCEPTION(T t, Args... args) {
    if (unlikely(nullptr == t)) {
        __ASSERT_NOT_NULL_THROW_EXCEPTION(t);
    }

    __ASSERT_NOT_NULL_THROW_EXCEPTION(args...);
}


/** 判断传入的多个指针信息，是否为空 */
#define UTIL_ASSERT_NOT_NULL(ptr, ...)                                                     \
    {                                                                                        \
        const CStatus& __cur_status__ = __ASSERT_NOT_NULL(ptr, ##__VA_ARGS__);               \
        if (unlikely(__cur_status__.isErr())) { return __cur_status__; }                     \
    }                                                                                        \


/** 判断传入的多个指针，是否为空。如果为空，则抛出异常信息 */
#define UTIL_ASSERT_NOT_NULL_THROW_ERROR(ptr, ...)                                         \
    __ASSERT_NOT_NULL_THROW_EXCEPTION(ptr, ##__VA_ARGS__);                                   \

/* 删除资源信息 */
#define UTIL_DELETE_PTR(ptr)                                                  \
    if (unlikely((ptr) != nullptr)) {                                           \
        delete (ptr);                                                           \
        (ptr) = nullptr;                                                        \
    }                                                                           \

#define UTIL_ASSERT_INIT(isInit)                                              \
    if (unlikely((isInit) != is_init_)) {                                       \
        UTIL_RETURN_ERROR_STATUS("init status is not suitable")               \
    }                                                                           \

#define UTIL_ASSERT_INIT_THROW_ERROR(isInit)                                  \
    if (unlikely((isInit) != is_init_)) {                                       \
        UTIL_THROW_EXCEPTION("[CException] init status is not suitable") }    \

#define UTIL_ASSERT_MUTABLE_INIT_THROW_ERROR(isInit)                                  \
    if (unlikely((isInit) != is_init_) && !isMutable()) {                               \
        UTIL_THROW_EXCEPTION("[CException] mutable init status is not suitable") }    \

#define UTIL_SLEEP_SECOND(s)                                                  \
    std::this_thread::sleep_for(std::chrono::seconds(s));                       \

#define UTIL_SLEEP_MILLISECOND(ms)                                            \
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));                 \

#define UTIL_FUNCTION_CHECK_STATUS                                                         \
    if (unlikely(status.isErr())) {                                                          \
        if (status.isCrash()) { throw CException(status.getInfo()); }                        \
        UTIL_LOCK_GUARD lock{ internal::g_check_status_mtx };                              \
        UTIL_ECHO("%s, errorCode = [%d], errorInfo = [%s].",                               \
            status.getLocate().c_str(), status.getCode(), status.getInfo().c_str());         \
        return status;                                                                       \
    }                                                                                        \

/**
* 定制化输出
* @param cmd
* @param ...
* 注：内部包含全局锁，不建议正式上线的时候使用
*/
inline CVoid UTIL_ECHO(const char *cmd, ...) {
#ifdef _UTIL_SILENCE_
    return;
#endif

    std::lock_guard<std::mutex> lock{ internal::g_echo_mtx };
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() % 1000;
    std::cout << "[" << std::put_time(std::localtime(&time), "%Y-%m-%d %H:%M:%S.")    \
    << std::setfill('0') << std::setw(3) << ms << "] ";

    va_list args;
    va_start(args, cmd);
    vprintf(cmd, args);
    va_end(args);
    std::cout << "\n";
}

UTIL_NAMESPACE_END

#endif //UTIL_UTILSDEFINE_H
