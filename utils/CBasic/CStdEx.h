/***************************
@Author: Chunel
@Contact: chunel@foxmail.com
@File: CStdEx.h
@Time: 2023/1/31 23:15
@Desc: 
***************************/

#ifndef UTIL_CSTDEX_H
#define UTIL_CSTDEX_H

#include <memory>
#include <type_traits>

UTIL_NAMESPACE_BEGIN

// 兼容 std::enable_if_t 的语法
template<bool B, typename T = void>
using c_enable_if_t = typename std::enable_if<B, T>::type;

// 兼容 std::make_unique 的语法
template<typename T, typename... Args>
typename std::unique_ptr<T> c_make_unique(Args&&... args) {
    return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}

UTIL_NAMESPACE_END

#endif //UTIL_CSTDEX_H
