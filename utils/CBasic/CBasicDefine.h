/***************************
@Author: Chunel
@Contact: chunel@foxmail.com
@File: CBasicDefine.h
@Time: 2021/4/26 8:15 下午
@Desc:
***************************/

#ifndef UTIL_CBASICDEFINE_H
#define UTIL_CBASICDEFINE_H

#include <cstddef>

#define UTIL_NAMESPACE_BEGIN                                          \
namespace Util {                                                      \

#define UTIL_NAMESPACE_END                                            \
} /* end of namespace Util */                                         \

UTIL_NAMESPACE_BEGIN

#define UTIL_INTERNAL_NAMESPACE_BEGIN                                 \
namespace internal {                                                    \

#define UTIL_INTERNAL_NAMESPACE_END                                   \
} /* end of namespace internal */                                       \

UTIL_INTERNAL_NAMESPACE_BEGIN

using CCHAR = char;
using CUINT = unsigned int;
using CVOID = void;
using CINT = int;
using CLONG = long;
using CULONG = unsigned long;
using CBOOL = bool;
using CBIGBOOL = int;
using CFLOAT = float;
using CDOUBLE = double;
using CCONSTR = const char*;
using CSIZE = size_t;

UTIL_INTERNAL_NAMESPACE_END
UTIL_NAMESPACE_END

#endif //UTIL_CBASICDEFINE_H
