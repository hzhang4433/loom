/***************************
@Author: Chunel
@Contact: chunel@foxmail.com
@File: CValTypes.h
@Time: 2022/2/3 12:58 下午
@Desc: 
***************************/

#ifndef UTIL_CVALTYPE_H
#define UTIL_CVALTYPE_H

#include "CBasicDefine.h"
#include "CStatus.h"
#include "CException.h"

using CChar = Util::internal::CCHAR;
using CCharPtr = Util::internal::CCHAR *;
using CUint = Util::internal::CUINT;
using CSize = Util::internal::CSIZE;
using CVoid = Util::internal::CVOID;
using CVoidPtr = Util::internal::CVOID *;
using CInt = Util::internal::CINT;
using CLong = Util::internal::CLONG;
using CULong = Util::internal::CULONG;
using CBool = Util::internal::CBOOL;
using CIndex = Util::internal::CINT;                // 表示标识信息，可以为负数
using CFloat = Util::internal::CFLOAT;
using CDouble = Util::internal::CDOUBLE;
using CConStr = Util::internal::CCONSTR;             // 表示 const char*
using CBigBool = Util::internal::CBIGBOOL;

using CLevel = Util::internal::CINT;
using CSec = Util::internal::CLONG;                  // 表示秒信息, for second
using CMSec = Util::internal::CLONG;                 // 表示毫秒信息, for millisecond
using CFMSec = Util::internal::CDOUBLE;              // 表示毫秒信息，包含小数点信息

using CStatus = Util::internal::CSTATUS;
using CException = Util::internal::CEXCEPTION;

#endif //UTIL_CVALTYPE_H
