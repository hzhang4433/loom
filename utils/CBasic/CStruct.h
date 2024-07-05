/***************************
@Author: Chunel
@Contact: chunel@foxmail.com
@File: CStruct.h
@Time: 2023/7/16 11:36
@Desc: 
***************************/

#ifndef UTIL_CSTRUCT_H
#define UTIL_CSTRUCT_H

#include "CBasicDefine.h"

UTIL_NAMESPACE_BEGIN

/**
 * 所有框架内部结构体定义的基类
 * 仅针对类似 pod 数据类型的定义
 */
class CStruct {
public:
    ~CStruct() = default;
};

UTIL_NAMESPACE_END

#endif //UTIL_CSTRUCT_H
