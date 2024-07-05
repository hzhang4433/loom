/***************************
@Author: Chunel
@Contact: chunel@foxmail.com
@File: UtilsObject.h
@Time: 2021/9/19 12:00 上午
@Desc: 
***************************/

#ifndef UTIL_UTILSOBJECT_H
#define UTIL_UTILSOBJECT_H

#include "UtilsDefine.h"

UTIL_NAMESPACE_BEGIN

class UtilsObject : public CObject {
protected:
    CStatus run() override {
        UTIL_NO_SUPPORT
    }
};

UTIL_NAMESPACE_END

#endif //UTIL_UTILSOBJECT_H
