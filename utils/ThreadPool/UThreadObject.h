/***************************
@Author: Chunel
@Contact: chunel@foxmail.com
@File: CThreadObject.h
@Time: 2021/7/2 10:39 下午
@Desc: 
***************************/

#ifndef UTIL_UTHREADOBJECT_H
#define UTIL_UTHREADOBJECT_H

#include "../UtilsObject.h"
#include "UThreadPoolDefine.h"

UTIL_NAMESPACE_BEGIN

class UThreadObject : public UtilsObject {
protected:
    /**
     * 部分thread中的算子，可以不实现run方法
     * @return
     */
    CStatus run() override {
        UTIL_NO_SUPPORT
    }
};

UTIL_NAMESPACE_END

#endif //UTIL_UTHREADOBJECT_H
