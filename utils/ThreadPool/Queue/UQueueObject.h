/***************************
@Author: Chunel
@Contact: chunel@foxmail.com
@File: UQueueObject.h
@Time: 2022/10/1 20:31
@Desc: 
***************************/

#ifndef UTIL_UQUEUEOBJECT_H
#define UTIL_UQUEUEOBJECT_H

#include <mutex>

#include "../UThreadObject.h"
#include "UQueueDefine.h"

UTIL_NAMESPACE_BEGIN

class UQueueObject : public UThreadObject {
protected:
    std::mutex mutex_;
    std::condition_variable cv_;
};

UTIL_NAMESPACE_END

#endif //UTIL_UQUEUEOBJECT_H
