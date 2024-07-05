/***************************
@Author: Chunel
@Contact: chunel@foxmail.com
@File: USemaphore.h
@Time: 2023/10/9 22:01
@Desc: 
***************************/

#ifndef UTIL_USEMAPHORE_H
#define UTIL_USEMAPHORE_H

UTIL_NAMESPACE_BEGIN

#include <mutex>
#include <condition_variable>

#include "../UThreadObject.h"

class USemaphore : public UThreadObject {
public:
    /**
     * 触发一次信号
     */
    CVoid signal() {
        UTIL_UNIQUE_LOCK lk(mutex_);
        cnt_++;
        if (cnt_ <= 0) {
            cv_.notify_one();
        }
    }

    /**
     * 等待信号触发
     */
    CVoid wait() {
        UTIL_UNIQUE_LOCK lk(mutex_);
        cnt_--;
        if (cnt_ < 0) {
            cv_.wait(lk);
        }
    }

private:
    CInt cnt_ = 0;    // 记录当前的次数
    std::mutex mutex_;
    std::condition_variable cv_;
};

UTIL_NAMESPACE_END

#endif //UTIL_USEMAPHORE_H
