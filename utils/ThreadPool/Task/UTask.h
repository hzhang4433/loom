/***************************
@Author: Chunel
@Contact: chunel@foxmail.com
@File: UTask.h
@Time: 2021/7/2 11:32 下午
@Desc: 
***************************/

#ifndef UTIL_UTASK_H
#define UTIL_UTASK_H

#include <vector>
#include <memory>
#include <type_traits>

#include "../UThreadObject.h"

UTIL_NAMESPACE_BEGIN

class UTask : public CStruct {
    struct TaskBased {
        explicit TaskBased() = default;
        virtual CVoid call() = 0;
        virtual ~TaskBased() = default;
    };

    // 退化以获得实际类型，修改思路参考：https://github.com/ChunelFeng/CThreadPool/pull/3
    template<typename F, typename T = typename std::decay<F>::type>
    struct TaskDerided : TaskBased {
        T func_;
        explicit TaskDerided(F&& func) : func_(std::forward<F>(func)) {}
        CVoid call() final { func_(); }
    };

public:
    template<typename F>
    UTask(F&& func, int priority = 0)
        : impl_(new TaskDerided<F>(std::forward<F>(func)))
        , priority_(priority) {}

    CVoid operator()() {
        // impl_ 理论上不可能为空
        impl_ ? impl_->call() : throw CException("UTask inner function is nullptr");
    }

    UTask() = default;

    UTask(UTask&& task) noexcept:
            impl_(std::move(task.impl_)),
            priority_(task.priority_) {}

    UTask &operator=(UTask&& task) noexcept {
        impl_ = std::move(task.impl_);
        priority_ = task.priority_;
        return *this;
    }

    CBool operator>(const UTask& task) const {
        return priority_ < task.priority_;    // 新加入的，放到后面
    }

    CBool operator<(const UTask& task) const {
        return priority_ >= task.priority_;
    }

    UTIL_NO_ALLOWED_COPY(UTask)

private:
    std::unique_ptr<TaskBased> impl_ = nullptr;
    int priority_ = 0;                                 // 任务的优先级信息
};


using UTaskRef = UTask &;
using UTaskPtr = UTask *;
using UTaskArr = std::vector<UTask>;
using UTaskArrRef = std::vector<UTask> &;

UTIL_NAMESPACE_END

#endif //UTIL_UTASK_H
