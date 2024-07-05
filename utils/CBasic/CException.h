/***************************
@Author: Chunel
@Contact: chunel@foxmail.com
@File: CException.h
@Time: 2022/4/15 20:51
@Desc: 异常处理类
***************************/

#ifndef UTIL_CEXCEPTION_H
#define UTIL_CEXCEPTION_H

#include <string>
#include <exception>

#include "CStrDefine.h"

UTIL_NAMESPACE_BEGIN
UTIL_INTERNAL_NAMESPACE_BEGIN

class CEXCEPTION : public std::exception {
public:
    explicit CEXCEPTION(const std::string& info,
                        const std::string& locate = UTIL_EMPTY) {
        /**
         * 这里的设计，和CStatus有一个联动
         * 如果不了解具体情况，不建议做任何修改
         */
        exception_info_ = locate + " | " + info;
    }

    /**
     * 获取异常信息
     * @return
     */
    const char* what() const noexcept override {
        return exception_info_.c_str();
    }

private:
    std::string exception_info_;            // 异常状态信息
};

UTIL_INTERNAL_NAMESPACE_END
UTIL_NAMESPACE_END

#endif //UTIL_CEXCEPTION_H
