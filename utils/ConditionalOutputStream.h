#pragma once

#include <iostream>
#include <common/common.h>



namespace Util{
// 自定义输出类
class ConditionalOutputStream {
public:
    template <typename T>
    ConditionalOutputStream& operator<<(const T& value) {
        if (loom::isOutputEnabled) {
            std::cout << value;
        }
        return *this;
    }

    // 处理流操纵器（例如 std::endl）
    ConditionalOutputStream& operator<<(std::ostream& (*pf)(std::ostream&)) {
        if (loom::isOutputEnabled) {
            std::cout << pf;
        }
        return *this;
    }
};

};