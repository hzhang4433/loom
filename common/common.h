#pragma once

#include <string>

namespace loom {
    
enum class DependencyType {
    STRONG,
    WEAK
};

enum class EdgeType {
    NONE,
    IN,
    OUT,
    BOTH
};

static std::string edgeTypeToString(loom::EdgeType type) {
    switch (type) {
        case loom::EdgeType::IN:
            return "IN";
        case loom::EdgeType::OUT:
            return "OUT";
        case loom::EdgeType::BOTH:
            return "BOTH";
        default:
            return "UNKNOWN";
    }
}

// 定义区块大小
static size_t BLOCK_SIZE = 1000;

// 全局变量控制输出
static bool isOutputEnabled = false;

}