#pragma once

#include <string>

namespace Loom {
    
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

static std::string edgeTypeToString(Loom::EdgeType type) {
    switch (type) {
        case Loom::EdgeType::IN:
            return "IN";
        case Loom::EdgeType::OUT:
            return "OUT";
        case Loom::EdgeType::BOTH:
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