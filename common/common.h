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

enum class TaskPriority {
    HIGH_PRIORITY,
    LOW_PRIORITY
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

static std::string taskPriorityToString(loom::TaskPriority type) {
    switch (type) {
        case loom::TaskPriority::HIGH_PRIORITY:
            return "HIGH_PRIORITY";
        case loom::TaskPriority::LOW_PRIORITY:
            return "LOW_PRIORITY";
        default:
            return "UNKNOWN";
    }
}

// 定义区块大小
extern size_t BLOCK_SIZE;

// 全局变量控制输出
static bool isOutputEnabled = false;

}