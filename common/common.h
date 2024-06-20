#pragma once

#include <string>

namespace minw {
    
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

static std::string edgeTypeToString(minw::EdgeType type) {
    switch (type) {
        case minw::EdgeType::IN:
            return "IN";
        case minw::EdgeType::OUT:
            return "OUT";
        case minw::EdgeType::BOTH:
            return "BOTH";
        default:
            return "UNKNOWN";
    }
}

// 定义区块大小
static size_t BLOCK_SIZE = 100;

}