#pragma once

#include <string>

namespace minw {
    
enum class DependencyType {
    STRONG,
    WEAK
};

enum class EdgeType {
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

}