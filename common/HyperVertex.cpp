#include "HyperVertex.h"

HyperVertex::HyperVertex() {}

HyperVertex::~HyperVertex() {}

Vertex::Ptr HyperVertex::getVertexById(const std::string& id) const {
    auto it = m_vertices.find(id);
    return it != m_vertices.end() ? it->second : nullptr;
}

void HyperVertex::buildTree() {

}