#include "Vertex.h"


Vertex::Vertex(std::shared_ptr<HyperVertex> hyperVertex, int hyperId, std::string id, bool isNested) : m_hyperVertex(hyperVertex), m_hyperId(hyperId), m_id(id), isNested(isNested = false) {
    // m_min_in = -1;
    // m_min_out = -1;
    m_degree = 0;
}

Vertex::~Vertex() {}

const tbb::concurrent_unordered_set<Vertex::ChildVertex, Vertex::ChildVertexHash, Vertex::ChildVertexEqual>& Vertex::getChildren() const { 
    return m_children; 
}

void Vertex::addChild(Vertex::Ptr child, minw::DependencyType dependency) { 
    m_children.insert({child, dependency}); 
}

// minw::DependencyType Vertex::getDependencyType() const { 
//     return m_dependencyType; 
// }

// void Vertex::setDependencyType(minw::DependencyType type) { 
//     m_dependencyType = type; 
// }

// int Vertex::mapToHyperId() const { 
//     return m_hyperId; 
// }
