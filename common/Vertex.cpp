#include "Vertex.h"


Vertex::Vertex(std::shared_ptr<HyperVertex> hyperVertex, std::string id, double cost, bool isNested) : m_hyperVertex(hyperVertex), m_id(id), m_cost(cost), isNested(isNested = false) {
    m_min_in = -1;
    m_min_out = -1;
    m_degree = 0;
}

Vertex::~Vertex() {}

const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash, Vertex::VertexEqual>& Vertex::getChildren() const { 
    return m_children; 
}

void Vertex::addChild(Vertex::Ptr child) { 
    m_children.insert(child); 
}

minw::DependencyType Vertex::getDependencyType() const { 
    return m_dependencyType; 
}

void Vertex::setDependencyType(minw::DependencyType type) { 
    m_dependencyType = type; 
}

// int Vertex::mapToHyperId() const { 
//     return m_hyperId; 
// }
