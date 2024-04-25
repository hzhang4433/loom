#include "Vertex.h"


Vertex::Vertex(int hyperId, std::string id, double cost, bool isNested) : m_hyperId(hyperId), m_id(id), m_cost(cost), isNested(isNested = false) {
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

DependencyType Vertex::getDependencyType() const { 
    return m_dependencyType; 
}

void Vertex::setDependencyType(DependencyType type) { 
    m_dependencyType = type; 
}
