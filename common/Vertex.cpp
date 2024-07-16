#include <iostream>
#include "Vertex.h"


Vertex::Vertex(shared_ptr<HyperVertex> hyperVertex, int hyperId, string id, int layer, bool isNested) : m_hyperVertex(hyperVertex), m_hyperId(hyperId), m_id(id), m_layer(layer), isNested(isNested) {
    // m_min_in = -1;
    // m_min_out = -1;
    m_degree = 0;
    m_cost = 0;
    m_self_cost = 0;
    hasStrong = false;
    m_strongParent = nullptr;
    scheduledTime = 0;
}

Vertex::~Vertex() {}

const unordered_set<Vertex::ChildVertex, Vertex::ChildVertexHash, Vertex::ChildVertexEqual>& Vertex::getChildren() const { 
    return m_children; 
}

// const set<Vertex::ChildVertex, Vertex::ChildVertexCmp>& Vertex::getChildren() const { 
//     return m_children; 
// }

void Vertex::addChild(Vertex::Ptr child, loom::DependencyType dependency) { 
    m_children.insert({child, dependency}); 
}

void Vertex::printVertex() {
    cout << "VertexId: " << m_id;
    cout << " Cost: " << m_cost;
    cout << " IsNested: " << isNested << endl;

    cout << "ReadSet: ";
    for (auto& read : readSet) {
        cout << read << " ";
    }
    cout << endl;

    cout << "WriteSet: ";
    for (auto& write : writeSet) {
        cout << write << " ";
    }
    cout << endl;

    cout << "CascadeVertices: ";
    for (auto& cascade : cascadeVertices) {
        cout << cascade->m_id << " ";
    }
    cout << endl;
    
    cout << "Children: " << endl;
    for (auto& child : m_children) {
        cout << "Dependency: " << DependencyTypeToString(child.dependency) << endl;
        child.vertex->printVertex();
    }
}

string Vertex::DependencyTypeToString(loom::DependencyType type) {
    switch (type) {
    case loom::DependencyType::STRONG:
        return "STRONG";
    case loom::DependencyType::WEAK:
        return "WEAK";
    default:
        return "UNKNOWN";
    }
}