#include <iostream>
#include "Vertex.h"
#include "HyperVertex.h"


namespace loom {

Vertex::Vertex(shared_ptr<HyperVertex> hyperVertex, int hyperId, string id, int layer, bool isNested) 
: Transaction(hyperVertex), m_hyperId(hyperId), m_id(id), m_layer(layer), isNested(isNested) {
    m_degree = 0;
    m_cost = 0;
    m_self_cost = 0;
    hasStrong = false;
    m_strongParent = nullptr;
    scheduledTime = 0;
    m_should_wait = nullptr;
}

// // 移动构造函数
// Vertex::Vertex(Vertex&& other) noexcept 
//     : Transaction(std::move(other)), 
//         committed(other.committed.load()), 
//         m_hyperId(other.m_hyperId),
//         scheduledTime(other.scheduledTime),
//         hasStrong(other.hasStrong),
//         m_strongChildren(std::move(other.m_strongChildren)),
//         m_self_cost(other.m_self_cost),
//         readSet(std::move(other.readSet)),
//         writeSet(std::move(other.writeSet)),
//         m_should_wait(other.m_should_wait) {
//     // other.committed.store(false); // 重置源对象的 flag
// }

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

void Vertex::Execute() {
    DLOG(INFO) << "subtx: " << this << " execute transaction: " << m_id << std::endl;
    if (getHandler) {
        getHandler(readSet);
    }
    if (setHandler) {
        setHandler(writeSet, "value");
    }
    auto& tx = m_self_cost;
    loom::Exec(tx);
}

}