#include "HyperVertex.h"
#include "Vertex.h"


HyperVertex::HyperVertex(int id) {
    this->hyperId = id;
}

HyperVertex::~HyperVertex() {}

// Vertex::Ptr HyperVertex::getVertexById(const std::string& id) const {
//     auto it = m_vertices.find(id);
//     return it != m_vertices.end() ? it->second : nullptr;
// }

double getCostandCascadeSet(Transaction::Ptr tx, Vertex::Ptr vertex, string txid) {

}

void HyperVertex::buildVertexs(Transaction::Ptr tx, Vertex::Ptr vertex, string txid) {
    // 随机生成执行时间
    double execTime = 1;

    // 如果事务没有子事务，则直接构建一个节点
    if (tx->getChildren().empty()) {
        vertex = make_shared<Vertex>(this, txid, execTime);
    } else {
        vertex = make_shared<Vertex>(this, txid, execTime, true);
        vector<Vertex::Ptr> strongChildren;
        // 递归统计级联回滚权重和级联子节点
        int index = 1;
        for (auto child : tx->getChildren()) {
            string subTxid = txid + "_" + to_string(index);
            Vertex::Ptr childVertex = make_shared<Vertex>(this, subTxid, 1);
            execTime += getCostandCascadeSet(child.transaction, childVertex, subTxid);
            if (child.dependency == minw::DependencyType::STRONG) {
                strongChildren.push_back(childVertex);
            }
            index++;
        }
        
        for (auto child : strongChildren) {
            child->m_cost = vertex->m_cost;
            child->cascadeVertices = vertex->cascadeVertices;
        }
    }
    // 添加读写集
    vertex->readSet = tx->getReadRows();
    vertex->writeSet = tx->getUpdateRows();
}