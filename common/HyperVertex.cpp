#include "HyperVertex.h"
#include "Vertex.h"


HyperVertex::HyperVertex(int id) {
    this->m_hyperId = id;
    this->m_min_in = UINT64_MAX;
    this->m_min_out = UINT64_MAX;
}

HyperVertex::~HyperVertex() {}

// Vertex::Ptr HyperVertex::getVertexById(const std::string& id) const {
//     auto it = m_vertices.find(id);
//     return it != m_vertices.end() ? it->second : nullptr;
// }

void HyperVertex::recognizeCascades(Vertex::Ptr vertex) {
    // 递归识别并更新级联子事务
    for (auto& child : vertex->getChildren()) {
        // 更新回滚代价
        if (child.dependency == minw::DependencyType::STRONG) {
            child.vertex->m_cost = vertex->m_cost;
            child.vertex->cascadeVertices = vertex->cascadeVertices;
        }
        // 递归识别并更新级联子事务
        recognizeCascades(child.vertex);
    }
}

double HyperVertex::buildVertexs(Transaction::Ptr tx, Vertex::Ptr vertex, string txid) {
    // 获取执行时间
    double execTime = 1;
    // double execTime = tx->getExecTime();

    // 如果事务有子事务，则递归构建子节点
    if (!tx->getChildren().empty()) {
        vertex->isNested = true;
        // 递归计算子节点权重 级联回滚权重和级联子节点
        auto& children = tx->getChildren();
        for (int i = 1; i <= children.size(); i++) {
            string subTxid = txid + "_" + to_string(i);
            Vertex::Ptr childVertex = make_shared<Vertex>(this, this->m_hyperId, subTxid, true);
            // 递归添加级联回滚代价
            execTime += buildVertexs(children[i - 1].transaction, childVertex, subTxid);
            // 递归添加级联回滚节点
            vertex->cascadeVertices.insert(childVertex->cascadeVertices.begin(), childVertex->cascadeVertices.end());
            // 添加子节点
            vertex->addChild(childVertex, children[i - 1].dependency);
        }
        // 添加自己
        vertex->cascadeVertices.insert(vertex);
    }
    // 添加读写集
    vertex->readSet = tx->getReadRows();
    vertex->writeSet = tx->getUpdateRows();
    // 添加回滚代价
    vertex->m_cost = execTime;
    return execTime;
}