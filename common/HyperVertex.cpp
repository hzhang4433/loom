#include "HyperVertex.h"
#include "Vertex.h"

using namespace std;

HyperVertex::HyperVertex(int id) {
    this->m_hyperId = id;
    this->m_min_in = INT_MAX;
    this->m_min_out = INT_MAX;
    m_out_edges.resize(minw::BLOCK_SIZE + 1);
    m_in_edges.resize(minw::BLOCK_SIZE + 1);
    m_out_rollback.resize(minw::BLOCK_SIZE + 1);
    m_in_rollback.resize(minw::BLOCK_SIZE + 1);
    m_out_weights.resize(minw::BLOCK_SIZE + 1);
    m_in_weights.resize(minw::BLOCK_SIZE + 1);

    m_out_mapS.resize(minw::BLOCK_SIZE + 1);
    m_in_mapS.resize(minw::BLOCK_SIZE + 1);
    m_out_rollbackMS.resize(minw::BLOCK_SIZE + 1);
    m_in_rollbackMS.resize(minw::BLOCK_SIZE + 1);
}

HyperVertex::~HyperVertex() {}


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

// 构建嵌套事务超节点
int HyperVertex::buildVertexs(const Transaction::Ptr& tx, HyperVertex::Ptr& hyperVertex, Vertex::Ptr& vertex, string& txid, tbb::concurrent_unordered_map<string, protocol::RWSets<Vertex::Ptr>>& invertedIndex) {
    // 获取执行时间
    int execTime = tx->getExecutionTime();
    vertex->m_self_cost = execTime;

    // cout << "txid: " << txid << endl;
    // cout << "child num: " << tx->getChildren().size() << endl;

    // 如果事务有子事务，则递归构建子节点
    if (!tx->getChildren().empty()) {
        vertex->isNested = true;
        // 递归计算子节点权重 级联回滚权重和级联子节点
        auto& children = tx->getChildren();
        for (int i = 1; i <= children.size(); i++) {
            string subTxid = txid + "_" + to_string(i);
            Vertex::Ptr childVertex = make_shared<Vertex>(hyperVertex, this->m_hyperId, subTxid, true);
            // 递归添加级联回滚代价
            execTime += buildVertexs(children[i - 1].transaction, hyperVertex, childVertex, subTxid, invertedIndex);
            // 递归添加级联回滚节点
            vertex->cascadeVertices.insert(childVertex->cascadeVertices.begin(), childVertex->cascadeVertices.end());
            // 添加子节点
            vertex->addChild(childVertex, children[i - 1].dependency);
        }
    }
    // 添加自己
    vertex->cascadeVertices.insert(vertex);
    
    // 添加读写集
    vertex->readSet = tx->getReadRows();
    vertex->writeSet = tx->getUpdateRows();
    auto hv = vertex->m_hyperVertex;
    // 构建倒排索引
    for (auto& readKey : vertex->readSet) {
        invertedIndex[readKey].readSet.insert(vertex);
        // // 构建map的key
        // for (auto& writeV : invertedIndex[readKey].writeSet) {
        //     auto whv = writeV->m_hyperVertex;
        //     if (whv != hv) {
        //         hv->m_out_edges[whv];
        //         whv->m_in_edges[hv];
        //         hv->m_out_rollback[whv];
        //     }
        // }
    }
    for (auto& writeKey : vertex->writeSet) {
        invertedIndex[writeKey].writeSet.insert(vertex);
        // // 构建map的key
        // for (auto& readV : invertedIndex[writeKey].readSet) {
        //     auto rhv = readV->m_hyperVertex;
        //     if (rhv != hv) {
        //         rhv->m_out_edges[hv];
        //         hv->m_in_edges[rhv];
        //         rhv->m_out_rollback[hv];
        //     }
        // }
    }

    // 添加回滚代价
    vertex->m_cost = execTime;
    // cout << "VertexId: " << vertex->m_id << " Cost: " << vertex->m_cost << endl;
    return execTime;
}

// 构建普通事务节点
void HyperVertex::buildVertexs(const Transaction::Ptr& tx, Vertex::Ptr& vertex, tbb::concurrent_unordered_map<string, protocol::RWSets<Vertex::Ptr>>& invertedIndex) {
    // 获取执行时间
    int execTime = tx->getExecutionTime();
    vertex->m_self_cost += execTime;

    // 添加读写集
    vertex->readSet.insert(tx->getReadRows().begin(), tx->getReadRows().end());
    vertex->writeSet.insert(tx->getUpdateRows().begin(), tx->getUpdateRows().end());

    // 构建倒排索引
    for (auto& readKey : tx->getReadRows()) {
        invertedIndex[readKey].readSet.insert(vertex);
    }
    for (auto& writeKey : tx->getUpdateRows()) {
        invertedIndex[writeKey].writeSet.insert(vertex);
    }

    // 如果事务有子事务，则添加子事务读写集和执行时间
    if (!tx->getChildren().empty()) {
        // 递归计算子节点权重 级联回滚权重和级联子节点
        auto& children = tx->getChildren();
        for (int i = 1; i <= children.size(); i++) {
            // 递归添加级联回滚代价
            buildVertexs(children[i - 1].transaction, vertex, invertedIndex);
        }
    }

    // cout << "VertexId: " << vertex->m_id << " Cost: " << vertex->m_cost << endl;
    return;
}

// 递归打印超节点结构树
// m_rootVertex是根节点,从根节点开始递归打印
void HyperVertex::printVertexTree() {
    if (m_rootVertex) {
        m_rootVertex->printVertex();
    }
}