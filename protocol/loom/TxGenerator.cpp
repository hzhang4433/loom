#include "TxGenerator.h"
#include "workload/tpcc/Workload.hpp"


// 构造函数
TxGenerator::TxGenerator(int txNum) : id_counter(0), m_txNum(txNum), m_blockSize(Loom::BLOCK_SIZE) {}  

// 生成事务
void TxGenerator::generateWorkload() {
    auto blockNum = m_txNum / m_blockSize;
    for (int i = 0; i < blockNum; i++) {
        auto block = generateBlock();
        m_blocks.push_back(block);
    }
}

// 生成区块
Block::Ptr TxGenerator::generateBlock() {
    Workload workload;
    vector<Vertex::Ptr> txLists;
    vector<HyperVertex::Ptr> txs;   
    unordered_map<string, protocol::RWSets<Vertex::Ptr>> invertedIndex;// 倒排索引
    unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> RWIndex;// rw冲突索引
    unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> conflictIndex;// 冲突索引

    // 生成事务
    for (int i = 0; i < m_blockSize; i++) {
        // 生成TPCC事务
        auto tx = workload.NextTransaction();
        // 构建事务 -- 控制嵌套事务比例
        auto txVertex = generateTransaction(tx, true, invertedIndex);
        // 记录所有子事务
        txLists.insert(txLists.end(), txVertex->m_vertices.begin(), txVertex->m_vertices.end());
        // 记录所有事务
        txs.push_back(txVertex);
    }

    // 生成索引
    generateIndex(txLists, invertedIndex, RWIndex, conflictIndex);
    
    // 生成并返回区块
    return make_shared<Block>(txs, invertedIndex, RWIndex, conflictIndex);
}

// 生成事务
HyperVertex::Ptr TxGenerator::generateTransaction(const Transaction::Ptr& tx, bool isNest, unordered_map<string, protocol::RWSets<Vertex::Ptr>>& invertedIndex) {
    int txid = getId();
    HyperVertex::Ptr hyperVertex = make_shared<HyperVertex>(txid, isNest);
    Vertex::Ptr rootVertex = make_shared<Vertex>(hyperVertex, txid, to_string(txid), 0, isNest);
    // 根据事务结构构建超节点
    string txid_str = to_string(txid);
    
    if (isNest) {
        hyperVertex->buildVertexs(tx, hyperVertex, rootVertex, txid_str, invertedIndex);
        // 记录超节点包含的所有节点
        hyperVertex->m_vertices = rootVertex->cascadeVertices;
        // 根据子节点依赖更新回滚代价和级联子事务
        hyperVertex->m_rootVertex = rootVertex;
        hyperVertex->recognizeCascades(rootVertex);
    } else {
        hyperVertex->buildVertexs(tx, rootVertex, invertedIndex);
        // 添加回滚代价
        rootVertex->m_cost = rootVertex->m_self_cost;
        // 添加自己
        rootVertex->cascadeVertices.insert(rootVertex);
        hyperVertex->m_vertices.insert(rootVertex);
        hyperVertex->m_rootVertex = rootVertex;
    }

    return hyperVertex;
}

// 生成事务索引
void TxGenerator::generateIndex(vector<Vertex::Ptr> txLists, unordered_map<string, protocol::RWSets<Vertex::Ptr>>& invertedIndex, 
                                unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& RWIndex, 
                                unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& conflictIndex) {
    // 利用倒排索引构建RWIndex
    for (auto& kv : invertedIndex) {
        auto& readTxs = kv.second.readSet;
        auto& writeTxs = kv.second.writeSet;
        for (auto& rTx : readTxs) {
            RWIndex[rTx].insert(writeTxs.begin(), writeTxs.end());
            RWIndex[rTx].erase(rTx);
        }
    }

    // 利用RWIndex构建conflictIndex
    for (auto& tx : txLists) {
        // 遍历交易写集，获得并保存与写集冲突的所有交易
        for (auto& wKey : tx->writeSet) {
            auto [readSet, writeSet] = invertedIndex.at(wKey);
            conflictIndex[tx].insert(writeSet.begin(), writeSet.end());
            conflictIndex[tx].insert(readSet.begin(), readSet.end());
        }
        for (auto& rKey : tx->readSet) {
            auto [readSet, writeSet] = invertedIndex.at(rKey);
            conflictIndex[tx].insert(writeSet.begin(), writeSet.end());
        }
        // 排除自己
        conflictIndex[tx].erase(tx);
    }
}

// 获取区块
std::vector<Block::Ptr> TxGenerator::getBlocks() {
    return m_blocks;
}

// 获取全局事务ID
int TxGenerator::getId() {
    return id_counter.fetch_add(1, std::memory_order_relaxed) + 1;
}