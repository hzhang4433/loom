#include "UTxGenerator.h"
#include "workload/tpcc/Workload.hpp"


// 构造函数
TxGenerator::TxGenerator(int txNum) : id_counter(0), m_txNum(txNum), m_blockSize(loom::BLOCK_SIZE) {}  

// 生成事务
std::vector<Block::Ptr> TxGenerator::generateWorkload(bool isNest) {
    auto blockNum = m_txNum / m_blockSize;
    for (int i = 0; i < blockNum; i++) {
        auto block = generateBlock(isNest);
        m_blocks.push_back(block);
    }
    return m_blocks;
}

// 生成区块
Block::Ptr TxGenerator::generateBlock(bool isNest) {
    Workload workload;
    vector<Vertex::Ptr> txLists;
    std::vector<Transaction::Ptr> txs;
    unordered_map<string, loom::RWSets<Vertex::Ptr>> invertedIndex;// 倒排索引
    unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> RWIndex;// rw冲突索引
    unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> conflictIndex;// 冲突索引
    unordered_map<string, set<Vertex::Ptr, Vertex::VertexCompare>> RBIndex;// 回滚索引

    workload.set_seed(uint64_t(140716047624829));
    auto seed = workload.get_seed();
    cout << "seed: " << seed << endl;

    // 生成事务
    // loom::Random random = workload.get_random();
    // loom::Random tx_random = workload.get_tx_random();
    // auto NO_txGenerator = std::make_shared<NewOrderTransaction>(tx_random);
    // auto P_txGenerator = std::make_shared<PaymentTransaction>(tx_random);
    // auto D_txGenerator = std::make_shared<DeliveryTransaction>(tx_random);
    // auto OS_txGenerator = std::make_shared<OrderStatusTransaction>(tx_random);
    // auto SL_txGenerator = std::make_shared<StockLevelTransaction>(tx_random);

    for (int i = 0; i < m_blockSize; i++) {
        // 生成TPCC事务
        auto tx = workload.NextTransaction();
        // cout << "tx " << i + 1 << " type: " << TPCC::transactionTypeToString(tx->getType()) << endl;
        // // 构建事务 -- 控制嵌套事务比例
        // HyperVertex::Ptr txVertex;
        // if (tx->getType() == TPCC::TransactionType::PAYMENT) {
        //     txVertex = generateTransaction(tx, false, invertedIndex);
        // } else {
        //     txVertex = generateTransaction(tx, true, invertedIndex);
        // }

        // // 全部生成NewOrder事务
        // auto tx = NO_txGenerator->makeTransaction();

        // // 48%的NewOrder事务,48%的Payment事务,4%的Delivery事务
        // TPCCTransaction::Ptr tx;
        // uint64_t option = random.uniform_dist(1, 100);
        // if (option <= 49) {
        //     tx = NO_txGenerator->makeTransaction();
        // } 
        // else if (option <= 96) {
        //     tx = P_txGenerator->makeTransaction();
        // } 
        // else {
        //     tx = D_txGenerator->makeTransaction();
        // }
        // cout << "tx " << i + 1 << " type: " << TPCC::transactionTypeToString(tx->getType()) << endl;
        HyperVertex::Ptr txVertex = generateTransaction(tx, isNest, invertedIndex);
        
        // 记录所有子事务
        txLists.insert(txLists.end(), txVertex->m_vertices.begin(), txVertex->m_vertices.end());
        // 记录所有事务
        txs.push_back(make_shared<Transaction>(txVertex));
    }

    // 生成索引
    generateIndex(txLists, invertedIndex, RWIndex, conflictIndex, RBIndex);
    
    // 生成并返回区块
    return make_shared<Block>(txs, invertedIndex, RWIndex, conflictIndex, RBIndex);
}

// 生成事务
HyperVertex::Ptr TxGenerator::generateTransaction(const TPCCTransaction::Ptr& tx, bool isNest, unordered_map<string, loom::RWSets<Vertex::Ptr>>& invertedIndex) {
    int txid = getId();
    HyperVertex::Ptr hyperVertex = make_shared<HyperVertex>(txid, isNest);
    Vertex::Ptr rootVertex = make_shared<Vertex>(hyperVertex, txid, to_string(txid), 0, isNest);
    // 根据事务结构构建超节点
    string txid_str = to_string(txid);
    
    if (isNest) {
        hyperVertex->buildVertexs(tx, hyperVertex, rootVertex, txid_str, invertedIndex);
        // 记录超节点包含的所有节点
        hyperVertex->m_vertices.insert(rootVertex->cascadeVertices.begin(), rootVertex->cascadeVertices.end());
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
void TxGenerator::generateIndex(vector<Vertex::Ptr> txLists, unordered_map<string, loom::RWSets<Vertex::Ptr>>& invertedIndex, 
                                unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& RWIndex, 
                                unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& conflictIndex,
                                unordered_map<string, set<Vertex::Ptr, Vertex::VertexCompare>>& RBIndex) {
    // 利用倒排索引构建RWIndex
    for (auto& kv : invertedIndex) {
        auto& readTxs = kv.second.readSet;
        auto& writeTxs = kv.second.writeSet;
        // 判断是否有写事务，若没有写事务则跳过
        if (writeTxs.empty()) {
            continue;
        }
        // 针对"Dytd-", "S-", "Cdelivery-"开头的key, 构建RBIndex
        if (kv.first.substr(0, 5) == "Dytd-" || kv.first.substr(0, 2) == "S-" || kv.first.substr(0, 10) == "Cdelivery-") {
            // 找到在readTxs和writeTxs中都存在的事务
            set<Vertex::Ptr, Vertex::VertexCompare> intersectTxs;
            for (auto& rTx : readTxs) {
                if (writeTxs.find(rTx) != writeTxs.end()) {
                    intersectTxs.insert(rTx);
                }
            }
            if (intersectTxs.size() <= 1) {
                continue;
            }
            // 将这些事务加入RBIndex
            RBIndex[kv.first] = intersectTxs;
            continue;
        }
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