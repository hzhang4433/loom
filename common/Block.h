#pragma once

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include "Transaction.h"

class Block {
    public:
        typedef std::shared_ptr<Block> Ptr;
        
        // 构造函数
        Block(std::vector<Transaction::Ptr> txs, unordered_map<string, loom::RWSets<Vertex::Ptr>> invertedIndex, 
        unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> RWIndex, 
        unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> conflictIndex,
        unordered_map<string, set<Vertex::Ptr, Vertex::VertexCompare>> RBIndex)
         : m_txs(txs), m_invertedIndex(invertedIndex), m_RWIndex(RWIndex), m_conflictIndex(conflictIndex), m_RBIndex(RBIndex) {}
         

        ~Block() = default; // 析构函数

        // 设置区块内事务
        void setTxs(const std::vector<Transaction::Ptr>& txs) {m_txs = txs;}

        // 获取区块内事务
        const std::vector<Transaction::Ptr> getTxs() const {return m_txs;}

        // 设置倒排索引
        void setInvertedIndex(const std::unordered_map<string, loom::RWSets<Vertex::Ptr>>& invertedIndex) {m_invertedIndex = invertedIndex;} 

        // 获取倒排索引
        std::unordered_map<string, loom::RWSets<Vertex::Ptr>> getInvertedIndex() {return m_invertedIndex;}
        
        // 设置rw冲突索引
        void setRWIndex(const std::unordered_map<Vertex::Ptr, std::unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& RWIndex) {m_RWIndex = RWIndex;}
        
        // 获取rw冲突索引
        std::unordered_map<Vertex::Ptr, std::unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> getRWIndex() {return m_RWIndex;}
        
        // 设置冲突索引
        void setConflictIndex(const std::unordered_map<Vertex::Ptr, std::unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& conflictIndex) {m_conflictIndex = conflictIndex;}
        
        // 获取冲突索引
        std::unordered_map<Vertex::Ptr, std::unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> getConflictIndex() {return m_conflictIndex;}

        // 设置回滚索引
        void setRBIndex(const std::unordered_map<string, std::set<Vertex::Ptr, Vertex::VertexCompare>>& RBIndex) {m_RBIndex = RBIndex;}

        // 获取回滚索引
        const std::unordered_map<string, std::set<Vertex::Ptr, Vertex::VertexCompare>> getRBIndex() const {return m_RBIndex;}

        // 获得区块事务信息
        const std::vector<HyperVertex::Ptr> getTxList() const {
            std::vector<HyperVertex::Ptr> txInfo;
            txInfo.reserve(m_txs.size()); // 预留空间
            std::transform(m_txs.begin(), m_txs.end(), std::back_inserter(txInfo),
                        [](const auto& tx) { return tx->GetTx(); });
            return txInfo;
        }

    private:
        // 区块ID
        size_t m_blockId; 
        // 区块内事务
        vector<Transaction::Ptr> m_txs;
        // 倒排索引
        unordered_map<string, loom::RWSets<Vertex::Ptr>> m_invertedIndex;
        // rw冲突索引
        unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> m_RWIndex;
        // 冲突索引
        unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> m_conflictIndex;
        // 回滚索引
        unordered_map<string, set<Vertex::Ptr, Vertex::VertexCompare>> m_RBIndex;
};