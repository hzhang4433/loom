#pragma once

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include "HyperVertex.h"

class Block {
    public:
        typedef std::shared_ptr<Block> Ptr;
        
        // 构造函数
        Block(std::unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash> txs, unordered_map<string, loom::RWSets<Vertex::Ptr>> invertedIndex, 
        unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> RWIndex, 
        unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> conflictIndex,
        unordered_map<string, set<Vertex::Ptr, Vertex::VertexCompare>> RBIndex)
         : m_txs(txs), m_invertedIndex(invertedIndex), m_RWIndex(RWIndex), m_conflictIndex(conflictIndex), m_RBIndex(RBIndex) {}
         

        ~Block() = default; // 析构函数

        // 设置区块内事务
        void setTxs(const std::unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& txs) {m_txs = txs;}

        // 获取区块内事务
        std::unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash> getTxs() {return m_txs;}

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

    private:
        // 区块内事务
        std::unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash> m_txs;
        // 倒排索引
        unordered_map<string, loom::RWSets<Vertex::Ptr>> m_invertedIndex;
        // rw冲突索引
        unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> m_RWIndex;
        // 冲突索引
        unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> m_conflictIndex;
        // 回滚索引
        unordered_map<string, set<Vertex::Ptr, Vertex::VertexCompare>> m_RBIndex;
};