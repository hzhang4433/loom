#pragma once

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include "HyperVertex.h"

class Block {
    public:
        typedef std::shared_ptr<Block> Ptr;
        
        // 构造函数
        Block(std::vector<HyperVertex::Ptr> txs, unordered_map<string, protocol::RWSets<Vertex::Ptr>> invertedIndex, 
        unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> RWIndex, 
        unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> conflictIndex)
         : m_txs(txs), m_invertedIndex(invertedIndex), m_RWIndex(RWIndex), m_conflictIndex(conflictIndex) {}
         

        ~Block() = default; // 析构函数

        void setTxs(const std::vector<HyperVertex::Ptr>& txs); // 设置区块内事务

        std::vector<HyperVertex::Ptr> getTxs(); // 获取区块内事务

        // 设置倒排索引
        void setInvertedIndex(const std::unordered_map<string, protocol::RWSets<Vertex::Ptr>>& invertedIndex) {m_invertedIndex = invertedIndex;} 

        // 获取倒排索引
        std::unordered_map<string, protocol::RWSets<Vertex::Ptr>> getInvertedIndex() {return m_invertedIndex;}
        
        // 设置rw冲突索引
        void setRWIndex(const std::unordered_map<Vertex::Ptr, std::unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& RWIndex) {m_RWIndex = RWIndex;}
        
        // 获取rw冲突索引
        std::unordered_map<Vertex::Ptr, std::unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> getRWIndex() {return m_RWIndex;}
        
        // 设置冲突索引
        void setConflictIndex(const std::unordered_map<Vertex::Ptr, std::unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& conflictIndex) {m_conflictIndex = conflictIndex;}
        
        // 获取冲突索引
        std::unordered_map<Vertex::Ptr, std::unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> getConflictIndex() {return m_conflictIndex;}

    private:
        // 区块内事务
        std::vector<HyperVertex::Ptr> m_txs;
        // 倒排索引
        unordered_map<string, protocol::RWSets<Vertex::Ptr>> m_invertedIndex;
        // rw冲突索引
        unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> m_RWIndex;
        // 冲突索引
        unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> m_conflictIndex;
};