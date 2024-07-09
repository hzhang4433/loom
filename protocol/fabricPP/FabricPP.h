#pragma once

#include <stack>
#include <set>
#include <atomic>
#include "common/HyperVertex.h"

class FabricPP {
    public:
        FabricPP() : id_counter(0) {};

        ~FabricPP() = default;

        void execute(const Transaction::Ptr& tx);

        void buildGraph();

        void rollback();

        bool Tarjan(tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& vertices, vector<tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>>& sccs);

        void strongConnect(const Vertex::Ptr& v, int& index, stack<Vertex::Ptr>& s, unordered_map<Vertex::Ptr, int>& indexMap,
                           unordered_map<Vertex::Ptr, int>& lowLinkMap, unordered_map<Vertex::Ptr, bool>& onStackMap,
                           vector<tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>>& components);
        
        void Johnson(tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& scc, vector<set<Vertex::Ptr, Vertex::VertexCompare>>& cycles);
        
        void findCycles(Vertex::Ptr& start, const Vertex::Ptr& v, vector<Vertex::Ptr>& stack, tbb::concurrent_unordered_map<Vertex::Ptr, bool, Vertex::VertexHash>& blockedMap, tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& blockedSet, vector<set<Vertex::Ptr, Vertex::VertexCompare>>& cycles, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& scc);

        void unblock(Vertex::Ptr& v, tbb::concurrent_unordered_map<Vertex::Ptr, bool, Vertex::VertexHash>& blockedMap, tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& blockedSet);

        int getId();

        int printRollbackTxs();

        void printGraph();
        
    private:
        // 分配事务ID
        std::atomic<int> id_counter; 
        // 记录超图中所有子事务节点
        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> m_vertices;
        // 记录超图中所有超节点
        tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash> m_hyperVertices; 
        // 记录所有可能的scc
        tbb::concurrent_unordered_map<long long, tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>> m_min2HyperVertex;
        // 记录所有回滚事务
        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> m_rollbackTxs;
        // 记录所有环路
        vector<set<Vertex::Ptr, Vertex::VertexCompare>> m_cycles;   
        int testCounter = 0; 
        // 建立倒排索引
        std::unordered_map<string, protocol::RWSets<Vertex::Ptr>> m_invertedIndex;
};