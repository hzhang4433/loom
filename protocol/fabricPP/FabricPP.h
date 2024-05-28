#pragma once

#include <stack>
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

        void strongConnect(Vertex::Ptr& v, int& index, stack<Vertex::Ptr>& s, unordered_map<Vertex::Ptr, int>& indexMap,
                           unordered_map<Vertex::Ptr, int>& lowLinkMap, unordered_map<Vertex::Ptr, bool>& onStackMap,
                           vector<tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>>& components);
        
        void Johnson(tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& scc, vector<tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>>& cycles);

        bool findCycles(Vertex::Ptr& start, Vertex::Ptr& v, vector<Vertex::Ptr>& stack, tbb::concurrent_unordered_map<Vertex::Ptr, int, Vertex::VertexHash>& blockedMap, tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& blockedSet, vector<tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>>& cycles);

        void unblock(Vertex::Ptr& u, tbb::concurrent_unordered_map<Vertex::Ptr, int, Vertex::VertexHash>& blockedMap, tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& blockedSet);

        int getId();

        int printRollbackTxs();
        
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
        vector<tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>> m_cycles;    
};