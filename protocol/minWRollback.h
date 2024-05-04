#pragma once

#include <atomic>
#include "../common/HyperVertex.h"

class minWRollback
{
    public:
        minWRollback() : id_counter(0) {}

        ~minWRollback() = default;

        int getId() {
            return id_counter.fetch_add(1, std::memory_order_relaxed) + 1;
        }

        void execute(const Transaction::Ptr& tx);

        void buileGraph(tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& vertices);

        void rollback();

        bool hasConflict(tbb::concurrent_unordered_set<std::string>& set1, tbb::concurrent_unordered_set<std::string>& set2);

        void recursiveUpdate(HyperVertex::Ptr hyperVertex, int min_value, minw::RecursiveType type); // 递归更新

        long long combine(int a, int b);


    private:
        //some type => rollbackTxs;

        std::atomic<int> id_counter;   // 分配事务ID
        // 存储超图中所有子事务节点
        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> m_vertices;
        tbb::concurrent_unordered_map<long long, tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>> m_min2HyperVertex;
};