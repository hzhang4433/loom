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

        void execute(Transaction::Ptr tx);

        void buileGraph(tbb::concurrent_unordered_set<Vertex::Ptr>& vertices);

        void rollback();

        bool hasConflict(tbb::concurrent_unordered_set<std::string>& set1, tbb::concurrent_unordered_set<std::string>& set2);

        void recursiveUpdate(HyperVertex::Ptr hyperVertex, int min_value, minw::RecursiveType type); // 递归更新

    private:
        //some type => rollbackTxs;

        std::atomic<int> id_counter;   // 分配事务ID
        // 存储超图中所有子事务节点
        tbb::concurrent_unordered_set<Vertex::Ptr> m_vertices;
};