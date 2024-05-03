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

    private:
        //some type => rollbackTxs;

        std::atomic<int> id_counter;   // 分配事务ID
        // 存储所有子事务节点
        tbb::concurrent_unordered_set<Vertex::Ptr> m_vertices;
};