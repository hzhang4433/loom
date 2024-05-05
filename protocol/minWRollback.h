#pragma once

#include <atomic>
#include <set>
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

        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> rollback();

        bool hasConflict(tbb::concurrent_unordered_set<std::string>& set1, tbb::concurrent_unordered_set<std::string>& set2);

        void handleNewEdge(Vertex::Ptr& v, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& edges);

        bool isAncester(const string& v1, const string& v2);

        void recursiveUpdate(HyperVertex::Ptr hyperVertex, int min_value, minw::EdgeType type); // 递归更新

        long long combine(int a, int b);
        
        // 优先队列比较函数: 按照回滚代价从小到大排序
        struct cmp {
            bool operator()(const HyperVertex::Ptr& a, const HyperVertex::Ptr& b) const {
                return a->m_cost < b->m_cost;
            }
        };

        void calculateHyperVertexWeight(tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, cmp>& pq);

        double calculateVertexWeight(HyperVertex::Ptr& hv1, HyperVertex::Ptr hv2, minw::EdgeType type);

        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> GreedySelectVertex(tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, cmp>& pq);

        void updateSCCandDependency(tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, set<HyperVertex::Ptr, cmp>& pq);


    private:
        //some type => rollbackTxs;

        std::atomic<int> id_counter;   // 分配事务ID
        // 存储超图中所有子事务节点
        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> m_vertices;
        tbb::concurrent_unordered_map<long long, tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>> m_min2HyperVertex;
};