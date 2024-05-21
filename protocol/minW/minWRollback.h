#pragma once

#include <atomic>
#include <set>
#include "common/HyperVertex.h"

class minWRollback
{
    public:
        minWRollback() : id_counter(0) {}

        ~minWRollback() = default;

        int getId() {
            return id_counter.fetch_add(1, std::memory_order_relaxed) + 1;
        }

        void execute(const Transaction::Ptr& tx, bool isNest = true);

        void buildGraph(tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& vertices);

        void rollback();

        bool hasConflict(tbb::concurrent_unordered_set<std::string>& set1, tbb::concurrent_unordered_set<std::string>& set2);

        // void handleNewEdge(Vertex::Ptr& v, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& edges);

        void handleNewEdge(Vertex::Ptr& v1, Vertex::Ptr& v2, tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& edges);

        bool isAncester(const string& v1, const string& v2);

        void recursiveUpdate(HyperVertex::Ptr hyperVertex, int min_value, minw::EdgeType type); // 递归更新

        long long combine(int a, int b);
        
        // 优先队列比较函数: 按照回滚代价从小到大排序
        struct cmp {
            bool operator()(const HyperVertex::Ptr& a, const HyperVertex::Ptr& b) const {
                if (a->m_cost == b->m_cost) {
                    return a->m_hyperId < b->m_hyperId;  // 如果 m_cost 相同，那么 id 小的在前
                }
                return a->m_cost < b->m_cost;
            }
        };

        // 计算超节点总回滚代价
        void calculateHyperVertexWeight(tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, cmp>& pq);

        // 计算超节点间一条依赖的回滚代价
        void calculateVertexRollback(HyperVertex::Ptr& hv1, HyperVertex::Ptr hv2, minw::EdgeType type);
        
        // 计算边权重
        void calculateEdgeRollback(tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& edges, double& weight, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rollbackVertex);
        void calculateEdgeRollback(tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& edges, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rollbackVertex);
        
        // 计算两个集合的差集
        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> diff(const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& cascadeVertices, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rollbackVertex);

        void GreedySelectVertex(tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, cmp>& pq, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& result);

        void updateSCCandDependency(tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, set<HyperVertex::Ptr, cmp>& pq, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs);

        void updateHyperVertexWeight(const tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, HyperVertex::Ptr& hyperVertex, const HyperVertex::Ptr& rb, set<HyperVertex::Ptr, cmp>& pq, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& udVertexs, 
                                     const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs, tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& updated);

        void calculateWeight(HyperVertex::Ptr& hyperVertex, minw::EdgeType& type);
        
        bool updateVertexRollback(HyperVertex::Ptr& hv1, HyperVertex::Ptr hv2, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs, minw::EdgeType type);
        
        void updateEdgeRollback(tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& edges, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rollbackVertex, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rollbacked);

        // 打印超图
        void printHyperGraph();

        // 打印回滚事务
        void printRollbackTxs();

    private:
        bool testFlag = true;
        std::atomic<int> id_counter;   // 分配事务ID
        // 存储超图中所有子事务节点
        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> m_vertices;  // 超图中所有事务节点
        tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash> m_hyperVertices; // 超图中所有超节点
        tbb::concurrent_unordered_map<long long, tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>> m_min2HyperVertex;
        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> minWRollbackTxs;
};