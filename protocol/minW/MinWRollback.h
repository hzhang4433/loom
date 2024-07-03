#pragma once

#include <atomic>
#include <set>
#include <stack>
#include <boost/heap/fibonacci_heap.hpp>
#include "common/HyperVertex.h"
#include "thread/ThreadPool.h"
#include "thread/threadpool.h"
#include "utils/ThreadPool/UThreadPool.h"

using namespace CGraph;

class MinWRollback
{
    public:
        MinWRollback() : id_counter(0) {}

        ~MinWRollback() = default;

        int getId() {
            return id_counter.fetch_add(1, std::memory_order_relaxed) + 1;
        }

        void execute(const Transaction::Ptr& tx, bool isNest = true);

        void onWarm();

        void build(unordered_set<Vertex::Ptr, Vertex::VertexHash>& vertices);

        void onRW(const Vertex::Ptr &rTx, const Vertex::Ptr &wTx);

        void onRWC(const Vertex::Ptr &rTx, const Vertex::Ptr &wTx);

        void onRWNoEdge(const Vertex::Ptr &rTx, const Vertex::Ptr &wTx);

        void onRWCNoEdge(const Vertex::Ptr &rTx, const Vertex::Ptr &wTx);

        void build(Vertex::Ptr& rootVertex);

        void buildGraph();

        void buildGraphSerial();

        void buildGraphNoEdge();

        void buildGraphNoEdgeC(UThreadPoolPtr& Pool, std::vector<std::future<void>>& futures);
        void buildGraphNoEdgeC(ThreadPool::Ptr& Pool, std::vector<std::future<void>>& futures);
        void buildGraphNoEdgeC(threadpool::Ptr& Pool, std::vector<std::future<void>>& futures);

        void buildGraphConcurrent(ThreadPool::Ptr& Pool);

        void buildGraphConcurrent(UThreadPoolPtr& Pool);

        void buildGraphConcurrent(threadpool::Ptr& Pool);

        void rollback(UThreadPoolPtr& Pool, std::vector<std::future<void>>& futures);

        void rollback(threadpool::Ptr& Pool, std::vector<std::future<void>>& futures);

        void rollback(ThreadPool::Ptr& Pool, std::vector<std::future<void>>& futures);

        bool recognizeSCC(tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& hyperVertexs, vector<unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>>& sccs);

        void strongconnect(tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& hyperVertexs, const HyperVertex::Ptr& v, int& index, stack<HyperVertex::Ptr>& S, unordered_map<HyperVertex::Ptr, int, HyperVertex::HyperVertexHash>& indices,
                   unordered_map<HyperVertex::Ptr, int, HyperVertex::HyperVertexHash>& lowlinks, unordered_map<HyperVertex::Ptr, bool, HyperVertex::HyperVertexHash>& onStack, vector<unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>>& components);

        // void handleNewEdge(Vertex::Ptr& v, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& edges);

        void handleNewEdge(const Vertex::Ptr& v1, const Vertex::Ptr& v2, tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& edges);

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
        void calculateHyperVertexWeight(const unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, cmp>& pq);
        
        void calculateHyperVertexWeightNoEdge(const unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, cmp>& pq);

        // fibonacci
        void calculateHyperVertexWeightNoEdge(const unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, boost::heap::fibonacci_heap<HyperVertex::Ptr, boost::heap::compare<HyperVertex::compare>>& heap, std::unordered_map<HyperVertex::Ptr, typename std::remove_reference<decltype(heap)>::type::handle_type, HyperVertex::HyperVertexHash>& handles);
        void GreedySelectVertexNoEdge(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, boost::heap::fibonacci_heap<HyperVertex::Ptr, boost::heap::compare<HyperVertex::compare>>& heap, std::unordered_map<HyperVertex::Ptr, typename std::remove_reference<decltype(heap)>::type::handle_type, HyperVertex::HyperVertexHash>& handles, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& result);
        void updateSCCandDependencyNoEdge(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, boost::heap::fibonacci_heap<HyperVertex::Ptr, boost::heap::compare<HyperVertex::compare>>& heap, std::unordered_map<HyperVertex::Ptr, typename std::remove_reference<decltype(heap)>::type::handle_type, HyperVertex::HyperVertexHash>& handles, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs);



        // 计算超节点间一条依赖的回滚代价
        void calculateVertexRollback(HyperVertex::Ptr& hv1, HyperVertex::Ptr hv2, minw::EdgeType type);
        
        // 计算边权重
        void calculateEdgeRollback(tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& edges, double& weight, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rollbackVertex);
        void calculateEdgeRollback(tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& edges, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rollbackVertex);
        
        // 计算两个集合的差集
        // tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> diff(const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& cascadeVertices, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rollbackVertex);
        
        // origin
        void rollback();
        void GreedySelectVertex(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, cmp>& pq, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& result);
        void updateSCCandDependency(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, set<HyperVertex::Ptr, cmp>& pq, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs, tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& calculated);
        void updateHyperVertexWeight(const unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, HyperVertex::Ptr& hyperVertex, const HyperVertex::Ptr& rb, set<HyperVertex::Ptr, cmp>& pq, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& udVertexs, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs, tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& calculated);
        void calculateWeight(HyperVertex::Ptr& hyperVertex, minw::EdgeType& type);
        bool updateVertexRollback(HyperVertex::Ptr& hv1, HyperVertex::Ptr hv2, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs, minw::EdgeType type);
        void updateEdgeRollback(tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& edges, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rollbackVertex, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rollbacked);


        // opt1
        void rollbackOpt1();
        void GreedySelectVertexOpt1(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, cmp>& pq, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& result);
        void updateSCCandDependencyOpt1(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, set<HyperVertex::Ptr, cmp>& pq, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs);
        void recursiveDelete(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, cmp>& pq, const HyperVertex::Ptr& rb, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs, unordered_map<HyperVertex::Ptr, minw::EdgeType, HyperVertex::HyperVertexHash>& waitToUpdate);
        
        //opt2:NoEdge
        void rollbackNoEdge(bool fastMode);
        void rollbackNoEdge();
        void rollbackNoEdgeConcurrent(UThreadPoolPtr& Pool, std::vector<std::future<void>>& futures, bool fastMode);
        void GreedySelectVertexNoEdge(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, cmp>& pq, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& result, bool fastMode);
        void updateSCCandDependencyNoEdge(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, set<HyperVertex::Ptr, cmp>& pq);
        void updateSCCandDependencyFastMode(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, set<HyperVertex::Ptr, cmp>& pq);

        // 打印超图
        void printHyperGraph();

        // 打印回滚事务
        int printRollbackTxs();

        void printEdgeRollBack(HyperVertex::Ptr& hyperVertex, const tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc);

        

    public:
        // 测试标志
        bool testFlag = true;
        // 统计加边次数
        std::atomic<int> edgeCounter;
        // 分配事务ID
        std::atomic<int> id_counter;   
        // 超图中所有事务节点
        unordered_set<Vertex::Ptr, Vertex::VertexHash> m_vertices; 
        // 超图中所有超节点
        tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash> m_hyperVertices; 
        // 记录所有可能的scc
        tbb::concurrent_unordered_map<long long, tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>> m_min2HyperVertex;
        // 记录所有回滚事务
        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> m_rollbackTxs;
        // 建立倒排索引
        tbb::concurrent_unordered_map<string, protocol::RWSets<Vertex::Ptr>> m_invertedIndex;

        unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> m_RWIndex;

        // fakes
        tbb::concurrent_unordered_map<HyperVertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, HyperVertex::HyperVertexHash> m_out_edges;
        tbb::concurrent_unordered_map<HyperVertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr ,Vertex::VertexHash>, HyperVertex::HyperVertexHash> m_in_edges;
};