#pragma once

#include <atomic>
#include <stack>
#include <boost/heap/fibonacci_heap.hpp>
#include "thread/ThreadPool.h"
#include "thread/threadpool.h"
#include "utils/ThreadPool/UThreadPool.h"
#include "common.h"


using namespace Util;

class MinWRollback
{
    public:
        MinWRollback() : id_counter(0) {}
        
        MinWRollback(std::unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash> hyperVertices, unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> RWIndex) : id_counter(0), m_hyperVertices(hyperVertices), m_RWIndex(RWIndex) {}
        
        MinWRollback(unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> RWIndex) : id_counter(0), m_RWIndex(RWIndex) {}

        ~MinWRollback() = default;

        int getId() {
            return id_counter.fetch_add(1, std::memory_order_relaxed) + 1;
        }

        HyperVertex::Ptr execute(const TPCCTransaction::Ptr& tx, bool isNest = true);

        void onWarm2RWIndex();

        void onWarm2SCC();

        void build(set<Vertex::Ptr, Vertex::VertexCompare>& vertices);

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
        void buildGraphConcurrent(UThreadPoolPtr& Pool, std::vector<std::future<void>>& futures);

        void buildGraphConcurrent(threadpool::Ptr& Pool);

        bool recognizeSCC(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& hyperVertexs, vector<unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>>& sccs);

        void strongconnect(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& hyperVertexs, const HyperVertex::Ptr& v, int& index, stack<HyperVertex::Ptr>& S, unordered_map<HyperVertex::Ptr, int, HyperVertex::HyperVertexHash>& indices,
                   unordered_map<HyperVertex::Ptr, int, HyperVertex::HyperVertexHash>& lowlinks, unordered_map<HyperVertex::Ptr, bool, HyperVertex::HyperVertexHash>& onStack, vector<unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>>& components);

        // void handleNewEdge(Vertex::Ptr& v, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& edges);

        void handleNewEdge(const Vertex::Ptr& v1, const Vertex::Ptr& v2, tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& edges);

        bool isAncester(const string& v1, const string& v2);

        void recursiveUpdate(HyperVertex::Ptr hyperVertex, int min_value, loom::EdgeType type); // 递归更新

        long long combine(int a, int b);
        
        // 计算超节点总回滚代价
        void calculateHyperVertexWeight(const unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, loom::cmp>& pq);
        
        void calculateHyperVertexWeightNoEdge(const unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, loom::cmp>& pq);

        // fibonacci
        void calculateHyperVertexWeightNoEdge(const unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, boost::heap::fibonacci_heap<HyperVertex::Ptr, boost::heap::compare<HyperVertex::compare>>& heap, std::unordered_map<HyperVertex::Ptr, typename std::remove_reference<decltype(heap)>::type::handle_type, HyperVertex::HyperVertexHash>& handles);
        void GreedySelectVertexNoEdge(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, boost::heap::fibonacci_heap<HyperVertex::Ptr, boost::heap::compare<HyperVertex::compare>>& heap, std::unordered_map<HyperVertex::Ptr, typename std::remove_reference<decltype(heap)>::type::handle_type, HyperVertex::HyperVertexHash>& handles, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& result);
        void updateSCCandDependencyNoEdge(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, boost::heap::fibonacci_heap<HyperVertex::Ptr, boost::heap::compare<HyperVertex::compare>>& heap, std::unordered_map<HyperVertex::Ptr, typename std::remove_reference<decltype(heap)>::type::handle_type, HyperVertex::HyperVertexHash>& handles, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs);



        // 计算超节点间一条依赖的回滚代价
        void calculateVertexRollback(HyperVertex::Ptr& hv1, HyperVertex::Ptr hv2, loom::EdgeType type);
        // 计算边权重
        void calculateEdgeRollback(tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& edges, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rollbackVertex);
        
        
        // origin
        void rollback();
        void GreedySelectVertex(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, loom::cmp>& pq, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& result);
        void updateSCCandDependency(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, set<HyperVertex::Ptr, loom::cmp>& pq, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs, tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& calculated);
        void updateHyperVertexWeight(const unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, HyperVertex::Ptr& hyperVertex, const HyperVertex::Ptr& rb, set<HyperVertex::Ptr, loom::cmp>& pq, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& udVertexs, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs, tbb::concurrent_unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& calculated);
        void calculateWeight(HyperVertex::Ptr& hyperVertex, loom::EdgeType& type);
        bool updateVertexRollback(HyperVertex::Ptr& hv1, HyperVertex::Ptr hv2, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs, loom::EdgeType type);
        void updateEdgeRollback(tbb::concurrent_unordered_map<Vertex::Ptr, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& edges, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rollbackVertex, const tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& rollbacked);


        // opt1
        loom::ReExecuteInfo rollbackOpt1(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc);
        void rollbackOpt1Concurrent(UThreadPoolPtr& Pool, std::vector<std::future<loom::ReExecuteInfo>>& futures);
        void GreedySelectVertexOpt1(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, loom::cmp>& pq, unordered_set<Vertex::Ptr, Vertex::VertexHash>& result, vector<int>& queueOrder, stack<int>& stackOrder);
        void updateSCCandDependencyOpt1(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, set<HyperVertex::Ptr, loom::cmp>& pq, const unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs, vector<int>& queueOrder, stack<int>& stackOrder);
        void recursiveDelete(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, loom::cmp>& pq, const HyperVertex::Ptr& rb, const unordered_set<Vertex::Ptr, Vertex::VertexHash>& rbVertexs, unordered_map<HyperVertex::Ptr, loom::EdgeType, HyperVertex::HyperVertexHash>& waitToUpdate, vector<int>& queueOrder, stack<int>& stackOrder);
        

        //opt2:NoEdge
        void rollbackNoEdge();
        void rollbackNoEdge(bool fastMode);
        loom::ReExecuteInfo rollbackNoEdge(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, bool fastMode);
        void rollbackNoEdgeConcurrent(UThreadPoolPtr& Pool, std::vector<std::future<void>>& futures, bool fastMode);
        void GreedySelectVertexNoEdge(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, loom::cmp>& pq, tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash>& result, bool fastMode);
        void GreedySelectVertexNoEdge(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, set<HyperVertex::Ptr, loom::cmp>& pq, unordered_set<Vertex::Ptr, Vertex::VertexHash>& result, vector<int>& queueOrder, stack<int>& stackOrder, bool fastMode);
        void updateSCCandDependencyNoEdge(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, set<HyperVertex::Ptr, loom::cmp>& pq);
        void updateSCCandDependencyNoEdge(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, set<HyperVertex::Ptr, loom::cmp>& pq, vector<int>& queueOrder, stack<int>& stackOrder);
        void updateSCCandDependencyFastMode(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, set<HyperVertex::Ptr, loom::cmp>& pq);
        void updateSCCandDependencyFastMode(unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>& scc, const HyperVertex::Ptr& rb, set<HyperVertex::Ptr, loom::cmp>& pq, vector<int>& queueOrder, stack<int>& stackOrder);


        //opt3:FastRollback
        void fastRollback(const unordered_map<string, set<Vertex::Ptr, Vertex::VertexCompare>>& RBIndex, std::vector<Vertex::Ptr>& rbList);
        void fastNormalRollback(const unordered_map<string, set<Vertex::Ptr, Vertex::VertexCompare>>& RBIndex, std::vector<Vertex::Ptr>& rbList);

        // 打印超图
        void printHyperGraph();
        // 打印回滚事务
        int printRollbackTxs();
        // 打印回滚边信息
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
        std::unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash> m_hyperVertices; 
        // 记录所有可能的scc
        tbb::concurrent_unordered_map<long long, std::unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>> m_min2HyperVertex;
        // 记录所有回滚事务
        tbb::concurrent_unordered_set<Vertex::Ptr, Vertex::VertexHash> m_rollbackTxs;
        // 建立倒排索引
        unordered_map<string, loom::RWSets<Vertex::Ptr>> m_invertedIndex;
        unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> m_RWIndex;
        // 记录图中所有强连通分量
        vector<unordered_set<HyperVertex::Ptr, HyperVertex::HyperVertexHash>> m_sccs;
};