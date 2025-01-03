#pragma once

#include <vector>
#include <mutex>
#include <tbb/concurrent_unordered_map.h>
#include <loom/utils/ThreadPool/UThreadPool.h>
#include <loom/utils/thread/ThreadPool.h>
#include <loom/protocol/loom/common.h>
#include <tbb/tbb.h>
#include <loom/utils/Statistic/Statistics.h>

using namespace loom;

class DeterReExecute {
    // 定义公有函数
    public:
        
        DeterReExecute(std::vector<Vertex::Ptr>& rbList, vector<vector<int>>& serialOrders, std::unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& conflictIndex); // 构造函数
        
        DeterReExecute(std::vector<HyperVertex::Ptr>& normalList, vector<vector<int>>& serialOrders, std::unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& conflictIndex, std::vector<Vertex::Ptr>& rbList); // 构造函数

        ~DeterReExecute(){}; // 析构函数


        // 时空图模块
        void buildGraph(); // 构建优化时空图
        void buildGraphByIndex();
        void buildGraphOrigin(); // 构建原始时空图,即不考虑嵌套事务结构的时空图
        void buildGraphOriginByIndex();
        void buildGraphConcurrent(Util::UThreadPoolPtr& Pool, std::vector<std::future<void>>& futures); // 并发构建时空图
        void buildAndReScheduleFlat();
        void buildAndReSchedule();
        void buildByWRSetNested(vector<Vertex::Ptr>& txList);
        void rescheduleTransactions(); // 重调度事务
        void getCandidateTxSet(const Vertex::Ptr& Tx, std::set<Vertex::Ptr, loom::lessScheduledTime>& Ts); // 获取候选重调度事务集
        void reschedule(Vertex::Ptr& Tx, int startTime); // 重调度事务，移动至时空图目标位置
        void recursiveRescheduleTxs(const Vertex::Ptr& Ti, const Vertex::Ptr& Tx, std::set<string>& movedTxIds, const std::set<Vertex::Ptr>& originalDependencies); // 递归重调度事务
        void clearGraph(); // 清空时空图

        // 时间计算模块
        int calculateTotalExecutionTime();      // 计算事务总执行时间
        int calculateTotalNormalExecutionTime();      // 计算事务总执行时间
        bool isIdle(const Vertex::Ptr& tx, int startTime); // 判断事务是否能够在startTime时刻执行
        int calculateExecutionTime(Vertex::Ptr& Tx); // 计算事务执行时间
        int calculateNormalExecutionTime(Vertex::Ptr& Tx);
        int calculateSerialTime(); // 计算事务串行执行时间

        // 功能函数模块
        std::vector<Vertex::Ptr>& getRbList(); // 获取事务列表
        void updateDependenciesAndScheduleTime(Vertex::Ptr& Tj, const Vertex::Ptr& Ti, std::unordered_set<Vertex::Ptr, Vertex::VertexHash>& unflictTxs, bool forward);
        bool canReorder(const Vertex::Ptr& Tx1, const Vertex::Ptr& Tx2); // 判断两个事务是否可调序
        static void setNormalList(const vector<Vertex::Ptr>& rbList, vector<Vertex::Ptr>& normalList);
        static void setNormalList(const vector<Vertex::Ptr>& rbList, vector<HyperVertex::Ptr>& normalList);
        void reExcution(Util::UThreadPoolPtr& Pool, std::vector<std::future<void>>& futures, Statistics& statistics);
        void reExcution(ThreadPool::Ptr& Pool, std::vector<std::future<void>>& futures, Statistics& statistics);
        void executeTransaction(const Vertex::Ptr& tx, Statistics& statistics);
        void executeTransactionWithPool(const Vertex::Ptr& tx, Statistics& statistics, ThreadPool::Ptr& Pool, std::atomic<int>& taskCounter);

    // 定义私有变量
    private:
        // 时空图模块
        tbb::concurrent_unordered_map<string, LoomLockEntry<Vertex::Ptr>> m_tsGraph; // 时空图
        // std::unordered_map<string, LoomLockEntry<Vertex::Ptr>> m_tsGraph; // 时空图
        std::vector<Vertex::Ptr>& m_rbList;                         // 事务列表
        std::vector<HyperVertex::Ptr>& m_normalList;                // 普通事务列表
        std::unordered_map<int, int> m_orderIndex;                  // 事务顺序索引,用于判断两个事务是否在一个集合中.在一个集合代表无法调序,不在一个集合代表可以调序
        std::unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& m_conflictIndex; // 读写冲突索引
        std::vector<vector<int>>& m_serialOrders;                    // 事务串行化顺序
        std::unordered_map<Vertex::Ptr, int> m_txOrder;             // 事务到顺序的映射
        // tbb::concurrent_unordered_map<string, std::unordered_set<Vertex::Ptr, Vertex::VertexHash>> m_unConflictTxMap;  // 无冲突事务映射，记录与每个事务不冲突的事务集合
        std::unordered_map<string, std::unordered_set<Vertex::Ptr, Vertex::VertexHash>> m_unConflictTxMap;  // 无冲突事务映射，记录与每个事务不冲突的事务集合
        std::vector<int> originalOrder;                             // 事务原始执行顺序
        std::vector<int> executionOrder;                            // 根据scheduledTime排序的事务执行顺序
        int N;                                                      // 重排序轮次

        // 线程模块
        int m_threadsNum;                                           // 线程数
        int m_totalExecTime;                                        // 总执行时间
        std::mutex mtx;                                             // 用于并发访问 m_tsGraph
        std::unordered_map<Vertex::Ptr, std::vector<Vertex::Ptr>> dependencyGraph; // 记录依赖关系

        // others
        static std::vector<HyperVertex::Ptr> dummyNormalList;

};