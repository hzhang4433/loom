#pragma once

#include <vector>
#include <tbb/concurrent_unordered_map.h>
#include "utils/ThreadPool/UThreadPool.h"
#include "common.h"

using namespace Loom;

class DeterReExecute {
    // 定义公有函数
    public:
        DeterReExecute(std::vector<Vertex::Ptr> rbList, const vector<vector<int>>& serialOrders); // 构造函数

        ~DeterReExecute(){}; // 析构函数

        std::vector<Vertex::Ptr>& getRbList(); // 获取事务列表

        bool canReorder(const Vertex::Ptr& Tx1, const Vertex::Ptr& Tx2); // 判断两个事务是否可调序

        // 时空图模块
        void buildGraph(); // 构建优化时空图
        void buildGraphOrigin(); // 构建原始时空图,即不考虑嵌套事务结构的时空图
        void buildGraphConcurrent(Util::UThreadPoolPtr& Pool); // 并发构建时空图
        void rescheduleTransactions(); // 重调度事务
        void getCandidateTxSet(const Vertex::Ptr& Tx, std::set<Vertex::Ptr, Loom::lessScheduledTime>& Ts); // 获取候选重调度事务集
        void reschedule(Vertex::Ptr& Tx, int startTime); // 重调度事务，移动至时空图目标位置
        void recursiveRescheduleTxs(const Vertex::Ptr& Ti, const Vertex::Ptr& Tx, std::set<string>& movedTxIds, const std::set<Vertex::Ptr>& originalDependencies); // 递归重调度事务


        // 时间计算模块
        int calculateTotalExecutionTime();      // 计算事务总执行时间
        bool isIdle(const Vertex::Ptr& tx, int startTime); // 判断事务是否能够在startTime时刻执行
        int calculateExecutionTime(Vertex::Ptr& Tx); // 计算事务执行时间


    // 定义私有变量
    private:

        std::vector<Vertex::Ptr> m_rbList;                          // 事务列表
        std::unordered_map<int, int> m_orderIndex;                  // 事务顺序索引,用于判断两个事务是否在一个集合中.在一个集合代表无法调序,不在一个集合代表可以调序
        tbb::concurrent_unordered_map<string, std::unordered_set<Vertex::Ptr, Vertex::VertexHash>> m_unConflictTxMap;  // 无冲突事务映射，记录与每个事务不冲突的事务集合
        std::vector<int> originalOrder;                             // 事务原始执行顺序
        std::vector<int> executionOrder;                            // 根据scheduledTime排序的事务执行顺序
        int N;                                                      // 重排序轮次

        // 线程模块
        int m_threadsNum;                                               // 线程数
        int m_totalExecTime;                                            // 总执行时间

};