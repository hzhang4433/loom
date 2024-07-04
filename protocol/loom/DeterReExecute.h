// #pragma once

// #include <vector>
// #include <queue>
// #include <tbb/concurrent_unordered_map.h>
// #include <mutex>
// #include <condition_variable>
// #include "ThreadPool.h"


// class TimeSpaceGraph {
//     // 定义公有函数
//     public:
//         TimeSpaceGraph(std::vector<RollBackTx::Ptr> txList, int threads_num) { // 构造函数
//             this->m_txList = txList;
//             this->m_threadsNum = threads_num;
//             // 重排序轮次定义为事务数的20%
//             this->N = txList.size() * 0.2;
//         }

//         ~TimeSpaceGraph(){}; // 析构函数

//         std::vector<RollBackTx::Ptr>& getTxList() { // 获取事务列表
//             return m_txList;
//         }


//         // 时空图模块
//         void buildGraph(); // 构建时空图
//         void buildGraphConcurrent(); // 并发构建时空图
//         void buildGraphConcurrent(ThreadPool::Ptr Pool); // 并发构建时空图
//         void rescheduleTransactions(); // 重调度事务
//         void getCandidateTxSet(const RollBackTx::Ptr& Tx, std::set<RollBackTx::Ptr, RollBackTx::lessScheduledTime>& Ts); // 获取候选重调度事务集
//         void reschedule(RollBackTx::Ptr& Tx, int startTime); // 重调度事务，移动至时空图目标位置
//         void recursiveRescheduleTxs(const RollBackTx::Ptr& Ti, const RollBackTx::Ptr& Tx, std::set<int>& movedTxIds, std::set<RollBackTx::Ptr>& originalDependencies); // 递归重调度事务


//         // 时间计算模块
//         int calculateTotalExecutionTime();      // 计算事务总执行时间
//         bool isIdle(const RollBackTx::Ptr& tx, int startTime); // 判断事务是否能够在startTime时刻执行
//         int calculateExecutionTime(RollBackTx::Ptr& Tx); // 计算事务执行时间


//         // 并发执行模块
//         void executeTransaction(const RollBackTx::Ptr& txn); // 执行单笔事务
//         void executeTransactionsConcurrently(); // 并发执行一批事务
//         void adjustTransactionOrder(std::vector<RollBackTx::Ptr>& txList); // 根据scheduledTime对事务进行排序
//         std::vector<int> topologicalSort(); // 获得事务的拓扑排序——可能不再需要了
//         void updateDependenciesAndQueue(int completed_id, std::vector<bool>& completed); // 更新依赖关系和队列


//     // 定义私有变量
//     private:
//         std::vector<RollBackTx::Ptr> m_txList;                          // 事务列表
//         tbb::concurrent_unordered_map<int, std::vector<RollBackTx::Ptr>> m_unConflictTxMap;  // 无冲突事务映射，记录与每个事务不冲突的事务集合
//         std::vector<int> originalOrder;                                 // 事务原始执行顺序
//         std::vector<int> executionOrder;                                // 根据scheduledTime排序的事务执行顺序
//         int N;                                                          // 重排序轮次

//         std::mutex queue_mutex;                                         // 互斥锁
//         std::condition_variable cv;                                     // 条件变量
//         std::queue<int> ready_queue;                                    // 待执行事务队列
//         int m_threadsNum;                                               // 线程数
//         int m_totalExecTime;                                            // 总执行时间

// };