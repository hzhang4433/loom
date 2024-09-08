#include <chrono>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <loom/utils/Generator/UTxGenerator.h>
#include <loom/protocol/loom/MinWRollback.h>
#include <loom/protocol/loom/DeterReExecute.h>
#include <loom/protocol/loom/Loom.h>
#include <loom/utils/Statistic/Statistics.h>


using namespace std;

TEST(LoomTest, TestTxGenerator) {
    // 定义变量
    TxGenerator txGenerator(loom::BLOCK_SIZE * 10);
    auto start = chrono::high_resolution_clock::now();
    auto blocks = txGenerator.generateWorkload(true);
    auto end = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
    cout << "Generate Workload Time: " << duration.count() / 1000.0 << "ms" << endl;
}

TEST(LoomTest, TestTxGenerator2MinW) {
    // 定义变量
    TxGenerator txGenerator(loom::BLOCK_SIZE);
    auto blocks = txGenerator.generateWorkload(true);
    UThreadPoolPtr threadPool = UAllocator::safeMallocTemplateCObject<UThreadPool>();
    std::vector<std::future<void>> futures;

    // 执行每个区块
    for (auto& block : blocks) {
        for (auto tx : block->getTxs()) {
            // 并行执行所有交易
        }
        // 执行完成 => 构图 => 回滚
        MinWRollback minw(block->getTxList(), block->getRWIndex());
        auto start = chrono::high_resolution_clock::now();
        minw.buildGraphNoEdgeC(threadPool, futures);
        auto end = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
        cout << "Build Time: " << duration.count() / 1000.0 << "ms" << endl;
        
        // 识别scc
        minw.onWarm2SCC();
        vector<vector<int>> serialOrders;
        std::vector<Vertex::Ptr> rbList;
        serialOrders.reserve(minw.m_sccs.size());
        // 回滚事务
        start = chrono::high_resolution_clock::now();
        for (auto& scc : minw.m_sccs) {
            // 回滚事务
            auto reExecuteInfo = minw.rollbackNoEdge(scc, true);
            // 获得回滚事务顺序
            serialOrders.push_back(reExecuteInfo.m_serialOrder);
            // loom::printTxsOrder(reExecuteInfo.m_serialOrder);
            // 获得回滚事务并根据事务顺序排序
            set<Vertex::Ptr, loom::customCompare> rollbackTxs(loom::customCompare(reExecuteInfo.m_serialOrder));
            rollbackTxs.insert(reExecuteInfo.m_rollbackTxs.begin(), reExecuteInfo.m_rollbackTxs.end());
            // 更新m_orderedRollbackTxs
            reExecuteInfo.m_orderedRollbackTxs = std::move(rollbackTxs);
            // loom::printRollbackTxs(reExecuteInfo.m_orderedRollbackTxs);

            // 将排序后的交易插入rbList
            rbList.insert(rbList.end(), reExecuteInfo.m_orderedRollbackTxs.begin(), reExecuteInfo.m_orderedRollbackTxs.end());
        }
        end = chrono::high_resolution_clock::now();
        duration = chrono::duration_cast<chrono::microseconds>(end - start);
        cout << "Rollback Time: " << duration.count() / 1000.0 << "ms" << endl;
    }

}

TEST(LoomTest, TestTxGenerator2ReExecute) {
    // 定义变量
    TxGenerator txGenerator(loom::BLOCK_SIZE);
    auto blocks = txGenerator.generateWorkload(true);
    UThreadPoolPtr threadPool = UAllocator::safeMallocTemplateCObject<UThreadPool>();
    // threadpool::Ptr threadPool = std::make_unique<threadpool>((unsigned short)48);
    std::vector<std::future<void>> futures;

    std::this_thread::sleep_for(std::chrono::seconds(5));

    // 执行每个区块
    for (auto& block : blocks) {
        for (auto tx : block->getTxs()) {
            // 并行执行所有交易
        }
        // 执行完成 => 构图 => 回滚 => 重调度 => 重执行
        MinWRollback minw(block->getTxList(), block->getRWIndex());
        auto start = chrono::high_resolution_clock::now();
        minw.buildGraphNoEdgeC(threadPool, futures);
        // minw.buildGraphConcurrent(threadPool, futures);
        auto end = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
        cout << "Build Time: " << duration.count() / 1000.0 << "ms" << endl;
        
        // 识别scc
        minw.onWarm2SCC();
        vector<vector<int>> serialOrders;
        std::vector<Vertex::Ptr> rbList;
        serialOrders.reserve(minw.m_sccs.size());
        // 回滚事务

        start = chrono::high_resolution_clock::now();
        for (auto& scc : minw.m_sccs) {
            // 回滚事务
            auto reExecuteInfo = minw.rollbackNoEdge(scc, true);
            // auto reExecuteInfo = minw.rollbackOpt1(scc);
            // 获得回滚事务顺序
            serialOrders.push_back(reExecuteInfo.m_serialOrder);
            loom::printTxsOrder(reExecuteInfo.m_serialOrder);
            // 获得回滚事务并根据事务顺序排序
            set<Vertex::Ptr, loom::customCompare> rollbackTxs(loom::customCompare(reExecuteInfo.m_serialOrder));            
            rollbackTxs.insert(reExecuteInfo.m_rollbackTxs.begin(), reExecuteInfo.m_rollbackTxs.end());
            // 更新m_orderedRollbackTxs
            reExecuteInfo.m_orderedRollbackTxs = std::move(rollbackTxs);
            loom::printRollbackTxs(reExecuteInfo.m_orderedRollbackTxs);

            // 将排序后的交易插入rbList
            rbList.insert(rbList.end(), reExecuteInfo.m_orderedRollbackTxs.begin(), reExecuteInfo.m_orderedRollbackTxs.end());
        }
        end = chrono::high_resolution_clock::now();
        duration = chrono::duration_cast<chrono::microseconds>(end - start);
        cout << "Rollback Time: " << duration.count() / 1000.0 << "ms" << endl;


        // 重调度
        DeterReExecute normalReExecute(rbList, serialOrders, block->getConflictIndex());
        DeterReExecute nestedReExecute(rbList, serialOrders, block->getConflictIndex());
        // 遍历构图
        start = chrono::high_resolution_clock::now();
        normalReExecute.buildGraphOrigin();
        end = chrono::high_resolution_clock::now();
        duration = chrono::duration_cast<chrono::microseconds>(end - start);
        cout << "txList size: " << rbList.size() << ", Normal Build Time: " << duration.count() / 1000.0 << "ms" << endl;
        int normalBuildTime = normalReExecute.calculateTotalExecutionTime();
        // loom::printRollbackTxs(rbList);
        normalReExecute.clearGraph();
        
        // loom::printRollbackTxs(nestedReExecute.getRbList());
        start = chrono::high_resolution_clock::now();
        // nestedReExecute.buildGraph();
        nestedReExecute.buildGraphByIndex();
        end = chrono::high_resolution_clock::now();
        duration = chrono::duration_cast<chrono::microseconds>(end - start);
        cout << "txList size: " << rbList.size() << ", Nested Build Time: " << duration.count() / 1000.0 << "ms" << endl;
        int nestedBuildTime = nestedReExecute.calculateTotalExecutionTime();

        // 重执行


        int serialTime = nestedReExecute.calculateSerialTime();
        cout << "Serial Execute Time: " << serialTime << endl;
        cout << "Normal Execute Time: " << normalBuildTime << endl;
        cout << "Nested Execute Time: " << nestedBuildTime << endl;


        // 计算优化效率

    }
    
}

TEST(LoomTest, TestConcurrentRollback) {
    // 定义变量
    TxGenerator txGenerator(loom::BLOCK_SIZE);
    auto blocks = txGenerator.generateWorkload(true);
    UThreadPoolPtr threadPool = UAllocator::safeMallocTemplateCObject<UThreadPool>();
    // threadpool::Ptr threadPool = std::make_unique<threadpool>(36);
    std::vector<std::future<void>> futures, TSGFutures;
    std::future<void> futureRbList, futureNestedList;
    std::vector<std::future<loom::ReExecuteInfo>> reExecuteFutures;

    // std::this_thread::sleep_for(std::chrono::seconds(5));

    // 执行每个区块
    for (auto& block : blocks) {
        size_t allTime = 0;
        for (auto tx : block->getTxs()) {
            // 并行执行所有交易
            // 计算交易执行时间
            allTime += tx->GetTx()->m_rootVertex->m_cost;
        }
        cout << "All Time: " << allTime << endl;
        
        // 执行完成 => 构图 => 回滚 => 重调度 => 重执行
        MinWRollback minw(block->getTxList(), block->getRWIndex());
        auto start = chrono::high_resolution_clock::now();
        minw.buildGraphNoEdgeC(threadPool, futures);
        // minw.buildGraphConcurrent(threadPool, futures);
        auto end = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
        cout << "Build Time: " << duration.count() / 1000.0 << "ms" << endl;
        
        // 识别scc
        minw.onWarm2SCC();
        vector<vector<int>> serialOrders;
        std::vector<Vertex::Ptr> rbList, nestedList, normalList;
        // std::vector<HyperVertex::Ptr> normalList;
        serialOrders.reserve(minw.m_sccs.size());
        // 回滚事务
        start = chrono::high_resolution_clock::now();
        // 局部快速回滚
        minw.fastRollback(block->getRBIndex(), rbList);
        // minw.fastRollback(block->getRBIndex(), rbList, nestedList);
        
        // 全局并发回滚
        // cout << "scc size: " << minw.m_sccs.size() << endl;
        for (auto& scc : minw.m_sccs) {
            reExecuteFutures.emplace_back(threadPool->commit([this, &scc, &minw] {
                // 回滚事务
                auto reExecuteInfo = minw.rollbackNoEdge(scc, true);
                // 获得回滚事务并根据事务顺序排序
                set<Vertex::Ptr, loom::customCompare> rollbackTxs(loom::customCompare(reExecuteInfo.m_serialOrder));            
                rollbackTxs.insert(reExecuteInfo.m_rollbackTxs.begin(), reExecuteInfo.m_rollbackTxs.end());
                // 更新m_orderedRollbackTxs
                reExecuteInfo.m_orderedRollbackTxs = std::move(rollbackTxs);
                return std::move(reExecuteInfo);
            }));
        }
        // 收集回滚结果
        for (auto& future : reExecuteFutures) {
            auto res = future.get();
            // 获得回滚事务顺序
            // loom::printTxsOrder(res.m_serialOrder);
            serialOrders.push_back(std::move(res.m_serialOrder));
            // loom::printRollbackTxs(res.m_orderedRollbackTxs);
            // 将排序后的交易插入rbList
            rbList.insert(rbList.end(), std::make_move_iterator(res.m_orderedRollbackTxs.begin()), 
                                                  std::make_move_iterator(res.m_orderedRollbackTxs.end()));
        }

        end = chrono::high_resolution_clock::now();
        duration = chrono::duration_cast<chrono::microseconds>(end - start);
        cout << "Rollback Time: " << duration.count() / 1000.0 << "ms" << endl;
        cout << "Rollback Size: " << rbList.size() << endl;
        
        start = chrono::high_resolution_clock::now();
        DeterReExecute::setNormalList(rbList, normalList);
        end = chrono::high_resolution_clock::now();
        cout << "Normal List Get Time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000.0 << "ms" << endl;

        // 重调度
        DeterReExecute normalReExecute(normalList, serialOrders, block->getConflictIndex());
        DeterReExecute nestedReExecute(rbList, serialOrders, block->getConflictIndex());
        // 遍历构图
        start = chrono::high_resolution_clock::now();
        // normalReExecute.buildGraphOrigin();
        normalReExecute.buildAndReScheduleFlat();
        end = chrono::high_resolution_clock::now();
        cout << "txList size: " << normalList.size() << ", Normal Build Time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000.0 << "ms" << endl;
        int normalBuildTime = normalReExecute.calculateTotalNormalExecutionTime();
        // loom::printRollbackTxs(normalList);
        

        start = chrono::high_resolution_clock::now();
        // nestedReExecute.buildGraph();
        // nestedReExecute.buildGraphByIndex(); // 12ms
        // nestedReExecute.buildGraphConcurrent(threadPool, TSGFutures); // 13ms
        // nestedReExecute.buildByWRSetNested(rbList); // 多线程版：4ms
        nestedReExecute.buildAndReSchedule(); // 1ms

        end = chrono::high_resolution_clock::now();
        cout << "txList size: " << rbList.size() << ", Nested Build Time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000.0 << "ms" << endl;
        int nestedBuildTime = nestedReExecute.calculateTotalNormalExecutionTime();
        // loom::printRollbackTxs(rbList);

        
        // // 重调度
        // start = chrono::high_resolution_clock::now();
        // nestedReExecute.rescheduleTransactions();
        // end = chrono::high_resolution_clock::now();
        // cout << "Reschedule Time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000.0 << "ms" << endl;
        // int nestedRescheduleTime = nestedReExecute.calculateTotalExecutionTime();


        // 重执行
        int serialTime = nestedReExecute.calculateSerialTime();
        cout << "Serial Execute Time: " << serialTime << endl;
        cout << "Normal Execute Time: " << normalBuildTime << endl;
        cout << "Nested build Time: " << nestedBuildTime << endl;
        // cout << "Nested Execute Time: " << nestedRescheduleTime << endl;


        // // 计算优化效率
        double normalEfficient = (1.0 * serialTime / normalBuildTime);
        double nestedEfficient = (1.0 * normalBuildTime / nestedBuildTime);
        cout << "Normal Optimized Percent: " << normalEfficient << endl;
        cout << "Nested Optimized Percent: " << nestedEfficient << endl;

    }
    
}

TEST(LoomTest, TestLooptime) {
    size_t loopTime = 1100;
    volatile int dummy = 0; // 使用volatile防止编译器优化
    auto start = chrono::high_resolution_clock::now();
    // for (int i = 0; i < loopTime; i++) {dummy++;}
    loom::Exec(500);
    auto end = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
    cout << "Loop Time: " << duration.count() << "us" << endl;
}

TEST(LoomTest, TestProtocol) {
    // 定义变量
    TxGenerator txGenerator(loom::BLOCK_SIZE);
    // 生成交易
    auto blocks = txGenerator.generateWorkload(true);
    // 定义线程池
    UThreadPoolPtr threadPool = UAllocator::safeMallocTemplateCObject<UThreadPool>();
    std::vector<std::future<void>> preExecFutures, buildFutures, reExecFutures;
    std::vector<std::future<loom::ReExecuteInfo>> reExecuteFutures;
    // 定义回滚变量
    vector<vector<int>> serialOrders;
    std::vector<Vertex::Ptr> rbList;

    // 定义计时变量
    chrono::steady_clock::time_point start, end;

    // 定义统计变量
    auto statistics = Statistics();

    // 休眠2s
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // 执行每个区块
    for (auto& block : blocks) {
        size_t allTime = block->getTotalCost();
        cout << "Serial Execution Time: " << allTime / 1000.0 << "ms" << endl;
        
        start = chrono::steady_clock::now();
        // 1. 并发执行所有交易
        for (auto& tx : block->getTxs()) {
            preExecFutures.emplace_back(threadPool->commit([this, tx] {
                // read locally from local storage
                tx->InstallGetStorageHandler([&](
                    const std::unordered_set<string>& readSet
                ) {
                    string keys;
                    std::unordered_map<string, string> local_get;
                    for (auto& key : readSet) {
                        keys += key + " ";
                        string value;
                        local_get[key] = value;
                    }
                });
                // write locally to local storage
                tx->InstallSetStorageHandler([&](
                    const std::unordered_set<string>& writeSet,
                    const std::string& value
                ) {
                    string keys;
                    std::unordered_map<string, string> local_put;
                    for (auto& key : writeSet) {
                        keys += key + " ";
                        local_put[key] = value;
                    }
                });
                tx->Execute();
            }));
        }

        /*
        auto txs = block->getTxs();
        size_t txSize = txs.size();
        size_t chunkSize = (txSize + UTIL_DEFAULT_THREAD_SIZE - 1) / (UTIL_DEFAULT_THREAD_SIZE * 1.2);
        for (size_t i = 0; i < txSize; i += chunkSize) {
            // 放入线程池并行执行所有交易
            preExecFutures.emplace_back(threadPool->commit([this, txs, i, chunkSize, txSize] {
                size_t end = std::min(i + chunkSize, txSize);
                for (size_t j = i; j < end; j++) {
                    auto& tx = txs[j];
                    // read locally from local storage
                    tx->InstallGetStorageHandler([&](
                        const std::unordered_set<string>& readSet
                    ) {
                        string keys;
                        std::unordered_map<string, string> local_get;
                        for (auto& key : readSet) {
                            keys += key + " ";
                            string value;
                            local_get[key] = value;
                        }
                    });
                    // write locally to local storage
                    tx->InstallSetStorageHandler([&](
                        const std::unordered_set<string>& writeSet,
                        const std::string& value
                    ) {
                        string keys;
                        std::unordered_map<string, string> local_put;
                        for (auto& key : writeSet) {
                            keys += key + " ";
                            local_put[key] = value;
                        }
                    });
                    tx->Execute();
                }
            }));
        }
        */
        
        // 等待所有交易执行完成
        for (auto& future : preExecFutures) {
            future.get();
        }
        end = chrono::steady_clock::now();
        cout << "Concurrent Pre-Execution Time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000.0 << "ms" << endl;

        // 2. 确定性回滚
        MinWRollback minw(block->getTxList(), block->getRWIndex());
        minw.buildGraphNoEdgeC(threadPool, buildFutures);
        minw.onWarm2SCC();
        // 局部快速回滚
        minw.fastRollback(block->getRBIndex(), rbList);
        // 全局并发回滚
        for (auto& scc : minw.m_sccs) {
            reExecuteFutures.emplace_back(threadPool->commit([this, &scc, &minw] {
                // 回滚事务
                auto reExecuteInfo = minw.rollbackNoEdge(scc, true);
                // 获得回滚事务并根据事务顺序排序
                set<Vertex::Ptr, loom::customCompare> rollbackTxs(loom::customCompare(reExecuteInfo.m_serialOrder));            
                rollbackTxs.insert(reExecuteInfo.m_rollbackTxs.begin(), reExecuteInfo.m_rollbackTxs.end());
                // 更新m_orderedRollbackTxs
                reExecuteInfo.m_orderedRollbackTxs = std::move(rollbackTxs);
                return std::move(reExecuteInfo);
            }));
        }
        // 收集回滚结果
        for (auto& future : reExecuteFutures) {
            auto res = future.get();
            // 获得回滚事务顺序
            // loom::printTxsOrder(res.m_serialOrder);
            serialOrders.push_back(std::move(res.m_serialOrder));
            // loom::printRollbackTxs(res.m_orderedRollbackTxs);
            // 将排序后的交易插入rbList
            rbList.insert(rbList.end(), std::make_move_iterator(res.m_orderedRollbackTxs.begin()), 
                                                  std::make_move_iterator(res.m_orderedRollbackTxs.end()));
        }
        // end = chrono::steady_clock::now();
        // cout << "Rollback Time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000.0 << "ms" << endl;

        // 3. 确定性重执行
        DeterReExecute reExecute(rbList, serialOrders, block->getConflictIndex());
        // end = chrono::steady_clock::now();
        // cout << "step1: " << chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000.0 << "ms" << endl;

        reExecute.buildAndReSchedule(); // 1ms
        // end = chrono::steady_clock::now();
        // cout << "step2: " << chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000.0 << "ms" << endl;

        reExecute.reExcution(threadPool, reExecFutures, statistics);
        
        end = chrono::steady_clock::now();
        cout << "Total Time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000.0 << "ms" << endl;
    }
}

TEST(LoomTest, TestPreExecute) {
    // 定义变量
    TxGenerator txGenerator(loom::BLOCK_SIZE);
    // 生成交易
    auto blocks = txGenerator.generateWorkload(true);
    // 定义线程池
    UThreadPoolPtr threadPool = UAllocator::safeMallocTemplateCObject<UThreadPool>();
    // ThreadPool::Ptr threadPool = make_shared<ThreadPool>(36);
    // threadpool::Ptr threadPool = make_unique<threadpool>(36);

    std::vector<std::future<void>> preExecFutures;
    // 定义计时变量
    chrono::steady_clock::time_point start, end;

    // 休眠2s
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // 执行每个区块
    for (auto& block : blocks) {
        size_t allTime = block->getTotalCost();
        cout << "Serial Execution Time: " << allTime / 1000.0 << "ms" << endl;
        // 事先分好交易并交给不同线程
        vector<vector<Transaction::Ptr>> batches;
        auto& txs = block->getTxs();
        auto tx_per_thread = (txs.size() + UTIL_DEFAULT_THREAD_SIZE - 1) / UTIL_DEFAULT_THREAD_SIZE;
        size_t index = 0;
        batches.reserve(UTIL_DEFAULT_THREAD_SIZE);
        for (size_t j = 0; j < UTIL_DEFAULT_THREAD_SIZE; j++) {
            size_t end = std::min(index + tx_per_thread, txs.size());
            vector<Transaction::Ptr> subBatch;
            // get one thread batch
            for (; index < end; ++index) {
                subBatch.push_back(txs[index]);
            }
            // store thread batch
            batches.push_back(subBatch);
        }

        // 1. 并发执行所有交易
        start = chrono::steady_clock::now();
        for (auto& batch : batches) {
            preExecFutures.emplace_back(threadPool->commit([this, batch] {
                for (auto& tx : batch) {
                    // read locally from local storage
                    tx->InstallGetStorageHandler([&](
                        const std::unordered_set<string>& readSet
                    ) {
                        string keys;
                        std::unordered_map<string, string> local_get;
                        for (auto& key : readSet) {
                            keys += key + " ";
                            string value;
                            local_get[key] = value;
                        }
                    });
                    // write locally to local storage
                    tx->InstallSetStorageHandler([&](
                        const std::unordered_set<string>& writeSet,
                        const std::string& value
                    ) {
                        string keys;
                        std::unordered_map<string, string> local_put;
                        for (auto& key : writeSet) {
                            keys += key + " ";
                            local_put[key] = value;
                        }
                    });
                    tx->Execute();
                }
            }));
        }
        
        // 等待所有交易执行完成
        for (auto& future : preExecFutures) {
            future.get();
        }
        end = chrono::steady_clock::now();
        cout << "Concurrent Pre-Execution Time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000.0 << "ms" << endl;
    }
}

// better version
TEST(LoomTest, TestOtherPool) {
    // 定义变量
    TxGenerator txGenerator(loom::BLOCK_SIZE);
    // 生成交易
    auto blocks = txGenerator.generateWorkload(true);
    // 定义线程池
    // UThreadPoolPtr threadPool = UAllocator::safeMallocTemplateCObject<UThreadPool>();
    ThreadPool::Ptr threadPool = std::make_shared<ThreadPool>(36);
    std::vector<std::future<void>> preExecFutures, buildFutures, reExecFutures;
    std::vector<std::future<loom::ReExecuteInfo>> reExecuteFutures;
    // 定义回滚变量
    vector<vector<int>> serialOrders;
    std::vector<Vertex::Ptr> rbList;

    // 定义计时变量
    chrono::steady_clock::time_point start, end;

    // 定义统计变量
    auto statistics = Statistics();

    // 休眠2s
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // 执行每个区块
    for (auto& block : blocks) {
        size_t allTime = block->getTotalCost();
        cout << "Serial Execution Time: " << allTime / 1000.0 << "ms" << endl;
        
        // 事先分好交易并交给不同线程
        
        // 1. 并发执行所有交易
        auto txs = block->getTxs();
        size_t txSize = txs.size();
        // size_t chunkSize = (txSize + UTIL_DEFAULT_THREAD_SIZE - 1) / (UTIL_DEFAULT_THREAD_SIZE * 1);
        size_t chunkSize = 1;
        
        start = chrono::steady_clock::now();
        // for (size_t i = 0; i < txSize; i += chunkSize) {
        //     // 放入线程池并行执行所有交易
        //     preExecFutures.emplace_back(threadPool->enqueue([this, txs, i, chunkSize, txSize] {
        //         size_t end = std::min(i + chunkSize, txSize);
        //         for (size_t j = i; j < end; j++) {
        //             auto& tx = txs[j];
        //             // read locally from local storage
        //             tx->InstallGetStorageHandler([&](
        //                 const std::unordered_set<string>& readSet
        //             ) {
        //                 string keys;
        //                 std::unordered_map<string, string> local_get;
        //                 for (auto& key : readSet) {
        //                     keys += key + " ";
        //                     string value;
        //                     local_get[key] = value;
        //                 }
        //             });
        //             // write locally to local storage
        //             tx->InstallSetStorageHandler([&](
        //                 const std::unordered_set<string>& writeSet,
        //                 const std::string& value
        //             ) {
        //                 string keys;
        //                 std::unordered_map<string, string> local_put;
        //                 for (auto& key : writeSet) {
        //                     keys += key + " ";
        //                     local_put[key] = value;
        //                 }
        //             });
        //             tx->Execute();
        //         }
        //     }));
        // }
        
        for (size_t i = 0; i < txSize; i++) {
            // 放入线程池并行执行所有交易
            auto tx = txs[i];
            preExecFutures.emplace_back(threadPool->enqueue([this, tx] {
                // read locally from local storage
                tx->InstallGetStorageHandler([&](
                    const std::unordered_set<string>& readSet
                ) {
                    string keys;
                    std::unordered_map<string, string> local_get;
                    for (auto& key : readSet) {
                        keys += key + " ";
                        string value;
                        local_get[key] = value;
                    }
                });
                // write locally to local storage
                tx->InstallSetStorageHandler([&](
                    const std::unordered_set<string>& writeSet,
                    const std::string& value
                ) {
                    string keys;
                    std::unordered_map<string, string> local_put;
                    for (auto& key : writeSet) {
                        keys += key + " ";
                        local_put[key] = value;
                    }
                });
                tx->Execute();
            }));
        }

        // 等待所有交易执行完成
        for (auto& future : preExecFutures) {
            future.get();
        }
        end = chrono::steady_clock::now();
        cout << "Concurrent Pre-Execution Time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000.0 << "ms" << endl;

        // 2. 确定性回滚
        MinWRollback minw(block->getTxList(), block->getRWIndex());
        end = chrono::steady_clock::now();
        cout << "Pass Time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000.0 << "ms" << endl;

        // 2.1 构图
        minw.buildGraphNoEdgeC(threadPool, buildFutures);
        // end = chrono::steady_clock::now();
        // cout << "Build Time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000.0 << "ms" << endl;

        // 2.2 识别scc
        minw.onWarm2SCC();

        // 2.3 回滚事务
        // 局部快速回滚
        minw.fastRollback(block->getRBIndex(), rbList);
        // 全局并发回滚
        for (auto& scc : minw.m_sccs) {
            reExecuteFutures.emplace_back(threadPool->enqueue([this, &scc, &minw] {
                // 回滚事务
                auto reExecuteInfo = minw.rollbackNoEdge(scc, true);
                // 获得回滚事务并根据事务顺序排序
                set<Vertex::Ptr, loom::customCompare> rollbackTxs(loom::customCompare(reExecuteInfo.m_serialOrder));            
                rollbackTxs.insert(reExecuteInfo.m_rollbackTxs.begin(), reExecuteInfo.m_rollbackTxs.end());
                // 更新m_orderedRollbackTxs
                reExecuteInfo.m_orderedRollbackTxs = std::move(rollbackTxs);
                return std::move(reExecuteInfo);
            }));
        }
        // 收集回滚结果
        for (auto& future : reExecuteFutures) {
            auto res = future.get();
            // 获得回滚事务顺序
            serialOrders.push_back(std::move(res.m_serialOrder));
            // 将排序后的交易插入rbList
            rbList.insert(rbList.end(), std::make_move_iterator(res.m_orderedRollbackTxs.begin()), 
                                                  std::make_move_iterator(res.m_orderedRollbackTxs.end()));
        }
        // end = chrono::steady_clock::now();
        // cout << "Rollback Time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000.0 << "ms" << endl;

        // 3. 确定性重执行
        DeterReExecute reExecute(rbList, serialOrders, block->getConflictIndex());
        end = chrono::steady_clock::now();
        cout << "step1: " << chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000.0 << "ms" << endl;

        reExecute.buildAndReSchedule(); // 1ms
        end = chrono::steady_clock::now();
        cout << "step2: " << chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000.0 << "ms" << endl;

        reExecute.reExcution(threadPool, reExecFutures, statistics);
        
        end = chrono::steady_clock::now();
        cout << "Total Time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() / 1000.0 << "ms" << endl;
    }
}

TEST(LoomTest, TestLoom) {
    // Generate a workload
    TxGenerator txGenerator(loom::BLOCK_SIZE * 1);
    auto blocks = txGenerator.generateWorkload(true);
    // Create a Statistics instance
    auto statistics = Statistics();
    // Create a loom instance
    auto protocol = Loom(blocks, statistics, 36, false, false, 36);
    // Start the protocol
    protocol.Start();
    // Wait for the protocol to finish
    std::this_thread::sleep_for(std::chrono::seconds(2));
    // Stop the protocol
    protocol.Stop();
    // Print the statistics
    LOG(INFO) << statistics.Print();
    cout << statistics.Print() << endl;
}