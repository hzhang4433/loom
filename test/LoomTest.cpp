#include <gtest/gtest.h>
#include <chrono>

#include "utils/Generator/UTxGenerator.h"
#include "protocol/loom/MinWRollback.h"
#include "protocol/loom/DeterReExecute.h"

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
        MinWRollback minw(block->getRWIndex());
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
        MinWRollback minw(block->getRWIndex());
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
    // threadpool::Ptr threadPool = std::make_unique<threadpool>((unsigned short)48);
    std::vector<std::future<void>> futures;
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
        std::vector<Vertex::Ptr> normalList, nestedList, rbList;
        serialOrders.reserve(minw.m_sccs.size());
        // 回滚事务
        start = chrono::high_resolution_clock::now();
        // 并发回滚
        for (auto& scc : minw.m_sccs) {
            reExecuteFutures.emplace_back(threadPool->commit([this, &scc, &minw] {
                // 回滚事务
                auto reExecuteInfo = minw.rollbackNoEdge(scc, true);
                // 获得回滚事务并根据事务顺序排序
                set<Vertex::Ptr, loom::customCompare> rollbackTxs(loom::customCompare(reExecuteInfo.m_serialOrder));            
                rollbackTxs.insert(reExecuteInfo.m_rollbackTxs.begin(), reExecuteInfo.m_rollbackTxs.end());
                // 更新m_orderedRollbackTxs
                reExecuteInfo.m_orderedRollbackTxs = std::move(rollbackTxs);
                return reExecuteInfo;
            }));
        }
        // 收集回滚结果
        for (auto& future : reExecuteFutures) {
            auto res = future.get();
            // 获得回滚事务顺序
            // loom::printTxsOrder(res.m_serialOrder);
            serialOrders.push_back(res.m_serialOrder);
            // loom::printRollbackTxs(res.m_orderedRollbackTxs);
            // 将排序后的交易插入rbList
            rbList.insert(rbList.end(), res.m_orderedRollbackTxs.begin(), res.m_orderedRollbackTxs.end());
        }

        // 局部快速回滚
        normalList = rbList;
        // minw.fastNormalRollback(block->getRBIndex(), normalList);
        // cout << "Normal Rollback Size: " << normalList.size() << endl;

        end = chrono::high_resolution_clock::now();
        duration = chrono::duration_cast<chrono::microseconds>(end - start);
        cout << "Rollback Time: " << duration.count() / 1000.0 << "ms" << endl;

        nestedList = rbList;
        // minw.fastRollback(block->getRBIndex(), nestedList);
        // cout << "Nested Rollback Size: " << nestedList.size() << endl;


        // 重调度
        DeterReExecute normalReExecute(normalList, serialOrders, block->getConflictIndex());
        DeterReExecute nestedReExecute(nestedList, serialOrders, block->getConflictIndex());
        // 遍历构图
        start = chrono::high_resolution_clock::now();
        normalReExecute.buildGraphOrigin();
        // normalReExecute.buildGraphOriginByIndex();
        end = chrono::high_resolution_clock::now();
        duration = chrono::duration_cast<chrono::microseconds>(end - start);
        cout << "txList size: " << normalList.size() << ", Normal Build Time: " << duration.count() / 1000.0 << "ms" << endl;
        int normalBuildTime = normalReExecute.calculateTotalNormalExecutionTime();
        // loom::printNormalRollbackTxs(normalList);
        normalReExecute.clearGraph();
        

        start = chrono::high_resolution_clock::now();
        // nestedReExecute.buildGraph();
        nestedReExecute.buildGraphByIndex();
        end = chrono::high_resolution_clock::now();
        duration = chrono::duration_cast<chrono::microseconds>(end - start);
        cout << "txList size: " << nestedList.size() << ", Nested Build Time: " << duration.count() / 1000.0 << "ms" << endl;
        // loom::printNestedRollbackTxs(nestedList);
        int nestedBuildTime = nestedReExecute.calculateTotalExecutionTime();
        nestedReExecute.rescheduleTransactions();
        int nestedRescheduleTime = nestedReExecute.calculateTotalExecutionTime();


        // 重执行
        int serialTime = normalReExecute.calculateSerialTime();
        cout << "Serial Execute Time: " << serialTime << endl;
        cout << "Normal Execute Time: " << normalBuildTime << endl;
        cout << "Nested build Time: " << nestedBuildTime << endl;
        cout << "Nested Execute Time: " << nestedRescheduleTime << endl;


        // 计算优化效率

    }
    
}

TEST(LoomTest, TestLooptime) {
    size_t loopTime = 3500;
    auto start = chrono::high_resolution_clock::now();
    for (int i = 0; i < loopTime; i++) {}
    auto end = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
    cout << "Loop Time: " << duration.count() << "us" << endl;
}