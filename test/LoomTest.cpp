#include <gtest/gtest.h>
#include <chrono>

#include "protocol/loom/TxGenerator.h"
#include "protocol/loom/MinWRollback.h"
#include "protocol/loom/DeterReExecute.h"

using namespace std;

TEST(LoomTest, TestTxGenerator) {
    // 定义变量
    TxGenerator txGenerator(Loom::BLOCK_SIZE * 10);
    auto start = chrono::high_resolution_clock::now();
    auto blocks = txGenerator.generateWorkload();
    auto end = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
    cout << "Generate Workload Time: " << duration.count() / 1000.0 << "ms" << endl;
}

TEST(LoomTest, TestTxGenerator2MinW) {
    // 定义变量
    TxGenerator txGenerator(Loom::BLOCK_SIZE);
    auto blocks = txGenerator.generateWorkload();
    UThreadPoolPtr threadPool = UAllocator::safeMallocTemplateCObject<UThreadPool>();
    std::vector<std::future<void>> futures;

    // 执行每个区块
    for (auto& block : blocks) {
        for (auto tx : block->getTxs()) {
            // 并行执行所有交易
        }
        // 执行完成 => 构图 => 回滚
        MinWRollback minw(block->getTxs(), block->getRWIndex());
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
            // Loom::printTxsOrder(reExecuteInfo.m_serialOrder);
            // 获得回滚事务并根据事务顺序排序
            set<Vertex::Ptr, Loom::customCompare> rollbackTxs(Loom::customCompare(reExecuteInfo.m_serialOrder));
            rollbackTxs.insert(reExecuteInfo.m_rollbackTxs.begin(), reExecuteInfo.m_rollbackTxs.end());
            // 更新m_orderedRollbackTxs
            reExecuteInfo.m_orderedRollbackTxs = std::move(rollbackTxs);
            // Loom::printRollbackTxs(reExecuteInfo.m_orderedRollbackTxs);

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
    TxGenerator txGenerator(Loom::BLOCK_SIZE);
    auto blocks = txGenerator.generateWorkload();
    UThreadPoolPtr threadPool = UAllocator::safeMallocTemplateCObject<UThreadPool>();
    std::vector<std::future<void>> futures;

    // 执行每个区块
    for (auto& block : blocks) {
        for (auto tx : block->getTxs()) {
            // 并行执行所有交易
        }
        // 执行完成 => 构图 => 回滚 => 重调度 => 重执行
        MinWRollback minw(block->getTxs(), block->getRWIndex());
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
        for (auto& scc : minw.m_sccs) {
            // 回滚事务
            auto reExecuteInfo = minw.rollbackNoEdge(scc, true);
            // auto reExecuteInfo = minw.rollbackOpt1(scc);
            // 获得回滚事务顺序
            serialOrders.push_back(reExecuteInfo.m_serialOrder);
            Loom::printTxsOrder(reExecuteInfo.m_serialOrder);
            // 获得回滚事务并根据事务顺序排序
            set<Vertex::Ptr, Loom::customCompare> rollbackTxs(Loom::customCompare(reExecuteInfo.m_serialOrder));            
            rollbackTxs.insert(reExecuteInfo.m_rollbackTxs.begin(), reExecuteInfo.m_rollbackTxs.end());
            // 更新m_orderedRollbackTxs
            reExecuteInfo.m_orderedRollbackTxs = std::move(rollbackTxs);
            Loom::printRollbackTxs(reExecuteInfo.m_orderedRollbackTxs);

            // 将排序后的交易插入rbList
            rbList.insert(rbList.end(), reExecuteInfo.m_orderedRollbackTxs.begin(), reExecuteInfo.m_orderedRollbackTxs.end());
        }


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
        // Loom::printRollbackTxs(rbList);
        normalReExecute.clearGraph();
        
        // Loom::printRollbackTxs(nestedReExecute.getRbList());
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