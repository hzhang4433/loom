#include <gtest/gtest.h>
#include <chrono>

#include "protocol/loom/TxGenerator.h"
#include "protocol/loom/MinWRollback.h"

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
    UThreadPoolPtr tp = UAllocator::safeMallocTemplateCObject<UThreadPool>();
    std::vector<std::future<void>> futures;

    // 执行每个区块
    for (auto& block : blocks) {
        for (auto tx : block->getTxs()) {
            // 并行执行所有交易
        }
        // 执行完成 => 构图 => 回滚
        MinWRollback minw(block->getTxs(), block->getRWIndex());
        minw.buildGraph();
    }
    
    
    
    
    
    
    
    auto start = chrono::high_resolution_clock::now();
    auto end = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
    cout << "Generate Workload Time: " << duration.count() / 1000.0 << "ms" << endl;
}