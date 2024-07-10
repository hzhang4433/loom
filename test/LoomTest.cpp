#include <gtest/gtest.h>
#include <chrono>

#include "protocol/loom/TxGenerator.h"

using namespace std;

TEST(LoomTest, TestTxGenerator) {
    // 定义变量
    TxGenerator txGenerator(1000);
    auto start = chrono::high_resolution_clock::now();
    txGenerator.generateWorkload();
    auto end = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
    cout << "Generate Workload Time: " << duration.count() / 1000.0 << "ms" << endl;
}