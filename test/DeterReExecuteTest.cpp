#include <gtest/gtest.h>
#include <chrono>
#include "workload/tpcc/Workload.hpp"
#include "protocol/loom/MinWRollback.h"
#include "protocol/loom/common.h"
#include "protocol/loom/TxGenerator.h"
#include "protocol/loom/DeterReExecute.h"

using namespace std;

TEST(DeterReExecuteTest, TestTimeSpaceGraph) {
    // 定义变量
    Workload workload;
    MinWRollback minw_normal, minw_nested;
    vector<Vertex::Ptr> rbList_normal, rbList_nested;
    vector<vector<int>> serialOrders;
    int executionTime_origin = 0;
    
    // auto seed = uint64_t(140718071595805);
    // workload.set_seed(seed);
    auto seed = workload.get_seed();
    cout << "workload seed: " << seed << endl;

    // 模拟生成事务执行顺序
    vector<int> serialOrder;
    for (int j = 1; j <= Loom::BLOCK_SIZE; j++) {
        serialOrder.push_back(j);
    }
    serialOrders.push_back(serialOrder);

    // 模拟生成事务列表
    set<Vertex::Ptr, Loom::customCompare> rollbackTxs(Loom::customCompare{serialOrder});
    for (int i = 0; i < Loom::BLOCK_SIZE; i++) {
        // 产生交易
        auto tx = workload.NextTransaction();
        // 记录嵌套事务
        auto nested_rbs = minw_nested.execute(tx, true)->m_vertices;
        rollbackTxs.insert(nested_rbs.begin(), nested_rbs.end());
        // 记录嵌套事务的正常形式
        auto normal_rb = minw_normal.execute(tx, false)->m_rootVertex;
        rbList_normal.push_back(normal_rb);
        executionTime_origin += normal_rb->m_cost;
    }
    for (const auto& vertex : rollbackTxs) {
        rbList_nested.push_back(vertex);
    }
    
    // 构造只有普通交易的DeterReExecute对象
    TxGenerator txGenerator_normal(Loom::BLOCK_SIZE), txGenerator_nested(Loom::BLOCK_SIZE);
    unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> RWIndex_normal, RWIndex_nested;// rw冲突索引
    unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash> conflictIndex_normal, conflictIndex_nested;// 冲突索引
    unordered_map<string, set<Vertex::Ptr, Vertex::VertexCompare>> RBIndex_normal, RBIndex_nested;// 回滚索引
    txGenerator_normal.generateIndex(rbList_normal, minw_normal.m_invertedIndex, RWIndex_normal, conflictIndex_normal, RBIndex_normal);

    auto start = chrono::high_resolution_clock::now();
    DeterReExecute deterReExecute_normal(rbList_normal, serialOrders, conflictIndex_normal);
    auto end = chrono::high_resolution_clock::now();
    auto duration_init = chrono::duration_cast<chrono::microseconds>(end - start);
    cout << "Normal Initial Time: " << duration_init.count() / 1000.0 << "ms" << endl;

    // 构造只有嵌套交易的DeterReExecute对象
    txGenerator_nested.generateIndex(rbList_nested, minw_nested.m_invertedIndex, RWIndex_nested, conflictIndex_nested, RBIndex_nested);

    start = chrono::high_resolution_clock::now();
    DeterReExecute deterReExecute_nested(rbList_nested, serialOrders, conflictIndex_nested);
    end = chrono::high_resolution_clock::now();
    duration_init = chrono::duration_cast<chrono::microseconds>(end - start);
    cout << "Nested Initial Time: " << duration_init.count() / 1000.0 << "ms" << endl;
    
    // 构建初始时空图
    start = chrono::high_resolution_clock::now();
    // deterReExecute_normal.buildGraphOrigin();
    deterReExecute_normal.buildGraphOriginByIndex();
    end = chrono::high_resolution_clock::now();
    auto duration_normal = chrono::duration_cast<chrono::microseconds>(end - start);
    // 构建优化时空图
    start = chrono::high_resolution_clock::now();
    // deterReExecute_nested.buildGraph();
    deterReExecute_nested.buildGraphByIndex();
    end = chrono::high_resolution_clock::now();
    auto duration_nested = chrono::duration_cast<chrono::microseconds>(end - start);
    cout << "Normal Build Time: " << duration_normal.count() / 1000.0 << "ms" << endl;
    cout << "Nested Build Time: " << duration_nested.count() / 1000.0 << "ms" << endl;
   
   
    // 计算重执行时间
    auto executionTime_normal = deterReExecute_normal.calculateTotalExecutionTime();
    auto executionTime_nested = deterReExecute_nested.calculateTotalExecutionTime();
    cout << "Origin Execution Time: " << executionTime_origin << endl;
    cout << "Normal Execution Time: " << executionTime_normal << endl;
    cout << "Nested Execution Time: " << executionTime_nested << endl;

    // 计算优化效率
    double originOptimizedPercent = (1.0/executionTime_normal - 1.0/executionTime_origin) * 100.0 / (1.0/executionTime_origin);
    cout << "Origin Optimized Percent: " << originOptimizedPercent << "%" << endl;
    double optimizedPercent = (1.0/executionTime_nested - 1.0/executionTime_normal) * 100.0 / (1.0/executionTime_normal);
    cout << "Optimized Percent: " << optimizedPercent << "%" << endl;
}