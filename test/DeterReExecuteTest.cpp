#include <gtest/gtest.h>
#include <chrono>
#include "workload/tpcc/Workload.hpp"
#include "protocol/loom/MinWRollback.h"
#include "protocol/loom/common.h"

#include "protocol/loom/DeterReExecute.h"

using namespace std;

TEST(DeterReExecuteTest, TestTimeSpaceGraph) {
    // 定义变量
    Workload workload;
    MinWRollback normal_minw, nested_minw;
    vector<Vertex::Ptr> rbList_normal, rbList_nested;
    vector<vector<int>> serialOrders;
    int executionTime_origin = 0;
    
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
        auto nested_rbs = nested_minw.execute(tx, true)->m_vertices;
        rollbackTxs.insert(nested_rbs.begin(), nested_rbs.end());
        // 记录嵌套事务的正常形式
        auto normal_rb = normal_minw.execute(tx, false)->m_rootVertex;
        rbList_normal.push_back(normal_rb);
        executionTime_origin += normal_rb->m_cost;
    }
    for (const auto& vertex : rollbackTxs) {
        rbList_nested.push_back(vertex);
    }
    
    // 构造DeterReExecute对象
    DeterReExecute deterReExecute_normal(rbList_normal, serialOrders);
    DeterReExecute deterReExecute_nested(rbList_nested, serialOrders);
    // 构建初始时空图
    auto start = chrono::high_resolution_clock::now();
    deterReExecute_normal.buildGraphOrigin();
    auto end = chrono::high_resolution_clock::now();
    auto duration_normal = chrono::duration_cast<chrono::milliseconds>(end - start);
    // 构建优化时空图
    start = chrono::high_resolution_clock::now();
    deterReExecute_nested.buildGraph();
    end = chrono::high_resolution_clock::now();
    auto duration_nested = chrono::duration_cast<chrono::milliseconds>(end - start);
    cout << "Normal Build Time: " << duration_normal.count() << "ms" << endl;
    cout << "Nested Build Time: " << duration_nested.count() << "ms" << endl;
    // 计算重执行时间
    auto executionTime_normal = deterReExecute_normal.calculateTotalExecutionTime();
    auto executionTime_nested = deterReExecute_nested.calculateTotalExecutionTime();
    cout << "Origin Execution Time: " << executionTime_origin << endl;
    cout << "Normal Execution Time: " << executionTime_normal << endl;
    cout << "Nested Execution Time: " << executionTime_nested << endl;

    // 计算优化效率
    double optimizedPercent = (1.0/executionTime_nested - 1.0/executionTime_normal) * 100.0 / (1.0/executionTime_normal);
    cout << "Optimized Percent: " << optimizedPercent << "%" << endl;
}