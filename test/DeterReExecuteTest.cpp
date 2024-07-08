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
    MinWRollback minw;
    vector<Vertex::Ptr> rbList_normal, rbList_nested;
    vector<vector<int>> serialOrders;
    
    // 模拟生成事务执行顺序
    vector<int> serialOrder;
    for (int j = 1; j <= Loom::BLOCK_SIZE; j++) {
        serialOrder.push_back(j);
    }
    serialOrders.push_back(serialOrder);

    // 模拟生成事务列表
    set<Vertex::Ptr, Loom::customCompare> rollbackTxs(Loom::customCompare{serialOrder});
    for (int i = 0; i < Loom::BLOCK_SIZE; i++) {
        auto tx = workload.NextTransaction();
        unordered_set<Vertex::Ptr, Vertex::VertexHash> nested_rbs = minw.execute(tx, true)->m_vertices;
        rollbackTxs.insert(nested_rbs.begin(), nested_rbs.end());
        
        auto normal_rb = minw.execute(tx, false)->m_rootVertex;
        rbList_normal.push_back(normal_rb);
    }
    for (const auto& vertex : rollbackTxs) {
        rbList_nested.push_back(vertex);
    }
    
    // 构造DeterReExecute对象
    DeterReExecute deterReExecute_normal(rbList_normal, serialOrders);
    DeterReExecute deterReExecute_nested(rbList_nested, serialOrders);
    // 构建时空图
    deterReExecute_normal.buildGraphOrigin();
    deterReExecute_nested.buildGraph();
    
    // 计算重执行时间
    auto executionTime_normal = deterReExecute_normal.calculateTotalExecutionTime();
    auto executionTime_nested = deterReExecute_nested.calculateTotalExecutionTime();
    cout << "Normal Execution Time: " << executionTime_normal << endl;
    cout << "Nested Execution Time: " << executionTime_nested << endl;

    // 计算优化效率
    double optimizedPercent = (1.0/executionTime_nested - 1.0/executionTime_normal) * 100.0 / (1.0/executionTime_normal);
    cout << "Optimized Percent: " << optimizedPercent << "%" << endl;
}