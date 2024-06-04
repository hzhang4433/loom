#include <gtest/gtest.h>
#include <chrono>
#include "protocol/fabricPP/FabricPP.h"
#include "protocol/minW/MinWRollback.h"
#include "workload/tpcc/Workload.hpp"

using namespace std;

TEST(CompareTest, TestRollback) {
    Workload workload;
    FabricPP fabricPP;
    MinWRollback minw;
    Transaction::Ptr tx;
    chrono::high_resolution_clock::time_point start, end;

    // workload.set_seed(uint64_t(140707099143341));
    cout << workload.get_seed() << endl;

    for (int i = 0; i < 500; i++) {
        tx = workload.NextTransaction();
        // fabricPP执行
        start = chrono::high_resolution_clock::now();
        fabricPP.execute(tx);
        end = chrono::high_resolution_clock::now();
        cout << "FabricPP Execute time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;

        // minw执行
        start = chrono::high_resolution_clock::now();
        minw.execute(tx);
        end = chrono::high_resolution_clock::now();
        cout << "MinW Execute time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;
    }

    cout << "========================== FabricPP ==========================" << endl;
    start = chrono::high_resolution_clock::now();
    fabricPP.buildGraph();
    end = chrono::high_resolution_clock::now();
    cout << "FabricPP Build time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;

    // fabricPP.printGraph();

    start = chrono::high_resolution_clock::now();
    fabricPP.rollback();
    end = chrono::high_resolution_clock::now();
    cout << "FabricPP Rollback time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;

    int cost_fabric = fabricPP.printRollbackTxs();

    cout << "========================== MinW ==========================" << endl;
    start = chrono::high_resolution_clock::now();
    minw.build();
    end = chrono::high_resolution_clock::now();
    cout << "MinW Build time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;
    
    // minw.printHyperGraph();
    
    start = chrono::high_resolution_clock::now();
    minw.rollback(0);
    end = chrono::high_resolution_clock::now();
    cout << "MinW Rollback time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;

    int cost_minw = minw.printRollbackTxs();

    // 输出优化比
    cout << "Optimization ratio: " << (double)cost_fabric / cost_minw << endl;
    /**/
}