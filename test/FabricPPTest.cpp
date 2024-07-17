#include <gtest/gtest.h>
#include <chrono>
#include "protocol/fabricPP/FabricPP.h"
#include "workload/tpcc/Workload.hpp"

using namespace std;


TEST(FabricPPTest, TestRollback) {
    Workload workload;
    FabricPP fabricPP;
    TPCCTransaction::Ptr tx;
    chrono::high_resolution_clock::time_point start, end;
    
    workload.set_seed(uint64_t(0));

    for (int i = 0; i < 6; i++) {
        tx = workload.NextTransaction();
        start = chrono::high_resolution_clock::now();
        fabricPP.execute(tx);
        end = chrono::high_resolution_clock::now();
        cout << "Execute time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;
    }

    start = chrono::high_resolution_clock::now();
    fabricPP.buildGraph();
    end = chrono::high_resolution_clock::now();
    cout << "Build time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;

    start = chrono::high_resolution_clock::now();
    fabricPP.rollback();
    end = chrono::high_resolution_clock::now();
    cout << "Rollback time: " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "us" << endl;

    fabricPP.printRollbackTxs();    
}