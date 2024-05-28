#include <iostream>
#include <gtest/gtest.h>
#include "test/TpccTest.cpp"
#include "test/MinWRollbackTest.cpp"
#include "test/FabricPPTest.cpp"
#include "test/CompareTest.cpp"


using namespace std;

int main(int argc, char** argv) {
    /* 启用gtest测试 */
    testing::InitGoogleTest(&argc, argv);
    // ::testing::GTEST_FLAG(filter) = "TpccTest.WorkloadTEST";
    // ::testing::GTEST_FLAG(filter) = "MinWRollbackTest.TestLoopPerformance";
    // ::testing::GTEST_FLAG(filter) = "MinWRollbackTest.TestSCC";
    // ::testing::GTEST_FLAG(filter) = "FabricPPTest.TestRollback";
    ::testing::GTEST_FLAG(filter) = "CompareTest.TestRollback";
    int result = RUN_ALL_TESTS();

    if (result == 0) {
        cout << "All tests passed." << endl;
    } else {
        cout << "Some tests failed." << endl;
    }

    return 0;
}