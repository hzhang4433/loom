#include <iostream>
#include <gtest/gtest.h>
#include "test/TpccTest.cpp"
#include "test/minWRollbackTest.cpp"


using namespace std;

int main(int argc, char** argv) {
    /* 启用gtest测试 */
    testing::InitGoogleTest(&argc, argv);
    ::testing::GTEST_FLAG(filter) = "minWRollbackTest.TestPerformance";
    // ::testing::GTEST_FLAG(filter) = "minWRollbackTest.TestSCC";
    // ::testing::GTEST_FLAG(filter) = "TpccTest.WorkloadTEST";
    int result = RUN_ALL_TESTS();

    if (result == 0) {
        cout << "All tests passed." << endl;
    } else {
        cout << "Some tests failed." << endl;
    }

    return 0;
}