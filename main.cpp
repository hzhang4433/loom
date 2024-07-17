#include <iostream>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <Loom/test/TpccTest.cpp>
#include <Loom/test/MinWRollbackTest.cpp>
#include <Loom/test/FabricPPTest.cpp>
#include <Loom/test/CompareTest.cpp>
#include <Loom/test/DeterReExecuteTest.cpp>
#include <Loom/test/LoomTest.cpp>


using namespace std;

int main(int argc, char** argv) {
    /* 启用gtest测试 */
    testing::InitGoogleTest(&argc, argv);
    // ::testing::GTEST_FLAG(filter) = "TpccTest.MultiWarehouseTEST";
    // ::testing::GTEST_FLAG(filter) = "MinWRollbackTest.TestConcurrentBuild";
    // ::testing::GTEST_FLAG(filter) = "MinWRollbackTest.TestSerialOrder";
    // ::testing::GTEST_FLAG(filter) = "FabricPPTest.TestRollback";
    // ::testing::GTEST_FLAG(filter) = "CompareTest.TestRollback";
    // ::testing::GTEST_FLAG(filter) = "DeterReExecuteTest.TestTimeSpaceGraph";
    ::testing::GTEST_FLAG(filter) = "LoomTest.TestConcurrentRollback";
    
    int result = RUN_ALL_TESTS();

    if (result == 0) {
        cout << "All tests passed." << endl;
    } else {
        cout << "Some tests failed." << endl;
    }

    return 0;
}