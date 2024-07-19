#include <iostream>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <Loom/test/TpccTest.cpp>
#include <Loom/test/MinWRollbackTest.cpp>
#include <Loom/test/FabricPPTest.cpp>
#include <Loom/test/CompareTest.cpp>
#include <Loom/test/DeterReExecuteTest.cpp>
#include <Loom/test/LoomTest.cpp>
#include <Loom/test/AriaTest.cpp>


using namespace std;

int main(int argc, char** argv) {
    // 设置日志输出路径
    FLAGS_log_dir = "/home/z/zh/Loom/log";
    // 初始化glog
    google::InitGoogleLogging(argv[0]);

    /* 启用gtest测试 */
    testing::InitGoogleTest(&argc, argv);
    // ::testing::GTEST_FLAG(filter) = "TpccTest.MultiWarehouseTEST";
    // ::testing::GTEST_FLAG(filter) = "MinWRollbackTest.TestConcurrentBuild";
    // ::testing::GTEST_FLAG(filter) = "MinWRollbackTest.TestSerialOrder";
    // ::testing::GTEST_FLAG(filter) = "FabricPPTest.TestRollback";
    // ::testing::GTEST_FLAG(filter) = "CompareTest.TestRollback";
    // ::testing::GTEST_FLAG(filter) = "DeterReExecuteTest.TestTimeSpaceGraph";
    // ::testing::GTEST_FLAG(filter) = "LoomTest.TestConcurrentRollback";
    // ::testing::GTEST_FLAG(filter) = "LoomTest.TestLooptime";
    ::testing::GTEST_FLAG(filter) = "AriaTest.TestAria";
    
    int result = RUN_ALL_TESTS();

    if (result == 0) {
        cout << "All tests passed." << endl;
    } else {
        cout << "Some tests failed." << endl;
    }

    // 关闭glog
    google::ShutdownGoogleLogging();

    return 0;
}