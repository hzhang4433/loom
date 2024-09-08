#include <iostream>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <loom/test/TpccTest.cpp>
#include <loom/test/MinWRollbackTest.cpp>
#include <loom/test/FabricPPTest.cpp>
#include <loom/test/CompareTest.cpp>
#include <loom/test/DeterReExecuteTest.cpp>
#include <loom/test/LoomTest.cpp>
#include <loom/test/SerialTest.cpp>
#include <loom/test/AriaTest.cpp>
#include <loom/test/HarmonyTest.cpp>
#include <loom/test/FractalTest.cpp>
#include <loom/test/MossTest.cpp>


using namespace std;

int main(int argc, char** argv) {
#ifdef NDEBUG
    std::cout << "NDEBUG is defined" << std::endl;
#else
    std::cout << "NDEBUG is not defined" << std::endl;
#endif

    // 设置日志输出路径
    FLAGS_log_dir = "/home/z/zh/loom/log";
    // 设置日志级别：0-INFO
    FLAGS_v = 0;

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
    // ::testing::GTEST_FLAG(filter) = "LoomTest.TestOtherPool";
    // ::testing::GTEST_FLAG(filter) = "LoomTest.TestLooptime";
    // ::testing::GTEST_FLAG(filter) = "SerialTest.TestSerial:AriaTest.TestAria";
    // ::testing::GTEST_FLAG(filter) = "AriaTest.TestAria";
    // ::testing::GTEST_FLAG(filter) = "HarmonyTest.TestHarmony";
    // ::testing::GTEST_FLAG(filter) = "FractalTest.TestFractal";
    ::testing::GTEST_FLAG(filter) = "MossTest.TestMoss";
    // ::testing::GTEST_FLAG(filter) = "LoomTest.TestLoom";
    
    
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