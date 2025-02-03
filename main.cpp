#include <iostream>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <gflags/gflags.h>
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
#include <loom/test/AriaERTest.cpp>
#include <loom/utils/UArgparse.hpp>


using namespace std;

namespace loom {
    size_t BLOCK_SIZE = 1000;
};

namespace TPCC {
    size_t N_WAREHOUSES = 1;
};

int main(int argc, char** argv) {
    // set log dir
    FLAGS_log_dir = "/home/z/zh/loom/log";
    // set log level to info
    FLAGS_v = google::INFO;
    // init google logging
    google::InitGoogleLogging(argv[0]);   

    // 启用gtest测试
    testing::InitGoogleTest(&argc, argv);
/**/
    // ::testing::GTEST_FLAG(filter) = "TpccTest.MultiWarehouseTEST";
    // ::testing::GTEST_FLAG(filter) = "MinWRollbackTest.TestConcurrentBuild";
    // ::testing::GTEST_FLAG(filter) = "MinWRollbackTest.TestSerialOrder";
    // ::testing::GTEST_FLAG(filter) = "FabricPPTest.TestRollback";
    // ::testing::GTEST_FLAG(filter) = "CompareTest.TestRollback";
    // ::testing::GTEST_FLAG(filter) = "DeterReExecuteTest.TestTimeSpaceGraph";
    // ::testing::GTEST_FLAG(filter) = "LoomTest.TestOtherPool";
    // ::testing::GTEST_FLAG(filter) = "LoomTest.TestLooptime";
    // ::testing::GTEST_FLAG(filter) = "SerialTest.TestSerial:AriaTest.TestAria";
    // ::testing::GTEST_FLAG(filter) = "SerialTest.TestSerial";
    // ::testing::GTEST_FLAG(filter) = "AriaTest.TestAria";
    ::testing::GTEST_FLAG(filter) = "AriaERTest.TestAriaER";
    // ::testing::GTEST_FLAG(filter) = "HarmonyTest.TestHarmony";
    // ::testing::GTEST_FLAG(filter) = "FractalTest.TestFractal";
    // ::testing::GTEST_FLAG(filter) = "MossTest.TestMoss";
    // ::testing::GTEST_FLAG(filter) = "LoomTest.TestLoom";
    
    int result = RUN_ALL_TESTS();

    if (result == 0) {
        cout << "All tests passed." << endl;
    } else {
        cout << "Some tests failed." << endl;
    }


    // for (int i = 0; i < 10; i++) {
    //     ::testing::GTEST_FLAG(filter) = "MossTest.TestMoss";
    //     cout << "Running test iteration: " << (i + 1) << endl;
    //     int result = RUN_ALL_TESTS();
    // }

    // 关闭glog
    google::ShutdownGoogleLogging();

    return 0;
}