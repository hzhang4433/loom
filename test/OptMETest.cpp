#include <chrono>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <loom/protocol/optme/OptME.h>
#include <loom/utils/Generator/UTxGenerator.h>
#include <loom/utils/UGlogPrefix.hpp>
#include <loom/utils/Statistic/Statistics.h>

using namespace std;

TEST(OptMETest, TestOptME) {
    try {
        // Generate a workload
        loom::BLOCK_SIZE = 1600;
        TPCC::N_WAREHOUSES = 60;
        TxGenerator txGenerator(loom::BLOCK_SIZE * 2);
        auto blocks = txGenerator.generateWorkload(false);
        // Create a Statistics instance
        auto statistics = Statistics();
        // Create a OptME instance
        auto protocol = OptME(blocks, statistics, 48, 9973, false);
        // Start the protocol
        protocol.Start();
        // Wait for the protocol to finish
        std::this_thread::sleep_for(std::chrono::seconds(2));
        // Stop the protocol
        protocol.Stop();
        // Print the statistics
        LOG(INFO) << statistics.Print();
        cout << statistics.Print() << endl;
    } catch (const std::exception& e) {
        std::cerr << "C++ exception with description \"" << e.what() << "\" thrown in the test body." << std::endl;
        raise(SIGSEGV);  // 手动触发崩溃以生成 core 文件
    } catch (...) {
        std::cerr << "Unknown exception thrown in the test body." << std::endl;
        raise(SIGSEGV);  // 手动触发崩溃以生成 core 文件
    }
}