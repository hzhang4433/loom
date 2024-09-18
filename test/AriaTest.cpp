#include <chrono>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <loom/protocol/aria/Aria.h>
#include <loom/utils/Generator/UTxGenerator.h>
#include <loom/utils/UGlogPrefix.hpp>
#include <loom/utils/Statistic/Statistics.h>

using namespace std;

TEST(AriaTest, TestAria) {
    // Generate a workload
    loom::BLOCK_SIZE = 1000;
    TPCC::N_WAREHOUSES = 60;
    TxGenerator txGenerator(loom::BLOCK_SIZE * 2);
    auto blocks = txGenerator.generateWorkload(false);
    // Create a Statistics instance
    auto statistics = Statistics();
    // Create a Aria instance
    auto protocol = Aria(blocks, statistics, 48, 9973, true);
    // Start the protocol
    protocol.Start();
    // Wait for the protocol to finish
    std::this_thread::sleep_for(std::chrono::seconds(2));
    // Stop the protocol
    protocol.Stop();
    // Print the statistics
    LOG(INFO) << statistics.Print();
    cout << statistics.Print() << endl;
}