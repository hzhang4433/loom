#include <chrono>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <loom/protocol/moss/Moss.h>
#include <loom/utils/Generator/UTxGenerator.h>
#include <loom/utils/UGlogPrefix.hpp>
#include <loom/utils/Statistic/Statistics.h>

using namespace std;

TEST(MossTest, TestMoss) {
    // Generate a workload
    loom::BLOCK_SIZE = 1000;
    TPCC::N_WAREHOUSES = 60;
    TxGenerator txGenerator(loom::BLOCK_SIZE * 2);
    auto blocks = txGenerator.generateWorkload(true);
    // Create a Statistics instance
    auto statistics = Statistics();
    // Create a moss instance
    auto protocol = Moss(blocks, statistics, 48, 9973);
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