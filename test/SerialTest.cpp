#include <chrono>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <loom/protocol/serial/Serial.h>
#include <loom/utils/Generator/UTxGenerator.h>
#include <loom/utils/UGlogPrefix.hpp>
#include <loom/utils/Statistic/Statistics.h>

using namespace std;

TEST(SerialTest, TestSerial) {
    // Generate a workload
    loom::BLOCK_SIZE = 100;
    TPCC::N_WAREHOUSES = 10;
    TxGenerator txGenerator(loom::BLOCK_SIZE * 2);
    auto blocks = txGenerator.generateWorkload(false);
    // Create a Statistics instance
    auto statistics = Statistics();
    // Create a Serial instance
    auto protocol = Serial(blocks, statistics, 1, 36);
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