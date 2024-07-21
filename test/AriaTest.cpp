#include <chrono>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <Loom/protocol/aria/Aria.h>
#include <Loom/utils/Generator/UTxGenerator.h>
#include <Loom/utils/UGlogPrefix.hpp>

using namespace std;

TEST(AriaTest, TestAria) {
    // Generate a workload
    TxGenerator txGenerator(loom::BLOCK_SIZE);
    auto blocks = txGenerator.generateWorkload(false);
    // Create a Aria instance
    auto protocol = Aria(blocks, 36, true, 16);
    // Start the protocol
    protocol.Start();
    // Wait for the protocol to finish
    std::this_thread::sleep_for(std::chrono::seconds(2));
    // Stop the protocol
    protocol.Stop();
}