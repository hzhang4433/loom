#include <chrono>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <loom/protocol/harmony/Harmony.h>
#include <loom/utils/Generator/UTxGenerator.h>
#include <loom/utils/UGlogPrefix.hpp>

using namespace std;

TEST(HarmonyTest, TestHarmony) {
    // Generate a workload
    TxGenerator txGenerator(loom::BLOCK_SIZE);
    auto blocks = txGenerator.generateWorkload(false);
    // Create a Harmony instance
    auto protocol = Harmony(blocks, 36, false, 36);
    // Start the protocol
    protocol.Start();
    // Wait for the protocol to finish
    std::this_thread::sleep_for(std::chrono::seconds(2));
    // Stop the protocol
    protocol.Stop();
}