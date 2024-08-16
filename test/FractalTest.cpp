#include <chrono>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <loom/protocol/fractal/Fractal.h>
#include <loom/utils/Generator/UTxGenerator.h>
#include <loom/utils/UGlogPrefix.hpp>

using namespace std;

TEST(FractalTest, TestFractal) {
    // Generate a workload
    TxGenerator txGenerator(loom::BLOCK_SIZE);
    auto blocks = txGenerator.generateWorkload(false);
    // Create a fractal instance
    auto protocol = Fractal(blocks, 36, 36);
    // Start the protocol
    protocol.Start();
    // Wait for the protocol to finish
    std::this_thread::sleep_for(std::chrono::seconds(2));
    // Stop the protocol
    protocol.Stop();
}