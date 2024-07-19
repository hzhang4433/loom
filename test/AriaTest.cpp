#include <chrono>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <Loom/protocol/aria/Aria.h>
#include <Loom/utils/Generator/UTxGenerator.h>
#include <Loom/utils/UGlog-prefix.hpp>

using namespace std;

TEST(AriaTest, TestAria) {
    // Generate a workload
    TxGenerator txGenerator(loom::BLOCK_SIZE);
    auto blocks = txGenerator.generateWorkload();
    // Create a Aria instance
    auto protocol = Aria(blocks, 8, true);
    // Start the protocol
    protocol.Start();
    // Wait for the protocol to finish
    std::this_thread::sleep_for(std::chrono::seconds(1));
    protocol.Stop();
    
    // auto start = chrono::steady_clock::now();
    // auto end = chrono::steady_clock::now();
    // auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
    // LOG(INFO) << "AriaTest.TestAria: " << duration.count() << " us.";
}