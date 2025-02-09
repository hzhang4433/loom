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
#include <loom/test/OptMETest.cpp>
#include <loom/utils/UArgparse.hpp>


using namespace std;

namespace loom {
    size_t BLOCK_SIZE = 1000;
};

namespace TPCC {
    size_t N_WAREHOUSES = 1;
};

int main(int argc, char** argv) {
    // Get protocol name from arguments
    string protocol_name;
    size_t warehouse_num, block_size, thread_num;


    /* parse arguments */
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    // check if the rest arguments have the correct number
    CHECK(argc == 4) << "Except google logging flags, we expect 3 arguments. " << "But we got " << argc - 1 << " ." << std::endl;
    DLOG(WARNING) << "Debug Mode: don't expect good performance. ";
    /* args list:
      for block:
        1. block size
        2. number of blocks
        3. warehouse number
        4. nest or not
      for protocol:
        5. protocol name
        6. thread number
        7. partition number
        8. inter-block / nested-reExecute
      for test:
        9. time for running
    */
    auto statistics = Statistics();
    auto workload = ParseWorkload(argv[2], warehouse_num, block_size);
    auto protocol = ParseProtocol(argv[1], workload, statistics, protocol_name, thread_num);
    auto duration = to<milliseconds>(argv[3]);
    
    /*  init glog  */
    // set log dir
    FLAGS_log_dir = "/home/z/zh/loom/log";
    
    // set log level to info
    FLAGS_v = google::INFO;
    
    // set error threshold to warning
    FLAGS_stderrthreshold = google::WARNING;
    
    // Generate log file prefix
    string log_prefix = protocol_name + "." + to_string(warehouse_num) + "." + to_string(block_size) + "." + to_string(thread_num) + ".";
    google::SetLogDestination(google::INFO, (FLAGS_log_dir + "/" + log_prefix).c_str());
    
    // init google logging
    google::InitGoogleLogging(argv[0]);

    protocol->Start();
    std::this_thread::sleep_for(duration);
    protocol->Stop();
    // print statistics
    cerr << statistics.Print() << endl;
    LOG(INFO) << statistics.Print();
    // showdown glog
    google::ShutdownGoogleLogging();
}