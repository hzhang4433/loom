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
#include <loom/utils/UArgparse.hpp>


using namespace std;

int main(int argc, char** argv) {
    // set log dir
    FLAGS_log_dir = "/home/z/zh/loom/log";
    // set log level to info
    FLAGS_v = google::INFO;
    // set error threshold to warning
    FLAGS_stderrthreshold = google::WARNING;
    // init google logging
    google::InitGoogleLogging(argv[0]);
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
    auto workload = ParseWorkload(argv[2]);
    auto protocol = ParseProtocol(argv[1], workload, statistics);
    auto duration = to<milliseconds>(argv[3]);
    protocol->Start();
    std::this_thread::sleep_for(duration);
    protocol->Stop();
    // print statistics
    cerr << statistics.Print() << endl;
    // showdown glog
    google::ShutdownGoogleLogging();
}