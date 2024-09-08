#include <atomic>
#include <loom/utils/Statistic/Statistics.h>
#include <fmt/core.h>
#include <fmt/chrono.h>
#include <fmt/format.h>
#include <iostream>
#include <glog/logging.h>
#include <cstdlib>

namespace loom {

void Statistics::JournalCommit(size_t latency) {
    if (count_commit.load() == 0) {
        begin_time = std::chrono::high_resolution_clock::now();
    } else {
        end_time = std::chrono::high_resolution_clock::now();
    }
    count_commit.fetch_add(1, std::memory_order_relaxed);
    count_latency.fetch_add(latency, std::memory_order_relaxed);
    DLOG(INFO) << "latency: " << latency << std::endl;
}

void Statistics::JournalExecute() {
    count_execution.fetch_add(1, std::memory_order_relaxed);
}

void Statistics::JournalOverheads(size_t count) {
    count_overhead.fetch_add(count, std::memory_order_relaxed);
}

std::string Statistics::Print() {
    // calculate the statistics duration
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - begin_time).count();
    LOG(INFO) << "duration: " << duration << " ms";

    #define LATENCY(X, Y) ((double)(X.load()) / (double)(Y.load()) / (double)(1000))
    #define TPS(X) ((double)(X.load()) / (double)(duration) * (double)(1000))

    
    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    char time_buffer[100];
    std::strftime(time_buffer, sizeof(time_buffer), "%F %T", std::localtime(&now));
    
    return std::string(fmt::format(
        "{}\n"
        "commit             {}\n"
        "execution          {}\n"
        "overhead           {}\n"
        "latency            {:.4f} ms\n"
        "tps                {:.4f} tx/s\n",
        // std::chrono::system_clock::now(),
        time_buffer,
        count_commit.load(),
        count_execution.load(),
        count_overhead.load(),
        LATENCY(count_latency, count_commit),
        TPS(count_commit)
    ));
    #undef TPS
    #undef LATENCY
}

} // namespace spectrum
