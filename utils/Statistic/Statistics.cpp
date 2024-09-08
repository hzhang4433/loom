#include <atomic>
#include <loom/utils/Statistic/Statistics.h>
#include <fmt/core.h>
#include <fmt/chrono.h>
#include <fmt/format.h>
#include <iostream>
#include <glog/logging.h>
#include <cstdlib>
#include <iomanip>

namespace loom {

void Statistics::JournalCommit(size_t latency) {
    if (count_commit.load() == 0) {
        begin_time = std::chrono::high_resolution_clock::now();
    } else {
        end_time = std::chrono::high_resolution_clock::now();
    }
    count_commit.fetch_add(1, std::memory_order_relaxed);
    count_latency.fetch_add(latency, std::memory_order_relaxed);
    DLOG(INFO) << "latency: " << latency << "us";
}

void Statistics::JournalExecute() {
    count_execution.fetch_add(1, std::memory_order_relaxed);
}

void Statistics::JournalOverheads(size_t count) {
    count_overhead.fetch_add(count, std::memory_order_relaxed);
}

std::string Statistics::Print() {
    // calculate the statistics duration
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - begin_time).count();
    DLOG(INFO) << std::fixed << std::setprecision(3) << "duration: " << duration / (double)(1000) << "ms";

    #define LATENCY(X, Y) ((double)(X.load()) / (double)(Y.load()) / (double)(1000))
    #define TPS(X) ((double)(X.load()) / (double)(duration) * (double)(1000000))

    
    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    char time_buffer[100];
    std::strftime(time_buffer, sizeof(time_buffer), "%F %T", std::localtime(&now));
    
    return std::string(fmt::format(
        "{}\n"
        "commit             {}\n"
        "execution          {}\n"
        "overhead           {}\n"
        "latency            {:.3f} ms\n"
        "tps                {:.3f} tx/s",
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
