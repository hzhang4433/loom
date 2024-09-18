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
    // if (count_commit.load() == 0) {
    //     begin_time = std::chrono::steady_clock::now();
    // } else {
    //     end_time = std::chrono::steady_clock::now();
    // }
    end_time = std::chrono::steady_clock::now();
    count_commit.fetch_add(1, std::memory_order_relaxed);
    count_latency.fetch_add(latency, std::memory_order_relaxed);
    DLOG(INFO) << "latency: " << latency << "us";
}

void Statistics::JournalCommit(size_t latency, std::chrono::time_point<std::chrono::steady_clock> n_end_time) {
    end_time = std::max(end_time, n_end_time);
    count_commit.fetch_add(1, std::memory_order_relaxed);
    count_latency.fetch_add(latency, std::memory_order_relaxed);
    DLOG(INFO) << "latency: " << latency << "us";
}

void Statistics::JournalExecute() {
    if (count_execution.load() == 0) {
        begin_time = std::chrono::steady_clock::now();
    }
    count_execution.fetch_add(1, std::memory_order_relaxed);
}

void Statistics::JournalOverheads(size_t count) {
    count_overhead.fetch_add(count, std::memory_order_relaxed);
}

void Statistics::JournalRollback(size_t count) {
    count_rollback.fetch_add(count, std::memory_order_relaxed);
}

void Statistics::JournalBlock() {
    count_block.fetch_add(1, std::memory_order_relaxed);
}

void Statistics::JournalRollbackExecution(size_t latency) {
    count_latency_rollback.fetch_add(latency, std::memory_order_relaxed);
}

void Statistics::JournalReExecution(size_t latency) {
    count_latency_reExecution.fetch_add(latency, std::memory_order_relaxed);
}

std::string Statistics::Print() {
    // calculate the statistics duration
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - begin_time).count();
    LOG(INFO) << std::fixed << std::setprecision(3) << "duration: " << duration / (double)(1000) << "ms";

    #define TIME(X, Y) ((double)(X.load()) / (double)(Y.load()) / (double)(1000))
    #define BLOCKLATENCY(X) ((double)(duration) / (double)(X.load()) / (double)(1000))
    #define PRELATENCY(X, Y, Z) (BLOCKLATENCY(Z) - TIME(X, Z) - TIME(Y, Z))
    #define RATIO(X, Y) ((double)(X.load()) / (double)(Y.load()))
    #define TPS(X) ((double)(X.load()) / (double)(duration) * (double)(1000000))

    
    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    char time_buffer[100];
    std::strftime(time_buffer, sizeof(time_buffer), "%F %T", std::localtime(&now));
    
    return std::string(fmt::format(
        "{}\n"
        "commit             {}\n"
        "execution          {}\n"
        "overhead           {:.3f}\n"
        "rollback           {:.3f}\n"
        "rollback ratio     {:.3f}\n"
        "tx latency         {:.3f} ms\n"
        "tps                {:.3f} tx/s\n"
        "block latency      {:.3f} ms\n"
        "execute latency    {:.3f} ms\n"
        "rollback latency   {:.3f} ms\n"
        "re-execute latency {:.3f} ms\n"
        "concurrency ratio  {:.3f}",
        time_buffer,
        count_commit.load(),
        count_execution.load(),
        TIME(count_overhead, count_block),
        TIME(count_rollback, count_block),
        RATIO(count_rollback, count_overhead),
        TIME(count_latency, count_commit),
        TPS(count_commit),
        BLOCKLATENCY(count_block),
        PRELATENCY(count_latency_rollback, count_latency_reExecution, count_block),
        TIME(count_latency_rollback, count_block),
        TIME(count_latency_reExecution, count_block),
        RATIO(count_rollback, count_latency_reExecution)
    ));
    #undef TPS
    #undef LATENCY
}

} // namespace spectrum
