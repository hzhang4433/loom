#pragma once
#include <atomic>
#include <chrono>
#include <loom/utils/Ulock.h>

namespace loom {


class Statistics {

    private:
    static const int SAMPLE = 1000;
    std::atomic<size_t> count_commit{0};
    std::atomic<size_t> count_execution{0};
    std::atomic<size_t> count_overhead{0};
    std::atomic<size_t> count_latency{0};
    std::chrono::high_resolution_clock::time_point begin_time;
    std::chrono::high_resolution_clock::time_point end_time;

    public:
    Statistics() = default;
    Statistics(const Statistics& statistics) = delete;
    void JournalCommit(size_t latency);
    void JournalExecute();
    void JournalOverheads(size_t count);
    std::string Print();

};

} // namespace spectrum
