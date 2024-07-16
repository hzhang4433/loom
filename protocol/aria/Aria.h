#pragma once

#include "protocol/common.h"
#include "common/Transaction.h"

using namespace std;

namespace loom {

struct AriaTransaction: public Transaction {

};

/// @brief aria protocol master class
class Aria: public Protocol {

public:
    Aria(Workload& workload, size_t num_threads, size_t table_partitions, size_t repeat, bool enable_reordering);
    void Start() override;
    void Stop() override;
    ~Aria() override = default;
    void Run();
    void Execute(T* tx);
    void Reserve(T* tx);
    void Verify(T* tx);
    void Commit(T* tx);
    void PrepareLockTable(T* tx);
    void Fallback(T* tx);
    void CleanLockTable(T* tx);

private:
    Workload&                               workload;
    AriaTable&                              table;
    AriaLockTable&                          lock_table;
    bool                                    enable_reordering;
    size_t                                  num_threads;
    std::atomic<size_t>&                    confirm_exit;
    std::atomic<bool>&                      stop_flag;
    std::barrier<std::function<void()>>&    barrier;
    std::atomic<size_t>&                    counter;
    std::atomic<bool>&                      has_conflict;
    size_t                                  repeat;
    size_t                                  worker_id;
};

}
