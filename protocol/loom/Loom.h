#pragma once

#include <chrono>
#include <loom/common/Transaction.h>
#include <loom/protocol/loom/common.h>

namespace loom {

#define T LoomTransaction

/// @brief loom tranaction.
struct LoomTransaction: public Transaction {
    size_t      id;
    size_t      block_id;
    std::atomic<bool>   committed{false};
    std::chrono::time_point<std::chrono::steady_clock> start_time;
    Vertex::Ptr tx;
    int scheduledTime = 0;
    LoomTransaction* should_wait = nullptr;
    LoomTransaction(Transaction&& inner, size_t id, size_t block_id);
    LoomTransaction(LoomTransaction&& tx) noexcept; // move constructor
    LoomTransaction(const LoomTransaction& other); // copy constructor
};

/// @brief loom protocol master class
class Loom {

};

#undef T

}
