#include "Transaction.hpp"

// 定义并初始化c_lasts数组
const std::array<std::string, 3000> Transaction::c_lasts = [] {
    Random random;
    std::array<std::string, 3000> temp{};
    for (int i = 0; i < 3000; i++) {
        if (i < 1000) {
            temp[i + 1] = random.rand_last_name(i);
        } else {
            temp[i + 1] = random.rand_last_name(random.non_uniform_distribution(255, 0, 999));
        }
    }
    return temp;
}();

// 定义并初始化c_last_to_c_id
const std::unordered_map<std::string, std::vector<int32_t>> Transaction::c_last_to_c_id = [] {
    std::unordered_map<std::string, std::vector<int32_t>> temp;
    for (int32_t c_id = 1; c_id <= 3000; c_id++) {
        temp[c_lasts[c_id]].push_back(c_id);
    }
    return temp;
}();

// 定义静态变量
std::unordered_map<std::string, Transaction::OrderInfo> Transaction::latestOrder;
std::unordered_map<std::string, Transaction::OrderInfo> Transaction::oldestNewOrder;
std::unordered_map<std::string, std::vector<Transaction::OrderLineInfo>> Transaction::latestOrderLines;