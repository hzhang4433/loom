#include "Transaction.hpp"

// // 定义并初始化c_lasts数组
// const std::array<std::string, 3000> Transaction::c_lasts = [] {
//     Random random;
//     std::array<std::string, 3000> temp{};
//     for (int i = 0; i < 3000; i++) {
//         if (i < 1000) {
//             temp[i] = random.rand_last_name(i);
//         } else {
//             temp[i] = random.rand_last_name(random.non_uniform_distribution(255, 0, 999));
//         }
//     }
//     return temp;
// }();

// // 定义并初始化c_last_to_c_id
// const std::unordered_map<std::string, std::vector<int32_t>> Transaction::c_last_to_c_id = [] {
//     std::unordered_map<std::string, std::vector<int32_t>> temp;
//     for (int32_t c_id = 0; c_id < 3000; c_id++) {
//         temp[c_lasts[c_id]].push_back(c_id + 1);
//     }
//     return temp;
// }();


// 定义并初始化c_lasts数组和c_last_to_c_id
static auto init_data = [] {
    Random random;
    std::array<std::string, 3000> c_lasts{};
    std::unordered_map<std::string, std::vector<int32_t>> c_last_to_c_id;
    for (int i = 0; i < 3000; i++) {
        if (i < 1000) {
            c_lasts[i] = random.rand_last_name(i);
        } else {
            c_lasts[i] = random.rand_last_name(random.non_uniform_distribution(255, 0, 999));
        }
        c_last_to_c_id[c_lasts[i]].push_back(i + 1);
    }
    return std::make_pair(c_lasts, c_last_to_c_id);
}();

const std::array<std::string, 3000> Transaction::c_lasts = init_data.first;
const std::unordered_map<std::string, std::vector<int32_t>> Transaction::c_last_to_c_id = init_data.second;


// 定义静态变量
std::unordered_map<std::string, Transaction::OrderInfo> Transaction::wdc_latestOrder;
std::unordered_map<std::string, std::queue<Transaction::OrderInfo>> Transaction::wd_oldestNewOrder;
std::unordered_map<std::string, std::vector<Transaction::OrderLineInfo>> Transaction::wd_latestOrderLines;

// 定义并初始化order_counters的每个元素
std::array<std::atomic<uint64_t>, 10> Transaction::order_counters;
static auto _ = [] {
    for(auto& counter : Transaction::order_counters) {
        counter.store(1);
    }
    return 0;
}();

uint64_t Transaction::wd_orderLineCounters[TPCC::n_warehouses][TPCC::n_districts];

std::map<size_t, int> Transaction::ol_i_id_num;