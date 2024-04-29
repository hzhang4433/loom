#pragma once

#include "Transaction.hpp"

/* 负载类：生成已经转化为嵌套事务形式的五种TPC-C事务 */ 
class Workload {
    public:
        Workload() {
            for (int i = 0; i < 3000; i++) {
                if (i < 1000) {
                    c_lasts[i + 1] = random.rand_last_name(i);
                } else {
                    c_lasts[i + 1] = random.rand_last_name(random.non_uniform_distribution(255, 0, 999));
                }
            }
            txGenerator = std::make_shared<Transaction>();
        }
        
        ~Workload() = default;
        
        // 随机生成封装好的五类事务
        Transaction::Ptr NextTransaction() {
            uint64_t option = random.uniform_dist(1, 100);
            if (option <= 45) {         // 生成由newOrder构成的负载
                txGenerator = std::make_shared<NewOrderTransaction>();
            } else if (option <= 88) {  // 生成由payment构成的负载
                txGenerator = std::make_shared<PaymentTransaction>();
            } else if (option <= 92) {  // 生成由orderStatus构成的负载
                txGenerator = std::make_shared<OrderStatusTransaction>();
            } else if (option <= 96) {  // 生成由delivery构成的负载
                txGenerator = std::make_shared<DeliveryTransaction>();
            } else {                    // 生成由stockLevel构成的负载
                txGenerator = std::make_shared<StockLevelTransaction>();
            }
            return txGenerator->makeTransaction();
        }
        
    private:
        Random random;
        Transaction::Ptr txGenerator;
        std::string c_lasts[3001];
};