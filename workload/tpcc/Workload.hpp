#pragma once

#include "Transaction.hpp"

/* 负载类：生成已经转化为嵌套事务形式的五种TPC-C事务 */ 
class Workload {
    public:
        Workload() : random(reinterpret_cast<uint64_t>(this)) {}
        
        Workload(uint64_t seed) : random(seed) {
            // 重置静态变量的值
            txGenerator->resetStatic();
            txGenerator = std::make_shared<NewOrderTransaction>(random);
            txGenerator->makeTransaction();
        };
        
        ~Workload() = default;
        
        // 随机生成封装好的五类事务
        Transaction::Ptr NextTransaction() {
            uint64_t option = random.uniform_dist(1, 100);
            if (option <= 45) {         // 生成由newOrder构成的负载
                txGenerator = std::make_shared<NewOrderTransaction>(random);
            } else if (option <= 88) {  // 生成由payment构成的负载
                txGenerator = std::make_shared<PaymentTransaction>(random);
            } else if (option <= 92) {  // 生成由orderStatus构成的负载
                txGenerator = std::make_shared<OrderStatusTransaction>(random);
            } else if (option <= 96) {  // 生成由delivery构成的负载
                txGenerator = std::make_shared<DeliveryTransaction>(random);
            } else {                    // 生成由stockLevel构成的负载
                txGenerator = std::make_shared<StockLevelTransaction>(random);
            }
            return txGenerator->makeTransaction();
        }
        
        uint64_t get_seed() {
            return random.get_seed();
        }

        void set_seed(uint64_t seed) {
            // 重置静态变量的值
            txGenerator->resetStatic();
            random.set_seed(seed);
            txGenerator = std::make_shared<NewOrderTransaction>(random);
            txGenerator->makeTransaction();
        }

    private:
        Random random;
        Transaction::Ptr txGenerator;
};