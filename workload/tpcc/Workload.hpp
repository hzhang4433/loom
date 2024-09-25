#pragma once

#include "Transaction.hpp"

/* 负载类：生成已经转化为嵌套事务形式的五种TPC-C事务 */ 
class Workload {
    public:
        Workload() : random(reinterpret_cast<uint64_t>(this)), tx_random(reinterpret_cast<uint64_t>(this)){
            // 重置静态变量的值
            txGenerator = std::make_shared<TPCCTransaction>(tx_random);
            txGenerator->resetStatic();
            init();
        }
        
        Workload(uint64_t seed) : random(seed), tx_random(seed) {
            // 重置静态变量的值
            txGenerator = std::make_shared<TPCCTransaction>(tx_random);
            txGenerator->resetStatic();
            txGenerator = std::make_shared<NewOrderTransaction>(tx_random);
            txGenerator->makeTransaction();
        };
        
        ~Workload() = default;
        
        // 随机生成封装好的五类事务
        TPCCTransaction::Ptr NextTransaction() {
            uint64_t option = random.uniform_dist(1, 100);
            if (option <= 45) {         // 生成由newOrder构成的负载
                txGenerator = std::make_shared<NewOrderTransaction>(tx_random);
            } else if (option <= 88) {  // 生成由payment构成的负载
                txGenerator = std::make_shared<PaymentTransaction>(tx_random);
            } else if (option <= 92) {  // 生成由orderStatus构成的负载
                txGenerator = std::make_shared<OrderStatusTransaction>(tx_random);
            } else if (option <= 96) {  // 生成由delivery构成的负载
                txGenerator = std::make_shared<DeliveryTransaction>(tx_random);
            } else {                    // 生成由stockLevel构成的负载
                txGenerator = std::make_shared<StockLevelTransaction>(tx_random);
            }            
            return txGenerator->makeTransaction();
        }
        
        uint64_t get_seed() {
            return random.get_seed();
        }

        loom::Random get_tx_random() {
            return tx_random;
        }

        loom::Random get_random() {
            return random;
        }

        void set_seed(uint64_t seed) {
            // 重置静态变量的值
            txGenerator->resetStatic();
            random.set_seed(seed);
            tx_random.set_seed(seed);
            txGenerator = std::make_shared<NewOrderTransaction>(tx_random);
            txGenerator->makeTransaction();
            // init();
        }

        void init() {
            txGenerator = std::make_shared<NewOrderTransaction>(tx_random);
            for (int i = 0; i < 50; i++)
                txGenerator->makeTransaction();
        }

    private:
        loom::Random random;
        loom::Random tx_random;
        TPCCTransaction::Ptr txGenerator;
};