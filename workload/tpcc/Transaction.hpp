#pragma once

#include <cstdint>
#include <atomic>
#include <ctime>
#include <memory>
#include "Random.hpp"
#include "common.hpp"

#include "Tables.hpp"

class Transaction : public std::enable_shared_from_this<Transaction>
{
    public:
        typedef std::shared_ptr<Transaction> Ptr;

        Transaction() : order_counter(0) {
            
        }

        ~Transaction() = default;

        // 原子地增加计数器，并返回增加后的值
        uint64_t increment_order() {
            return order_counter.fetch_add(1, std::memory_order_relaxed) + 1;   
        }

        // 定义纯虚函数，生成事务
        virtual Transaction::Ptr makeTransaction() = 0; 

    protected:
        Random random;                          // random generator
        std::atomic<uint64_t> order_counter;    // order counter
};

class NewOrderTransaction : public Transaction
{
    public:
        typedef std::shared_ptr<NewOrderTransaction> Ptr;
        NewOrderTransaction() = default;
        ~NewOrderTransaction() = default;

        // 构造NewOrder事务
        NewOrderTransaction::Ptr makeNewOrder() {
            NewOrderTransaction::Ptr newOrderTx = std::make_shared<NewOrderTransaction>();
            // NewOrder主键ID
            newOrderTx->w_id = TPCC::n_warehouses;
            newOrderTx->d_id = random.uniform_dist(1, TPCC::n_districts);
            newOrderTx->c_id = random.non_uniform_distribution(1023, 1, TPCC::n_customers);
            newOrderTx->o_ol_cnt = random.uniform_dist(5, 15);
            
            // NewOrder订单行参数
            for (auto i = 0; i < newOrderTx->o_ol_cnt; i++) {
                bool retry;

                do {
                    retry = false;
                    newOrderTx->orderLines[i].ol_i_id = random.non_uniform_distribution(8191, 1, 100000);
                    for (int k = 0; k < i; k++) {
                        if (newOrderTx->orderLines[i].ol_i_id == newOrderTx->orderLines[k].ol_i_id) {
                            retry = true;
                            break;
                        }
                    }
                } while (retry);

                newOrderTx->orderLines[i].ol_supply_w_id = newOrderTx->w_id;
                newOrderTx->orderLines[i].ol_quantity = random.uniform_dist(1, 10);
            }

            return newOrderTx;
        }

        // 生成嵌套事务
        Transaction::Ptr makeTransaction() override {
            // return makeTransaction();
        }

    private:
        // NewOrder事务需要参数
        uint64_t w_id;
        uint64_t d_id;
        uint64_t c_id;
        uint32_t o_ol_cnt;
        struct OrderLineInfo {
            uint64_t ol_i_id;
            uint64_t ol_supply_w_id;
            uint8_t ol_quantity;
        };
        OrderLineInfo orderLines[15];
};

class PaymentTransaction : public Transaction
{
    public:
        typedef std::shared_ptr<PaymentTransaction> Ptr;
        PaymentTransaction() = default;
        ~PaymentTransaction() = default;
        
        // 构造Payment事务
        PaymentTransaction::Ptr makePayment() {
            PaymentTransaction::Ptr paymentTx = std::make_shared<PaymentTransaction>();
            paymentTx->w_id = TPCC::n_warehouses;
            paymentTx->d_id = random.uniform_dist(1, TPCC::n_districts);
            
            // 随机选择c_last或c_id
            int y = random.uniform_dist(1, 100);
            if (y <= 60) {
                paymentTx->c_last = random.rand_last_name(random.non_uniform_distribution(255, 0, 999));
            } else {
                paymentTx->c_id = random.non_uniform_distribution(1023, 1, TPCC::n_customers);
            }
            // paymentTx->c_id = random.non_uniform_distribution(1023, 1, n_customers);
            paymentTx->h_amount = random.uniform_dist(1, 5000);
            return paymentTx;
        }

        // 生成嵌套事务
        Transaction::Ptr makeTransaction() override {
            // return makeTransaction();
        }
    private:
        // Payment事务需要参数
        uint64_t w_id;
        uint64_t d_id;
        uint64_t c_id;
        std::string c_last;
        uint64_t h_amount;
};

class OrderStatusTransaction : public Transaction
{
    public:
        typedef std::shared_ptr<OrderStatusTransaction> Ptr;
        OrderStatusTransaction() = default;
        ~OrderStatusTransaction() = default;
        
        // 构造OrderStatus事务
        OrderStatusTransaction::Ptr makeOrderStatus() {
            OrderStatusTransaction::Ptr orderStatusTx = std::make_shared<OrderStatusTransaction>();
            orderStatusTx->w_id = TPCC::n_warehouses;
            orderStatusTx->d_id = random.uniform_dist(1, TPCC::n_districts);
            
            int y = random.uniform_dist(1, 100);
            if (y <= 60) {
                orderStatusTx->c_last = random.rand_last_name(random.non_uniform_distribution(255, 0, 999));
            } else {
                orderStatusTx->c_id = random.non_uniform_distribution(1023, 1, TPCC::n_customers);
            }
            // orderStatusTx->c_id = random.non_uniform_distribution(1023, 1, n_customers);

            return orderStatusTx;
        }

        // 生成嵌套事务
        Transaction::Ptr makeTransaction() override {
            // return makeTransaction();
        }

    private:
        // OrderStatus事务需要参数
        uint64_t w_id;
        uint64_t d_id;
        uint64_t c_id;
        std::string c_last;
};

class DeliveryTransaction : public Transaction
{
    public:
        typedef std::shared_ptr<DeliveryTransaction> Ptr;
        DeliveryTransaction() = default;
        ~DeliveryTransaction() = default;
        
        // 构造Delivery事务
        DeliveryTransaction::Ptr makeDelivery() {
            DeliveryTransaction::Ptr deliveryTx = std::make_shared<DeliveryTransaction>();
            deliveryTx->w_id = TPCC::n_warehouses;
            deliveryTx->o_carrier_id = random.uniform_dist(1, TPCC::n_carriers);
            deliveryTx->ol_delivery_d = std::time(nullptr);
            return deliveryTx;
        }

        // 生成嵌套事务
        Transaction::Ptr makeTransaction() override {
            // return makeTransaction();
        }

    private:
        // Delivery事务需要参数
        uint64_t w_id;
        uint64_t o_carrier_id;
        uint64_t ol_delivery_d;
};

class StockLevelTransaction : public Transaction
{
    public:
        typedef std::shared_ptr<StockLevelTransaction> Ptr;
        StockLevelTransaction() = default;
        ~StockLevelTransaction() = default;
        
        // 构造StockLevel事务
        StockLevelTransaction::Ptr makeStockLevel() {
            StockLevelTransaction::Ptr stockLevelTx = std::make_shared<StockLevelTransaction>();
            stockLevelTx->w_id = TPCC::n_warehouses;
            stockLevelTx->d_id = random.uniform_dist(1, TPCC::n_districts);
            stockLevelTx->threshold = random.uniform_dist(10, 20);
            return stockLevelTx;
        }

        // 生成嵌套事务
        Transaction::Ptr makeTransaction() override {
            // return makeTransaction();
        }

    private:
        // StockLevel事务需要参数
        uint64_t w_id;
        uint64_t d_id;
        uint64_t threshold;
};

