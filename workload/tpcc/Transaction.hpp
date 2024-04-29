#pragma once

#include <cstdint>
#include <atomic>
#include <ctime>
#include <memory>
#include <unordered_set>
#include <unordered_map>
#include "Random.hpp"
#include "common.hpp"

#include "Tables.hpp"

class Transaction : public std::enable_shared_from_this<Transaction>
{
    public:
        typedef std::shared_ptr<Transaction> Ptr;

        Transaction() {
            for(auto& counter : order_counters) {
                counter.store(0);
            }
        }

        ~Transaction() = default;

        // 定义纯虚函数，生成事务
        virtual Transaction::Ptr makeTransaction() = 0; 

        // 原子地增加计数器，并返回增加后的值
        uint64_t increment_order(int idx = 0) {
            return order_counters[idx].fetch_add(1, std::memory_order_relaxed) + 1;   
        }

        // 获取当前计数器的值
        uint64_t get_order(int idx = 0) {
            return order_counters[idx].load(std::memory_order_relaxed);
        }

        // add child transaction
        void addChild(Transaction::Ptr child, minw::DependencyType dependency) {
            children.push_back({child, dependency});
        }

        // add readRows
        void addReadRow(const std::string &row) {
            readRows.insert(row);
        }

        // add updateRows
        void addUpdateRow(const std::string &row) {
            updateRows.insert(row);
        }

        // add sibling transaction
        void addSibling(Transaction::Ptr sibling) {
            siblings.push_back(sibling);
        }

        struct ChildTransaction {
            Transaction::Ptr transaction;
            minw::DependencyType dependency;
        };

    protected:
        Random random;                                          // random generator
        std::array<std::atomic<uint64_t>, 10> order_counters;   // order counter
        // 待测试...
        static const std::array<std::string, 3000> c_lasts;     // const last name
        static const std::unordered_map<std::string, std::vector<int32_t>> c_last_to_c_id; // last name to customer id
        // tx operations
        std::unordered_set<std::string> readRows;               // read rows
        std::unordered_set<std::string> updateRows;             // update rows
        // tx structure
        std::vector<ChildTransaction> children;                 // child transactions
        std::vector<Transaction::Ptr> siblings;                 // sibling transactions
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
            auto newOrderTx = makeNewOrder();
            /* 事务逻辑：
                1. warehouse表：读取w_tax字段
                2. district表：读取d_tax, d_next_o_id字段，更新d_next_o_id字段
                3. customer表：读取c_discount, c_last, c_credit字段
                4. newOrder表：插入一条新的记录 => 更新no_o_id, no_d_id, no_w_id字段
                5. order表：插入一条新的记录 => 更新o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt字段
                6. 对每个item进行如下操作：
                    6.1 item表：读取i_price, i_name字段
                    6.2 stock表：读取s_quantity字段，更新s_quantity, s_ytd, s_order_cnt字段
                    6.3 计算ol_amount = ol_quantity * i_price
                    6.4 orderLine表：插入一条新的记录 => 更新ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount字段
                7. 计算总金额：total_amount = sum(ol_amount)*（1-c_discount）*(1+w_tax+d_tax)
            */

            // 根事务
            Transaction::Ptr root = std::make_shared<Transaction>();
            
            // warehouse子事务
            Transaction::Ptr wAccess = std::make_shared<Transaction>();
            wAccess->addReadRow(std::to_string(newOrderTx->w_id));
            
            // district子事务
            Transaction::Ptr dAccess = std::make_shared<Transaction>();
            dAccess->addReadRow(std::to_string(newOrderTx->w_id) + std::to_string(newOrderTx->d_id));
            dAccess->addUpdateRow(std::to_string(newOrderTx->w_id) + std::to_string(newOrderTx->d_id));

// o_id这个冲突需要表达吗？
            uint64_t next_o_id = dAccess->increment_order();
            
            // newOrder子事务
            Transaction::Ptr noAccess = std::make_shared<Transaction>();
            noAccess->addUpdateRow(std::to_string(newOrderTx->w_id) + std::to_string(newOrderTx->d_id) + std::to_string(next_o_id));
            
            // order子事务
            Transaction::Ptr oAccess = std::make_shared<Transaction>();
            oAccess->addUpdateRow(std::to_string(newOrderTx->w_id) + std::to_string(newOrderTx->d_id) + std::to_string(next_o_id));
            
            // items子事务
            Transaction::Ptr itemsAccess = std::make_shared<Transaction>();
            for (auto i = 0; i < newOrderTx->o_ol_cnt; i++) {
                // ol_amount子事务
                Transaction::Ptr olAmountAccess = std::make_shared<Transaction>();
                
                // stock子事务
                Transaction::Ptr sAccess = std::make_shared<Transaction>();
                sAccess->addReadRow(std::to_string(newOrderTx->orderLines[i].ol_supply_w_id) + std::to_string(newOrderTx->orderLines[i].ol_i_id));
                sAccess->addUpdateRow(std::to_string(newOrderTx->orderLines[i].ol_supply_w_id) + std::to_string(newOrderTx->orderLines[i].ol_i_id));
                
                // orderLine子事务
                Transaction::Ptr olAccess = std::make_shared<Transaction>();
                olAccess->addUpdateRow(std::to_string(newOrderTx->w_id) + std::to_string(newOrderTx->d_id) + std::to_string(next_o_id) + std::to_string(i));
                
                // item子事务
                Transaction::Ptr iAccess = std::make_shared<Transaction>();
                iAccess->addReadRow(std::to_string(newOrderTx->orderLines[i].ol_i_id));
                iAccess->addSibling(olAccess);

                // ol_amount子事务添加依赖
                olAmountAccess->addChild(sAccess, minw::DependencyType::WEAK);
                olAmountAccess->addChild(iAccess, minw::DependencyType::STRONG);
                olAmountAccess->addChild(olAccess, minw::DependencyType::WEAK);

                // items子事务添加依赖
                itemsAccess->addChild(olAmountAccess, minw::DependencyType::STRONG);
            }
            
            // district子事务添加依赖
            dAccess->addChild(noAccess, minw::DependencyType::WEAK);
            dAccess->addChild(oAccess, minw::DependencyType::WEAK);
            dAccess->addChild(itemsAccess, minw::DependencyType::STRONG);

            // customer子事务
            Transaction::Ptr cAccess = std::make_shared<Transaction>();
            cAccess->addReadRow(std::to_string(newOrderTx->w_id) + std::to_string(newOrderTx->d_id) + std::to_string(newOrderTx->c_id));
            
            // 根节点添加依赖
            root->addChild(wAccess, minw::DependencyType::STRONG);
            root->addChild(dAccess, minw::DependencyType::STRONG);
            root->addChild(cAccess, minw::DependencyType::STRONG);
            return root;
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
                paymentTx->c_id = -1;
            } else {
                paymentTx->c_id = random.non_uniform_distribution(1023, 1, TPCC::n_customers);
            }
            // paymentTx->c_id = random.non_uniform_distribution(1023, 1, n_customers);
            paymentTx->h_amount = random.uniform_dist(1, 5000);
            return paymentTx;
        }

        // 生成嵌套事务
        Transaction::Ptr makeTransaction() override {
            auto paymentTx = makePayment();
            /* 事务逻辑：
                1. warehouse表: 读取并更新w_ytd字段
                2. district表: 读取并更新d_ytd字段
                3. customer表: 读取并更新c_balance, c_ytd_payment, c_payment_cnt字段
                    3.1 利用c_w_id, c_d_id, c_id精确查询
                    3.2 利用c_w_id, c_d_id, c_last范围查询 => 转化为c_ids => 取第n/2(向上取整)个id
                        paymentTx->c_ids = c_last_to_c_id.at(paymentTx->c_last);
                        (注意 size==0 的情况)
                4. history表: 插入一条新的记录 => 更新(h_c_id, h_c_d_id, h_c_w_id), h_d_id, h_w_id, h_amount字段
            */

            Transaction::Ptr root = std::make_shared<Transaction>();
            return root;
        }
    private:
        // Payment事务需要参数
        uint32_t w_id;
        uint32_t d_id;
        int32_t c_id;
        std::vector<int32_t> c_ids;
        std::string c_last;
        uint32_t h_amount;
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
            auto orderStatusTx = makeOrderStatus();
            /* 事务逻辑：
                1. customer表: 读取c_balance, c_first, c_middle, c_last字段
                    1.1 利用c_w_id, c_d_id, c_id精确查询
                    1.2 利用c_w_id, c_d_id, c_last范围查询 => 转化为c_ids => 取第n/2(向上取整)个id
                2. order表: 根据w_id, d_id, c_id查找最近的o_id, 读取o_id字段 => 随机生成一个
                3. orderLine表: 根据w_id, d_id, o_id查找, 读取ol_i_id, ol_supply_w_id, ol_quantity, ol_amount等字段
            */

            Transaction::Ptr root = std::make_shared<Transaction>();
            return root;
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
            auto deliveryTx = makeDelivery();
            /* 事务逻辑：对warehouse中每个district进行如下操作：(如果和前面的delivery事务有冲突，则需等待)
                1. newOrder表: 根据w_id, d_id查找, 读取no_o_id字段最小的一条记录, 删除该记录
                2. order表: 根据w_id, d_id, o_id查找, 读取o_ol_cnt, o_c_id字段, 更新o_carrier_id字段
                3. orderLine表: 根据w_id, d_id, o_id查找, 读取ol_amount字段, 更新ol_delivery_d字段 => 计算ol_amount总和
                4. customer表: 根据w_id, d_id, o_c_id查找, 读取c_balance字段, 更新c_balance字段
            */

            Transaction::Ptr root = std::make_shared<Transaction>();
            return root;
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
            auto stockLevelTx = makeStockLevel();
            /* 事务逻辑：(顺序执行可看成一笔事务)
                1. district表: 根据w_id, d_id查找, 读取d_next_o_id字段
                2. orderLine表: 根据w_id, d_id, d_next_o_id查找最近的20条orderLine记录, 读取ol_i_id字段
                3. stock表: 根据w_id, ol_i_id查找, 读取s_quantity字段并统计该字段小于threshold的记录数
                需要额外维护的数据：(d_id, [{o_id, ol_i_id}, ...])
            */

            Transaction::Ptr root = std::make_shared<Transaction>();
            return root;
        }

    private:
        // StockLevel事务需要参数
        uint64_t w_id;
        uint64_t d_id;
        uint64_t threshold;
};

