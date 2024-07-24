#pragma once

#include <cstdint>
#include <atomic>
#include <ctime>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <map>
#include "Random.hpp"
#include "Define.h"
#include "common/common.h"
#include "utils/UConditionalOutputStream.h"


#include <iostream>
using namespace std;
using namespace loom;

class TPCCTransaction : public std::enable_shared_from_this<TPCCTransaction>
{
    public:
        typedef std::shared_ptr<TPCCTransaction> Ptr;
        
        // 使用自定义输出流代替std::cout
        Util::ConditionalOutputStream coutConditional;

        struct ChildTransaction {
            TPCCTransaction::Ptr transaction;
            loom::DependencyType dependency;
        };

        struct OrderInfo {
            uint64_t o_id;
            uint64_t o_ol_cnt;
            int64_t o_c_id;
        };

        struct OrderLineInfo {
            uint64_t o_id;
            uint64_t ol_i_id;
            int ol_number;
        };

        // 随机
        // TPCCTransaction(): random(reinterpret_cast<uint64_t>(this)) {}
        TPCCTransaction(Random& random): random(random) {}

        ~TPCCTransaction() = default;

        // 定义虚函数，生成事务
        virtual TPCCTransaction::Ptr makeTransaction() {}

        // 原子地增加计数器，并返回增加后的值 
        // 带测试？ 这个值不会每次初始化都是1吧？
        uint64_t increment_order(int idx = 0) {return order_counters[idx].fetch_add(1, std::memory_order_relaxed);}

        // 获取当前计数器的值
        const uint64_t get_order(int idx = 0) const {return order_counters[idx].load(std::memory_order_relaxed);}

        // 获取交易执行时间
        const int getExecutionTime() const {return executionTime;}

        // 设置交易执行时间
        void setExecutionTime(int time) {executionTime = time;}

        // add child transaction
        void addChild(TPCCTransaction::Ptr child, loom::DependencyType dependency) {children.push_back({child, dependency});}

        // get children
        const std::vector<ChildTransaction>& getChildren() const {return children;}

        // add readRows
        void addReadRow(const std::string &row) {readRows.insert(row);}

        // get readRows
        const unordered_set<std::string>& getReadRows() const {return readRows;}

        // add updateRows
        void addUpdateRow(const std::string &row) {updateRows.insert(row);}

        // get updateRows
        const unordered_set<std::string>& getUpdateRows() const {return updateRows;}

        // add sibling transaction
        void addSibling(TPCCTransaction::Ptr sibling) {siblings.push_back(sibling);}

        // get siblings
        const std::vector<TPCCTransaction::Ptr>& getSiblings() const {return siblings;}

        // set transaction type
        void setType(TPCC::TransactionType type) {m_type = type;}

        // get transaction type
        const TPCC::TransactionType getType() const {return m_type;}

        static std::array<std::atomic<uint64_t>, 10> order_counters;   // order counter

        void resetStatic() {
            // wdc_latestOrder
            wdc_latestOrder.clear();
            wd_oldestNewOrder.clear();
            wd_latestOrderLines.clear();
            for(auto& counter : TPCCTransaction::order_counters) {
                counter.store(1);
            }
            // wd_orderLineCounters
            for (int i = 0; i < TPCC::N_WAREHOUSES; i++) {
                for (int j = 0; j < TPCC::N_DISTRICTS; j++) {
                    wd_orderLineCounters[i][j] = 0;
                }
            }
        }

        void printCustomerInfo() {
            coutConditional << "c_lasts: ";
            for (int i = 0; i < 3000; i++) {
                coutConditional << TPCCTransaction::c_lasts[i] << " ";
            }
            coutConditional << endl;
        }

        // 打印订单行频率
        void printOrderLineInfo() {
            coutConditional << "ol_i_id_num: " << endl;
            for (auto& it : ol_i_id_num) {
                coutConditional << it.first << ":" << it.second << "\t";
            }
            coutConditional << endl;
        }

        Random& random;                                          // random generator
    protected:
        
// 静态变量 待测试...
        static const std::array<std::string, 3000> c_lasts;     // const last name
        static const std::unordered_map<std::string, std::vector<int32_t>> c_last_to_c_id;      // last name to customer id
        static std::unordered_map<std::string, OrderInfo> wdc_latestOrder;                      // format: (w_id-d_id-c_id, {o_id, o_ol_cnt})
        static std::unordered_map<std::string, std::queue<OrderInfo>> wd_oldestNewOrder;        // format: (w_id-d_id, {o_id, o_c_id, o_ol_cnt})
        static std::unordered_map<std::string, std::vector<OrderLineInfo>> wd_latestOrderLines; // format: (d_id, [{o_id, ol_i_id}, ...])
        static uint64_t wd_orderLineCounters[TPCC::N_WAREHOUSES][TPCC::N_DISTRICTS];            // warehouse-distirct orderLine counter
        
        static std::map<size_t, int> ol_i_id_num; // 测试订单行出现频率

        // tx operations
        unordered_set<std::string> readRows;                    // read rows
        unordered_set<std::string> updateRows;                  // update rows
        // tx structure
        std::vector<ChildTransaction> children;                 // child transactions
        std::vector<TPCCTransaction::Ptr> siblings;                 // sibling transactions
        int executionTime = TPCC::ConsumptionType::MEDIUM;      // execution time
        TPCC::TransactionType m_type;                             // transaction type
};

class NewOrderTransaction : public TPCCTransaction
{
    public:
        typedef std::shared_ptr<NewOrderTransaction> Ptr;
        // NewOrderTransaction() = default;
        NewOrderTransaction(Random& random) : TPCCTransaction(random) {};
        ~NewOrderTransaction() = default;

        // 构造NewOrder事务生成所需参数
        NewOrderTransaction::Ptr makeNewOrder() {
            NewOrderTransaction::Ptr newOrderTx = std::make_shared<NewOrderTransaction>(random);
            // NewOrder主键ID
            newOrderTx->w_id = random.uniform_dist(1, TPCC::N_WAREHOUSES);
            newOrderTx->d_id = random.uniform_dist(1, TPCC::N_DISTRICTS);
            newOrderTx->c_id = random.non_uniform_distribution(1023, 1, TPCC::N_CUSTOMERS);
            newOrderTx->o_ol_cnt = random.uniform_dist(5, 15);
            
            // NewOrder订单行参数
            for (auto i = 0; i < newOrderTx->o_ol_cnt; i++) {
                bool retry;
                do {
                    retry = false;
                    uint32_t random_id = random.non_uniform_distribution(8191, 1, 100000);
                    for (int k = 0; k < i; k++) {
                        if (random_id == newOrderTx->orderLines[k].ol_i_id) {
                            retry = true;
                            break;
                        }
                    }
                    newOrderTx->orderLines[i].ol_i_id = random_id;
                } while (retry);

                // newOrderTx->orderLines[i].ol_i_id = random.non_uniform_distribution(8191, 1, 100000);
                newOrderTx->orderLines[i].ol_supply_w_id = newOrderTx->w_id;
                newOrderTx->orderLines[i].ol_quantity = random.uniform_dist(1, 10);
            }

            return newOrderTx;
        }

        // 生成嵌套事务
        TPCCTransaction::Ptr makeTransaction() override {
            // coutConditional << "======= making new order transaction =======" << endl;

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
            TPCCTransaction::Ptr root = std::make_shared<TPCCTransaction>(random);
            root->setType(TPCC::TransactionType::NEW_ORDER);
            root->setExecutionTime(TPCC::ConsumptionType::LOW);
            
            // warehouse子事务
            TPCCTransaction::Ptr wAccess = std::make_shared<TPCCTransaction>(random);
            wAccess->addReadRow("Wtax-" + std::to_string(newOrderTx->w_id));
            
            // district子事务
            TPCCTransaction::Ptr dAccess = std::make_shared<TPCCTransaction>(random);
            dAccess->addReadRow("Dtax-" + std::to_string(newOrderTx->w_id) + "-" + std::to_string(newOrderTx->d_id));
            dAccess->addUpdateRow("DnextOId-" + std::to_string(newOrderTx->w_id) + "-" + std::to_string(newOrderTx->d_id));
            

            // 获取下一个订单号
            uint64_t next_o_id = increment_order(newOrderTx->d_id - 1);
            // coutConditional << "In NewOrderTransaction, w_id = " << newOrderTx->w_id
            //      << " d_id = " << newOrderTx->d_id 
            //      << " next_o_id = " << next_o_id 
            //      << " ol_cnt = " << newOrderTx->o_ol_cnt 
            //      << " c_id = " << newOrderTx->c_id << endl;
            // coutConditional << "item ids: ";
            // for (auto i = 0; i < newOrderTx->o_ol_cnt; i++) {
            //     coutConditional << newOrderTx->orderLines[i].ol_i_id << " ";
            // }
            // coutConditional << endl;
            
            // newOrder子事务
            TPCCTransaction::Ptr noAccess = std::make_shared<TPCCTransaction>(random);
            noAccess->addReadRow("NO-" + std::to_string(newOrderTx->w_id) + "-" + std::to_string(newOrderTx->d_id) + "-" + std::to_string(next_o_id));
            // noAccess->addUpdateRow("NO-" + std::to_string(newOrderTx->w_id) + "-" + std::to_string(newOrderTx->d_id) + "-" + std::to_string(next_o_id));
            noAccess->addUpdateRow("NO-" + std::to_string(newOrderTx->w_id) + "-" + std::to_string(newOrderTx->d_id));
            
            // order子事务
            TPCCTransaction::Ptr oAccess = std::make_shared<TPCCTransaction>(random);
            oAccess->addUpdateRow("O-" + std::to_string(newOrderTx->w_id) + "-" + std::to_string(newOrderTx->d_id) + "-" + std::to_string(next_o_id) + "-" + std::to_string(newOrderTx->c_id));
            // oAccess->addReadRow("O-" + std::to_string(newOrderTx->w_id) + "-" + std::to_string(newOrderTx->d_id) + "-" + std::to_string(next_o_id) + "-" + std::to_string(newOrderTx->c_id));
            
            // items子事务
            TPCCTransaction::Ptr itemsAccess = std::make_shared<TPCCTransaction>(random);
            itemsAccess->setExecutionTime(TPCC::ConsumptionType::HIGH);
            
            // /*
            for (auto i = 0; i < newOrderTx->o_ol_cnt; i++) {

                ol_i_id_num[newOrderTx->orderLines[i].ol_i_id]++;

                // item子事务
                TPCCTransaction::Ptr iAccess = std::make_shared<TPCCTransaction>(random);
                iAccess->addReadRow("I-" + std::to_string(newOrderTx->orderLines[i].ol_i_id));
                
                // orderLine子事务
                TPCCTransaction::Ptr olAccess = std::make_shared<TPCCTransaction>(random);
                olAccess->addReadRow("OL-" + std::to_string(newOrderTx->w_id) + "-" + std::to_string(newOrderTx->d_id) + "-" + std::to_string(next_o_id) + "-" + std::to_string(i));
                olAccess->addUpdateRow("OL-" + std::to_string(newOrderTx->w_id) + "-" + std::to_string(newOrderTx->d_id) + "-" + std::to_string(next_o_id) + "-" + std::to_string(i));
                
                // stock子事务
                TPCCTransaction::Ptr sAccess = std::make_shared<TPCCTransaction>(random);
                sAccess->addReadRow("S-" + std::to_string(newOrderTx->orderLines[i].ol_supply_w_id) + "-" + std::to_string(newOrderTx->orderLines[i].ol_i_id));
                sAccess->addUpdateRow("S-" + std::to_string(newOrderTx->orderLines[i].ol_supply_w_id) + "-" + std::to_string(newOrderTx->orderLines[i].ol_i_id));
        
                // item子事务添加依赖
                iAccess->addChild(sAccess, loom::DependencyType::WEAK);
                iAccess->addChild(olAccess, loom::DependencyType::WEAK);
                
                //  更新d_latestOrderLines，d_latestOrderLines中只存储最近的20条orderLine记录
                //    判断是否超过20条：
                //     若没超过20条，则直接添加
                //     若超过20条，则删除最旧的一条 => 覆盖最旧的一条 
                auto wd_key = std::to_string(newOrderTx->w_id) + "-" + std::to_string(newOrderTx->d_id);
                if (wd_orderLineCounters[newOrderTx->w_id - 1][newOrderTx->d_id - 1] < 20) {
                    wd_latestOrderLines[wd_key].push_back({next_o_id, newOrderTx->orderLines[i].ol_i_id, i});
                } else {
                    wd_latestOrderLines[wd_key][wd_orderLineCounters[newOrderTx->w_id - 1][newOrderTx->d_id - 1] % 20] = {next_o_id, newOrderTx->orderLines[i].ol_i_id, i};
                }
                // coutConditional << "In neworderTx, wd_orderLineCounters[" << newOrderTx->d_id << "] = " << wd_orderLineCounters[newOrderTx->w_id - 1][newOrderTx->d_id - 1] << endl;

                wd_orderLineCounters[newOrderTx->w_id - 1][newOrderTx->d_id - 1]++;

                // items子事务添加依赖
                itemsAccess->addChild(iAccess, loom::DependencyType::STRONG);
            }
            // */

/*
            // orderLine子事务
            TPCCTransaction::Ptr olAccess = std::make_shared<TPCCTransaction>(random);
            olAccess->setExecutionTime(TPCC::ConsumptionType::HIGH);
            // stock子事务
            TPCCTransaction::Ptr sAccess = std::make_shared<TPCCTransaction>(random);
            sAccess->setExecutionTime(TPCC::ConsumptionType::HIGH);

            for (auto i = 0; i < newOrderTx->o_ol_cnt; i++) {

                // ol_i_id_num[newOrderTx->orderLines[i].ol_i_id]++;

                // item子事务读写集
                itemsAccess->addReadRow("I-" + std::to_string(newOrderTx->orderLines[i].ol_i_id));
                
                olAccess->addReadRow("OL-" + std::to_string(newOrderTx->w_id) + "-" + std::to_string(newOrderTx->d_id) + "-" + std::to_string(next_o_id) + "-" + std::to_string(i));
                olAccess->addUpdateRow("OL-" + std::to_string(newOrderTx->w_id) + "-" + std::to_string(newOrderTx->d_id) + "-" + std::to_string(next_o_id) + "-" + std::to_string(i));

                sAccess->addReadRow("S-" + std::to_string(newOrderTx->orderLines[i].ol_supply_w_id) + "-" + std::to_string(newOrderTx->orderLines[i].ol_i_id));
                sAccess->addUpdateRow("S-" + std::to_string(newOrderTx->orderLines[i].ol_supply_w_id) + "-" + std::to_string(newOrderTx->orderLines[i].ol_i_id));
                
                auto wd_key = std::to_string(newOrderTx->w_id) + "-" + std::to_string(newOrderTx->d_id);
                if (wd_orderLineCounters[newOrderTx->w_id - 1][newOrderTx->d_id - 1] < 20) {
                    wd_latestOrderLines[wd_key].push_back({next_o_id, newOrderTx->orderLines[i].ol_i_id, i});
                } else {
                    wd_latestOrderLines[wd_key][wd_orderLineCounters[newOrderTx->w_id - 1][newOrderTx->d_id - 1] % 20] = {next_o_id, newOrderTx->orderLines[i].ol_i_id, i};
                }
                // coutConditional << "In neworderTx, wd_orderLineCounters[" << newOrderTx->d_id << "] = " << wd_orderLineCounters[newOrderTx->w_id - 1][newOrderTx->d_id - 1] << endl;

                wd_orderLineCounters[newOrderTx->w_id - 1][newOrderTx->d_id - 1]++;
            }
            // item事务添加依赖
            itemsAccess->addChild(sAccess, loom::DependencyType::WEAK);
            itemsAccess->addChild(olAccess, loom::DependencyType::WEAK);
*/
            
            // district子事务添加依赖
            dAccess->addChild(noAccess, loom::DependencyType::WEAK);
            dAccess->addChild(oAccess, loom::DependencyType::WEAK);
            dAccess->addChild(itemsAccess, loom::DependencyType::STRONG);

            // customer子事务
            TPCCTransaction::Ptr cAccess = std::make_shared<TPCCTransaction>(random);
            cAccess->addReadRow("Cdiscount-" + std::to_string(newOrderTx->w_id) + "-" + std::to_string(newOrderTx->d_id) + "-" + std::to_string(newOrderTx->c_id));
            // cAccess->addReadRow("C-" + std::to_string(newOrderTx->w_id) + "-" + std::to_string(newOrderTx->d_id) + "-" + std::to_string(newOrderTx->c_id));
            
            // 根节点添加依赖
            root->addChild(wAccess, loom::DependencyType::STRONG);
            root->addChild(dAccess, loom::DependencyType::STRONG);
            root->addChild(cAccess, loom::DependencyType::STRONG);

            // 更新wdc_latestOrder
            string wdc_key = std::to_string(newOrderTx->w_id) + "-" + std::to_string(newOrderTx->d_id) + "-" + std::to_string(newOrderTx->c_id);
            // coutConditional << "In NewOrderTransaction, before wdc_latestOrder[" << wdc_key << "] = {" 
            //      << "o_id: " << wdc_latestOrder[wdc_key].o_id << ", " 
            //      << "o_ol_cnt: " << wdc_latestOrder[wdc_key].o_ol_cnt << ", "
            //      << "o_c_id: " << wdc_latestOrder[wdc_key].o_c_id << "}" << endl;
            wdc_latestOrder[wdc_key] = {next_o_id, newOrderTx->o_ol_cnt, newOrderTx->c_id};
            // coutConditional << "In NewOrderTransaction, now wdc_latestOrder[" << wdc_key << "] = {" 
            //      << "o_id: " << wdc_latestOrder[wdc_key].o_id << ", " 
            //      << "o_ol_cnt: " << wdc_latestOrder[wdc_key].o_ol_cnt << ", "
            //      << "o_c_id: " << wdc_latestOrder[wdc_key].o_c_id << "}" << endl;
            
            // 更新wd_oldestNewOrder
            string wd_key = std::to_string(newOrderTx->w_id) + "-" + std::to_string(newOrderTx->d_id);
            wd_oldestNewOrder[wd_key].push({next_o_id, newOrderTx->o_ol_cnt, newOrderTx->c_id});

            return root;
        }

    private:
        // NewOrder事务需要参数
        uint64_t w_id;
        uint64_t d_id;
        int64_t c_id;
        uint64_t o_ol_cnt;
        struct OrderLineInfo {
            uint64_t ol_i_id;
            uint64_t ol_supply_w_id;
            uint64_t ol_quantity;
        };
        OrderLineInfo orderLines[15];
};

class PaymentTransaction : public TPCCTransaction
{
    public:
        typedef std::shared_ptr<PaymentTransaction> Ptr;
        // PaymentTransaction() = default;
        PaymentTransaction(Random& random) : TPCCTransaction(random) {};
        ~PaymentTransaction() = default;
        
        // 构造Payment事务
        PaymentTransaction::Ptr makePayment() {
            PaymentTransaction::Ptr paymentTx = std::make_shared<PaymentTransaction>(random);
            paymentTx->w_id = random.uniform_dist(1, TPCC::N_WAREHOUSES);
            paymentTx->d_id = random.uniform_dist(1, TPCC::N_DISTRICTS);
            
            // 随机选择c_last或c_id
            int y = random.uniform_dist(1, 100);
            if (y <= 60) {
                // 保证c_last对应的c_id不为空
                do { 
                    paymentTx->c_last = random.rand_last_name(random.non_uniform_distribution(255, 0, 999));
                } while (c_last_to_c_id.at(paymentTx->c_last).size() == 0);
                
                paymentTx->c_id = -1;
            } else {
                paymentTx->c_id = random.non_uniform_distribution(1023, 1, TPCC::N_CUSTOMERS);
            }
            // paymentTx->c_id = random.non_uniform_distribution(1023, 1, N_CUSTOMERS);
            paymentTx->h_amount = random.uniform_dist(1, 5000);
            return paymentTx;
        }

        // 生成嵌套事务
        TPCCTransaction::Ptr makeTransaction() override {
            // coutConditional << "======= making payment transaction =======" << endl;

            auto paymentTx = makePayment();
            // coutConditional << "paymentTx: w_id = " << paymentTx->w_id << 
            //                     ", d_id = " << paymentTx->d_id <<
            //                     ", c_id = " << paymentTx->c_id << 
            //                     ", c_last = " << paymentTx->c_last << endl;
            
            /* 事务逻辑：
                1. warehouse表: 读取并更新w_ytd字段
                2. district表: 读取并更新d_ytd字段
                3. customer表: 读取并更新c_balance, c_ytd_payment, c_payment_cnt字段
                    3.1 利用c_w_id, c_d_id, c_id精确查询
                    3.2 利用c_w_id, c_d_id, c_last范围查询 => 转化为c_ids => 取第n/2(向上取整)个id
                        paymentTx->c_ids = c_last_to_c_id.at(paymentTx->c_last);
                        (测试是否存在 size==0 的情况)
                4. history表: 插入一条新的记录 => 更新(h_c_id, h_c_d_id, h_c_w_id), h_d_id, h_w_id, h_amount字段
            */

            // 根事务
            TPCCTransaction::Ptr root = std::make_shared<TPCCTransaction>(random);
            root->setType(TPCC::TransactionType::PAYMENT);
            root->setExecutionTime(TPCC::ConsumptionType::LOW);
            
            // warehouse子事务
            TPCCTransaction::Ptr wAccess = std::make_shared<TPCCTransaction>(random);
            // wAccess->addReadRow("Wytd-" + std::to_string(paymentTx->w_id));
            // wAccess->addUpdateRow("Wytd-" + std::to_string(paymentTx->w_id));

            // district子事务
            TPCCTransaction::Ptr dAccess = std::make_shared<TPCCTransaction>(random);
            dAccess->addReadRow("Dytd-" + std::to_string(paymentTx->w_id) + "-" + std::to_string(paymentTx->d_id));
            dAccess->addUpdateRow("Dytd-" + std::to_string(paymentTx->w_id) + "-" + std::to_string(paymentTx->d_id));

            // history子事务
            TPCCTransaction::Ptr hAccess = std::make_shared<TPCCTransaction>(random);

            // customer子事务
            TPCCTransaction::Ptr cAccess = std::make_shared<TPCCTransaction>(random);
            if (paymentTx->c_id == -1) {
                auto& c_ids = c_last_to_c_id.at(paymentTx->c_last);
                
                for (auto& c_id : c_ids) {
                    cAccess->addReadRow("Cbalance-" + std::to_string(paymentTx->w_id) + "-" + std::to_string(paymentTx->d_id) + "-" + std::to_string(c_id));
                }
                
                paymentTx->c_id = c_ids[(c_ids.size() - 1) / 2];
                // coutConditional << "now have paymentTx->c_id: " << paymentTx->c_id << endl;

                cAccess->setExecutionTime(TPCC::ConsumptionType::HIGH);
                // 该情况下history子事务依赖customer子事务
                cAccess->addChild(hAccess, loom::DependencyType::WEAK);
            } else {
                // coutConditional << "already have paymentTx->c_id: " << paymentTx->c_id << endl;
                cAccess->addReadRow("Cbalance-" + std::to_string(paymentTx->w_id) + "-" + std::to_string(paymentTx->d_id) + "-" + std::to_string(paymentTx->c_id));
                // 否则history子事务独立
                root->addChild(hAccess, loom::DependencyType::WEAK);
            }
            // coutConditional << endl;

            // cAccess->addUpdateRow("Cbalance-" + std::to_string(paymentTx->w_id) + "-" + std::to_string(paymentTx->d_id) + "-" + std::to_string(paymentTx->c_id));
            cAccess->addUpdateRow("C-" + std::to_string(paymentTx->w_id) + "-" + std::to_string(paymentTx->d_id) + "-" + std::to_string(paymentTx->c_id));
            
            hAccess->addUpdateRow("H-" + std::to_string(paymentTx->w_id) + "-" + std::to_string(paymentTx->d_id) + "-" + std::to_string(paymentTx->c_id));

            // 根节点添加依赖
            root->addChild(wAccess, loom::DependencyType::WEAK);
            root->addChild(dAccess, loom::DependencyType::WEAK);
            root->addChild(cAccess, loom::DependencyType::WEAK);

            return root;
        }
    private:
        // Payment事务需要参数
        uint64_t w_id;
        uint64_t d_id;
        int64_t c_id;
        std::string c_last;
        uint64_t h_amount;
};

class OrderStatusTransaction : public TPCCTransaction
{
    public:
        typedef std::shared_ptr<OrderStatusTransaction> Ptr;
        // OrderStatusTransaction() = default;
        OrderStatusTransaction(Random& random) : TPCCTransaction(random) {};
        ~OrderStatusTransaction() = default;
        
        // 构造OrderStatus事务
        OrderStatusTransaction::Ptr makeOrderStatus() {
            OrderStatusTransaction::Ptr orderStatusTx = std::make_shared<OrderStatusTransaction>(random);
            string wdc_key;

            do {
                orderStatusTx->w_id = random.uniform_dist(1, TPCC::N_WAREHOUSES);
                orderStatusTx->d_id = random.uniform_dist(1, TPCC::N_DISTRICTS);
            
                int y = random.uniform_dist(1, 100);
                int32_t temp_c_id;
                
                if (y <= 60) {
                    // 保证c_last对应的c_id不为空
                    do { 
                        orderStatusTx->c_last = random.rand_last_name(random.non_uniform_distribution(255, 0, 999));
                    } while (c_last_to_c_id.at(orderStatusTx->c_last).size() == 0);
                    orderStatusTx->c_id = -1;
                    auto c_ids = c_last_to_c_id.at(orderStatusTx->c_last);
                    temp_c_id = c_ids[(c_ids.size() - 1) / 2];
                } else {
                    orderStatusTx->c_id = random.non_uniform_distribution(1023, 1, TPCC::N_CUSTOMERS);
                    orderStatusTx->c_last = "";
                    temp_c_id = orderStatusTx->c_id;
                }
                
                wdc_key = std::to_string(orderStatusTx->w_id) + "-" + std::to_string(orderStatusTx->d_id) + "-" + std::to_string(temp_c_id);
            } while(wdc_latestOrder[wdc_key].o_id == 0);
            
            return orderStatusTx;
        }

        // 生成嵌套事务
        TPCCTransaction::Ptr makeTransaction() override {
            coutConditional << "======= making order status transaction =======" << endl;

            auto orderStatusTx = makeOrderStatus();
            coutConditional << "orderStatusTx: w_id = " << orderStatusTx->w_id << 
                                    ", d_id = " << orderStatusTx->d_id <<
                                    ", c_id = " << orderStatusTx->c_id << 
                                    ", c_last = " << orderStatusTx->c_last << endl;

            /* 事务逻辑：
                1. customer表: 读取c_balance, c_first, c_middle, c_last字段
                    1.1 利用c_w_id, c_d_id, c_id精确查询
                    1.2 利用c_w_id, c_d_id, c_last范围查询 => 转化为c_ids => 取第n/2(向上取整)个id
                2. order表: 根据w_id, d_id, c_id查找最近的o_id, 读取o_id字段 => 查找wdc_latestOrder表
                3. orderLine表: 根据w_id, d_id, o_id查找, 读取ol_i_id, ol_supply_w_id, ol_quantity, ol_amount等字段
                需要额外维护的数据：(w_id-d_id-c_id, {o_id, o_ol_cnt})
            */
            
            // 根子事务：可能为customer子事务
            TPCCTransaction::Ptr root;
            // customer子事务
            TPCCTransaction::Ptr cAccess = std::make_shared<TPCCTransaction>(random);
            // order子事务
            TPCCTransaction::Ptr oAccess = std::make_shared<TPCCTransaction>(random);
            oAccess->setExecutionTime(TPCC::ConsumptionType::HIGH);

            if (orderStatusTx->c_id == -1) {
                auto& c_ids = c_last_to_c_id.at(orderStatusTx->c_last);
                for (auto& c_id : c_ids) {
                    cAccess->addReadRow("C-" + std::to_string(orderStatusTx->w_id) + "-" + std::to_string(orderStatusTx->d_id) + "-" + std::to_string(c_id));
                }

                orderStatusTx->c_id = c_ids[(c_ids.size() - 1) / 2];
                // coutConditional << "now have orderStatusTx->c_id: " << orderStatusTx->c_id << endl;

                // 设置customer子事务执行时间
                cAccess->setExecutionTime(TPCC::ConsumptionType::HIGH);
                // customer子事务添加依赖
                cAccess->addChild(oAccess, loom::DependencyType::WEAK);
                // 根节点为customer子事务
                root = cAccess;
            } else {
                // coutConditional << "already have orderStatusTx->c_id: " << orderStatusTx->c_id << endl;
                cAccess->addReadRow("C-" + std::to_string(orderStatusTx->w_id) + "-" + std::to_string(orderStatusTx->d_id) + "-" + std::to_string(orderStatusTx->c_id));

                // 新建根子事务
                root = std::make_shared<TPCCTransaction>(random);
                root->setExecutionTime(TPCC::ConsumptionType::LOW);
                root->addChild(cAccess, loom::DependencyType::WEAK);
                root->addChild(oAccess, loom::DependencyType::WEAK);
            }
            
            string wdc_key = std::to_string(orderStatusTx->w_id) + "-" + std::to_string(orderStatusTx->d_id) + "-" + std::to_string(orderStatusTx->c_id);
            // 没有order则直接返回
            if (wdc_latestOrder.count(wdc_key) == 0) {
                // coutConditional << "\tWARNING: wdc_latestOrder[" << wdc_key << "] is empty..." << endl << endl;
                return root;
            }

            // 有则继续添加子事务和读写集
            auto& latestOrder = wdc_latestOrder.at(wdc_key);
            oAccess->addReadRow("O-" + wdc_key);
            // coutConditional << "latestOrder[" << wdc_key << "] " << 
            //         ": o_id: " << latestOrder.o_id << 
            //         ", o_ol_cnt: " << latestOrder.o_ol_cnt << 
            //         ", o_c_id: " << latestOrder.o_c_id << endl << endl;

            for (int i = 0; i < latestOrder.o_ol_cnt; i++) {
                // orderLine子事务
                TPCCTransaction::Ptr olAccess = std::make_shared<TPCCTransaction>(random);
                olAccess->addReadRow("OL-" + std::to_string(orderStatusTx->w_id) + "-" + std::to_string(orderStatusTx->d_id) + "-" + std::to_string(latestOrder.o_id));
                
                // order子事务添加依赖
                oAccess->addChild(olAccess, loom::DependencyType::WEAK);
            }

            root->setType(TPCC::TransactionType::ORDER_STATUS);
            return root;
        }

    private:
        // OrderStatus事务需要参数
        uint64_t w_id;
        uint64_t d_id;
        int64_t c_id;
        std::string c_last;
};

class DeliveryTransaction : public TPCCTransaction
{
    public:
        typedef std::shared_ptr<DeliveryTransaction> Ptr;
        // DeliveryTransaction() = default;
        DeliveryTransaction(Random& random) : TPCCTransaction(random) {};
        ~DeliveryTransaction() = default;
        
        // 构造Delivery事务
        DeliveryTransaction::Ptr makeDelivery() {
            DeliveryTransaction::Ptr deliveryTx = std::make_shared<DeliveryTransaction>(random);
            deliveryTx->w_id = random.uniform_dist(1, TPCC::N_WAREHOUSES);
            deliveryTx->o_carrier_id = random.uniform_dist(1, TPCC::N_CARRIERS);
            deliveryTx->ol_delivery_d = std::time(0);
            return deliveryTx;
        }

        // 生成嵌套事务
        TPCCTransaction::Ptr makeTransaction() override {
            coutConditional << "======= making delivery transaction =======" << endl;

            auto deliveryTx = makeDelivery();
            coutConditional << "deliveryTx: w_id = " << deliveryTx->w_id << 
                    ", o_carrier_id = " << deliveryTx->o_carrier_id <<
                    ", ol_delivery_d = " << deliveryTx->ol_delivery_d << endl;

            /* 事务逻辑：对warehouse中每个district进行如下操作：(如果和前面的delivery事务有冲突，则需等待)
                1. newOrder表: 根据w_id, d_id查找, 读取no_o_id字段最小的一条记录, 删除该记录 => 通过wd_oldestNewOrder表
                2. order表: 根据w_id, d_id, o_id查找, 读取o_ol_cnt, o_c_id字段, 更新o_carrier_id字段
                3. orderLine表: 根据w_id, d_id, o_id查找, 读取ol_amount字段, 更新ol_delivery_d字段 => 计算ol_amount总和
                4. customer表: 根据w_id, d_id, o_c_id查找, 读取c_balance字段, 更新c_balance字段
                需要额外维护的数据：(w_id-d_id, {o_id, o_c_id, o_ol_cnt})
            */
            // 根事务
            TPCCTransaction::Ptr root = std::make_shared<TPCCTransaction>(random);
            root->setType(TPCC::TransactionType::DELIVERY);
            root->setExecutionTime(TPCC::ConsumptionType::HIGH);

            for (int i = 1; i <= TPCC::N_DISTRICTS; i++) {
                // newOrder子事务 + customer子事务
                TPCCTransaction::Ptr no_cAccess = std::make_shared<TPCCTransaction>(random);
                
                // 获得oldestNewOrder并更新wd_oldestNewOrder
                auto wd_key = std::to_string(deliveryTx->w_id) + "-" + std::to_string(i);
                // 若不存在了，则跳过
                if (wd_oldestNewOrder.count(wd_key) == 0 || wd_oldestNewOrder.at(wd_key).empty()) {
                    coutConditional << "\tWARNING: warehouse: " << deliveryTx->w_id << " distirct: " << i << " has no new order..." << endl;
                    if (i == 10) coutConditional << endl;
                    continue;
                }
                // 取出最旧的一条记录
                const auto oldestNewOrder = wd_oldestNewOrder.at(wd_key).front();

                // coutConditional << "wd_oldestNewOrder[" << wd_key << "] " << 
                //         ": o_id: " << oldestNewOrder.o_id << 
                //         ", o_ol_cnt: " << oldestNewOrder.o_ol_cnt << 
                //         ", o_c_id: " << oldestNewOrder.o_c_id << endl;

                wd_oldestNewOrder.at(wd_key).pop();
                // if (!wd_oldestNewOrder.at(wd_key).empty()) {
                //     coutConditional << "new wd_oldestNewOrder[" << wd_key << "] " << 
                //             ": o_id: " << wd_oldestNewOrder.at(wd_key).front().o_id << 
                //             ", o_ol_cnt: " << wd_oldestNewOrder.at(wd_key).front().o_ol_cnt << 
                //             ", o_c_id: " << wd_oldestNewOrder.at(wd_key).front().o_c_id << endl;
                // }
                // coutConditional << endl;


                // 添加newOrder子事务读写集
                // no_cAccess->addReadRow("NO-" + wd_key);
                no_cAccess->addUpdateRow("NO-" + wd_key + "-" + std::to_string(oldestNewOrder.o_id));
                no_cAccess->addUpdateRow("NO-" + wd_key);

                no_cAccess->setExecutionTime(TPCC::ConsumptionType::HIGH);

                // order子事务
                TPCCTransaction::Ptr oAccess = std::make_shared<TPCCTransaction>(random);
                oAccess->addReadRow("O-" + wd_key + "-" + std::to_string(oldestNewOrder.o_id) + "-" + std::to_string(oldestNewOrder.o_c_id));
                oAccess->addUpdateRow("O-" + wd_key + "-" + std::to_string(oldestNewOrder.o_id) + "-" + std::to_string(oldestNewOrder.o_c_id));
                
                // orderlines子事务
                TPCCTransaction::Ptr olsAccess = std::make_shared<TPCCTransaction>(random);
                for (int j = 0; j < oldestNewOrder.o_ol_cnt; j++) {
                    // orderLine子事务
                    TPCCTransaction::Ptr olAccess = std::make_shared<TPCCTransaction>(random);
                    olAccess->addReadRow("OL-" + wd_key + "-" + std::to_string(oldestNewOrder.o_id));
                    // olAccess->addUpdateRow("OLdelivery-" + wd_key + "-" + std::to_string(oldestNewOrder.o_id) + "-" + std::to_string(j));
                    // orderlines子事务添加依赖
                    olsAccess->addChild(olAccess, loom::DependencyType::STRONG);
                }
                olsAccess->addUpdateRow("OLdelivery-" + wd_key);
                
                // no_cAccess添加依赖   
                no_cAccess->addChild(oAccess, loom::DependencyType::STRONG);
                no_cAccess->addChild(olsAccess, loom::DependencyType::STRONG);

                // 添加customer子事务读写集
                no_cAccess->addReadRow("C-" + wd_key + "-" + std::to_string(oldestNewOrder.o_c_id));
                no_cAccess->addUpdateRow("Cbalance-" + wd_key + "-" + std::to_string(oldestNewOrder.o_c_id));
                no_cAccess->addReadRow("Cdelivery-" + wd_key + "-" + std::to_string(oldestNewOrder.o_c_id));
                no_cAccess->addUpdateRow("Cdelivery-" + wd_key + "-" + std::to_string(oldestNewOrder.o_c_id));

                // root添加依赖
                root->addChild(no_cAccess, loom::DependencyType::WEAK);
            }

            return root;
        }

    private:
        // Delivery事务需要参数
        uint64_t w_id;
        uint64_t o_carrier_id;
        uint64_t ol_delivery_d;
};

class StockLevelTransaction : public TPCCTransaction
{
    public:
        typedef std::shared_ptr<StockLevelTransaction> Ptr;
        // StockLevelTransaction() = default;
        StockLevelTransaction(Random& random) : TPCCTransaction(random) {};
        ~StockLevelTransaction() = default;
        
        // 构造StockLevel事务
        StockLevelTransaction::Ptr makeStockLevel() {
            StockLevelTransaction::Ptr stockLevelTx = std::make_shared<StockLevelTransaction>(random);
            do {
                stockLevelTx->w_id = random.uniform_dist(1, TPCC::N_WAREHOUSES);
                stockLevelTx->d_id = random.uniform_dist(1, TPCC::N_DISTRICTS);
            } while (wd_latestOrderLines.count(std::to_string(stockLevelTx->w_id) + "-" + std::to_string(stockLevelTx->d_id)) == 0);
            
            // stockLevelTx->d_id = random.uniform_dist(1, TPCC::N_DISTRICTS);
            stockLevelTx->threshold = random.uniform_dist(10, 20);
            return stockLevelTx;
        }

        // 生成嵌套事务
        TPCCTransaction::Ptr makeTransaction() override {
            coutConditional << "======= making stock level transaction =======" << endl;

            auto stockLevelTx = makeStockLevel();
            coutConditional << "deliveryTx: w_id = " << stockLevelTx->w_id << 
                    ", d_id = " << stockLevelTx->d_id <<
                    ", threshold = " << stockLevelTx->threshold << endl;

            /* 事务逻辑：(顺序执行可看成一笔事务)
                1. district表: 根据w_id, d_id查找, 读取d_next_o_id字段
                2. orderLine表: 根据w_id, d_id, [d_next_o_id-20, d_next_o_id)查找最近的20条orderLine记录, 读取ol_i_id字段
                3. stock表: 根据w_id, ol_i_id查找, 读取s_quantity字段并统计该字段小于threshold的记录数
                需要额外维护的数据：(d_id, [{o_id, ol_i_id, ol_number}, ...])
            */

            /* 嵌套事务实现 */
            // district子事务
            TPCCTransaction::Ptr dAccess = std::make_shared<TPCCTransaction>(random);
            dAccess->setType(TPCC::TransactionType::STOCK_LEVEL);
            dAccess->addReadRow("D-" + std::to_string(stockLevelTx->w_id) + "-" + std::to_string(stockLevelTx->d_id));
            // 获取d_next_o_id
            uint64_t d_next_o_id = get_order(stockLevelTx->d_id - 1);
            // coutConditional << "In StockLevelTransaction, now next_o_id: " << d_next_o_id << endl;

            // orderLine子事务
            TPCCTransaction::Ptr olAccess = std::make_shared<TPCCTransaction>(random);
            auto wd_key = std::to_string(stockLevelTx->w_id) + "-" + std::to_string(stockLevelTx->d_id);
            // 获取最近的20条orderLine记录
            if (wd_latestOrderLines.count(wd_key) == 0) {
                coutConditional << "\tERROR: distirct " << stockLevelTx->d_id << " has no orderLine..." << endl;
                return nullptr;
            }

            const auto latestOrderLines = wd_latestOrderLines.at(wd_key);            
            for (auto& orderLine : latestOrderLines) {
                // coutConditional << "orderLine: o_id = " << orderLine.o_id << ", ol_i_id = " << orderLine.ol_i_id << endl;

                // stock子事务
                TPCCTransaction::Ptr sAccess = std::make_shared<TPCCTransaction>(random);
                sAccess->addReadRow("Sqtys-" + std::to_string(stockLevelTx->w_id) + "-" + std::to_string(orderLine.ol_i_id));

                // orderLine子事务添加依赖
                olAccess->addChild(sAccess, loom::DependencyType::WEAK);
            }
            olAccess->addReadRow("OL-" + wd_key);
            olAccess->setExecutionTime(TPCC::ConsumptionType::HIGH);

            // district子事务添加依赖
            dAccess->addChild(olAccess, loom::DependencyType::WEAK);
            
            return dAccess;
        }

    private:
        // StockLevel事务需要参数
        uint64_t w_id;
        uint64_t d_id;
        uint64_t threshold;
};

