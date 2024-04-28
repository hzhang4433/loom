#pragma once

#include <cstdint>
#include <string>

struct Warehouse {
    // Primary Key - w_id
    uint64_t w_id;          // 仓库编号
    // Other
    uint64_t w_ytd;         // 仓库总收入
};

struct District {
    // Primary Key - (d_id, d_w_id)
    // Foreign Key - (d_w_id) => (w_id)
    uint64_t d_id;          // 区域编号
    uint64_t d_w_id;        // 仓库编号
    // Other
    uint64_t d_ytd;         // 区域总收入
    uint64_t d_next_o_id;   // 下一个订单编号
};

struct Customer {
    // Primary Key - (c_id, c_d_id, c_w_id)
    // Foreign Key - (c_d_id, c_w_id) => (d_id, w_id)
    uint64_t c_id;          // 客户编号
    uint64_t c_d_id;        // 区域编号
    uint64_t c_w_id;        // 仓库编号
    // Other
    uint64_t c_balance;     // 余额
    std::string c_last;     // 姓氏
};

struct History {
    // Primary Key - none
    // Foreign Key - (h_c_id, h_c_d_id, h_c_w_id) => (c_id, c_d_id, c_w_id)
    uint64_t h_c_id;        // 客户编号
    uint64_t h_c_d_id;      // 客户所在区域编号
    uint64_t h_c_w_id;      // 客户所在仓库编号
    // Foreign Key - (h_d_id, h_w_id) => (d_id, d_w_id)
    uint64_t h_d_id;        // 区域编号
    uint64_t h_w_id;        // 仓库编号
    // Other
    uint64_t h_amount;      // 支付金额
};

struct NewOrder {
    // Primary Key - (no_o_id, no_d_id, no_w_id)
    // Foreign Key - (no_o_id, no_d_id, no_w_id) => Order(o_id, o_d_id, o_w_id)
    uint64_t no_o_id;       // 订单编号
    uint64_t no_d_id;       // 区域编号
    uint64_t no_w_id;       // 仓库编号
};

struct Order {
    // Primary Key - (o_id, o_d_id, o_w_id)
    // Foreign Key - (o_d_id, o_w_id, o_c_id) => (c_d_id, c_w_id, c_id)
    uint64_t o_id;          // 订单编号
    uint64_t o_d_id;        // 区域编号
    uint64_t o_w_id;        // 仓库编号
    uint64_t o_c_id;        // 客户编号
    // Other
    uint64_t o_entry_d;     // 订单日期
    uint64_t o_carrier_id;  // 承运人编号
    uint32_t o_ol_cnt;      // 订单中的商品数量
};

struct OrderLine {
    // Primary Key - (ol_o_id, ol_d_id, ol_w_id, ol_number)
    // Foreign Key - (ol_o_id, ol_d_id, ol_w_id) => (o_id, o_d_id, o_w_id)
    uint64_t ol_o_id;       // 订单编号
    uint64_t ol_d_id;       // 区域编号
    uint64_t ol_w_id;       // 仓库编号
    uint64_t ol_number;     // 订单行号
    // Foreign Key - (ol_i_id, ol_supply_w_id) => (s_i_id, s_w_id)
    uint64_t ol_i_id;       // 商品编号
    uint64_t ol_supply_w_id;// 供应仓库编号
    // Other
    uint64_t ol_delivery_d; // 交货日期
    uint64_t ol_quantity;   // 商品数量
    uint64_t ol_amount;     // 商品金额
    // uint64_t ol_dist_info;  // 区域信息
};

struct Item {
    // Primary Key - i_id
    uint64_t i_id;          // 商品编号
    // Other
    uint64_t i_price;       // 商品价格
};

struct Stock {
    // Primary Key - (s_i_id, s_w_id)
    // Foreign Key - (s_i_id) => (i_id)
    // Foreign Key - (s_w_id) => (w_id)
    uint64_t s_i_id;        // 商品编号
    uint64_t s_w_id;        // 仓库编号
    // Other
    uint64_t s_quantity;    // 商品数量
    uint64_t s_ytd;         // 商品总销售数量
    uint64_t s_order_cnt;   // 商品订单数量
    // uint64_t s_dist_info;   // 区域信息
};