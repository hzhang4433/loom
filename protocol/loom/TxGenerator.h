/***************************
@Author: huan
@File: TxGenerator.h
@Desc: 
    1. 封装事务生成过程，提供对外接口
    2. 生成大量事务，并以区块为单位组装事务
    3. 生成事务对应索引
    4. 输入：事务数量，区块大小
    5. 输出：区块，区块内事务索引
***************************/
#pragma once

#include <vector>
#include "common/Block.h"

class TxGenerator {
    public:
        TxGenerator(int txNum); // 构造函数

        ~TxGenerator(){}; // 析构函数

        std::vector<Block::Ptr> generateWorkload(); // 生成负载
        
        Block::Ptr generateBlock(); // 生成区块
        
        HyperVertex::Ptr generateTransaction(const Transaction::Ptr& tx, bool isNest, unordered_map<string, loom::RWSets<Vertex::Ptr>>& invertedIndex); // 生成事务
        
        void generateIndex(vector<Vertex::Ptr> txLists, unordered_map<string, loom::RWSets<Vertex::Ptr>>& invertedIndex, unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& RWIndex, unordered_map<Vertex::Ptr, unordered_set<Vertex::Ptr, Vertex::VertexHash>, Vertex::VertexHash>& conflictIndex, unordered_map<string, set<Vertex::Ptr, Vertex::VertexCompare>>& RBIndex); // 生成索引
        
        std::vector<Block::Ptr> getBlocks(); // 获取区块
        
        int getId(); // 获取事务ID

    private:
        int m_txNum;                        // 生成事务数量
        int m_blockSize;                    // 区块大小
        std::vector<Block::Ptr> m_blocks;   // 产生的所有区块
        std::atomic<int> id_counter;        // 分配事务ID
};