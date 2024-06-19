#pragma once

#include <memory>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <future>

class ThreadPool : public std::enable_shared_from_this<ThreadPool>
{
    public:
        typedef std::shared_ptr <ThreadPool> Ptr;

        ThreadPool(size_t threadNum);
        
        ~ThreadPool();

        std::future<void> enqueue(std::function<void()> task);

        const std::vector<std::chrono::microseconds>& getThreadDurations() const;

        const std::vector<size_t>& getTaskCounts() const; // 新增获取任务计数的方法

        const int getThreadNum() const;

        std::vector<std::future<void>> BG_futures; // 并发构建时空图的future

    private:
        void PinRoundRobin(std::thread& thread, unsigned rotate_id);
        void PinRoundRobin(pthread_t& thread, unsigned rotate_id);

        
        std::vector<std::thread> workers;
        std::queue<std::function<void()>> tasks;
        std::mutex queue_mutex;
        std::condition_variable condition;
        bool stop;
        

        // 线程统计信息
        std::vector<std::chrono::microseconds> threadDurations; // 存储每个线程的工作时间
        std::vector<size_t> taskCounts; // 新增成员变量存储任务计数
        int threadNum;

        /*
        // 线程池中的工作线程
        std::vector<std::thread> workers;
        // 全局任务队列
        std::queue<std::function<void()>> global_tasks;
        // 本地任务队列
        std::vector<std::shared_ptr<std::deque<std::function<void()>>>> local_queues;

        // 同步机制
        std::mutex global_queue_mutex;
        std::condition_variable condition;
        std::atomic<bool> stop;
        std::vector<std::mutex> local_queue_mutexes; // 每个本地队列一个互斥锁
        std::atomic<size_t> next_queue; // 下一个任务分配到的队列索引
        */
};