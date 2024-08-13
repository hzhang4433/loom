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

        // 添加任务到任务队列
        template<class F, class... Args>
        auto enqueue(F&& f, Args&&... args) -> std::future<typename std::result_of<F(Args...)>::type> {
            using return_type = typename std::result_of<F(Args...)>::type;

            // 创建一个packaged_task，它包装了你的任务
            auto task = std::make_shared<std::packaged_task<return_type()>>(
                std::bind(std::forward<F>(f), std::forward<Args>(args)...)
            );

            // 获取与packaged_task关联的future
            std::future<return_type> future = task->get_future();
            {
                // 锁住任务队列
                std::unique_lock<std::mutex> lock(queue_mutex);
                // 如果收到停止信号，就抛出异常
                if(stop) throw std::runtime_error("enqueue on stopped ThreadPool");
                // 将任务添加到任务队列
                tasks.emplace([task](){ (*task)(); });
            }
            // 唤醒一个等待的线程
            condition.notify_one();
            return future;
        }

        const std::vector<std::chrono::microseconds>& getThreadDurations() const;

        const std::vector<size_t>& getTaskCounts() const; // 新增获取任务计数的方法

        const int getThreadNum() const;

        void shutdown();

        std::vector<std::future<void>> BG_futures; // 并发构建时空图的future

        static void PinRoundRobin(std::thread& thread, unsigned rotate_id);
        static void PinRoundRobin(pthread_t& thread, unsigned rotate_id);
        static void PinRoundRobin(std::jthread& thread, unsigned rotate_id);

    private:
        
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