#pragma once

#include <memory>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <future>
#include <loom/common/common.h>

using namespace loom;

struct Task {
    TaskPriority priority;
    std::function<void()> func;
    
    // compare function for priority_queue
    bool operator<(const Task& other) const {
        return static_cast<int>(priority) > static_cast<int>(other.priority);
    }
};

class ThreadPool : public std::enable_shared_from_this<ThreadPool>
{
    public:
        typedef std::shared_ptr <ThreadPool> Ptr;

        ThreadPool(size_t threadNum);
        
        ~ThreadPool();

        // std::future<void> enqueue(std::function<void()> task);
        // std::future<void> enqueue(std::function<void()> task, TaskPriority priority = TaskPriority::HIGH_PRIORITY);
        
        // 添加任务到任务队列
        /*
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
        */

        template<class F, class... Args>
        auto enqueue(F&& f, Args&&... args, TaskPriority priority = TaskPriority::HIGH_PRIORITY) -> std::future<typename std::result_of<F(Args...)>::type> {
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
                // 将任务添加到任务队列，按优先级插入
                // tasks.push({priority, [task]() { (*task)(); }});
                
                // 根据优先级将任务放入不同的队列
                if (priority == TaskPriority::HIGH_PRIORITY) {
                    highPriorityTasks.push([task]() { (*task)(); });
                } else {
                    lowPriorityTasks.push([task]() { (*task)(); });
                }
            }
            // 唤醒一个等待的线程
            condition.notify_one();
            return future;
        }

        template<class F, class... Args>
        auto enqueueBatch(const std::vector<std::tuple<F, Args...>>& taskList, TaskPriority priority = TaskPriority::HIGH_PRIORITY) 
            -> std::vector<std::future<typename std::result_of<F(Args...)>::type>> {
            using return_type = typename std::result_of<F(Args...)>::type;
            std::vector<std::future<return_type>> futures;
            futures.reserve(taskList.size());  // 预先分配足够的空间

            {
                std::unique_lock<std::mutex> lock(queue_mutex);  // 锁住任务队列

                // 如果线程池已经停止，抛出异常
                if (stop) throw std::runtime_error("enqueue on stopped ThreadPool");

                for (const auto& taskTuple : taskList) {
                    // 创建一个packaged_task，将任务包装
                    auto task = std::make_shared<std::packaged_task<return_type()>>(
                        std::bind(std::get<0>(taskTuple), std::get<Args>(taskTuple)...)
                    );

                    // 获取与packaged_task关联的future，并将其存储
                    futures.push_back(task->get_future());

                    // 根据优先级将任务放入不同的队列
                    if (priority == TaskPriority::HIGH_PRIORITY) {
                        highPriorityTasks.push([task]() { (*task)(); });
                    } else {
                        lowPriorityTasks.push([task]() { (*task)(); });
                    }
                }
            }
            
            // 唤醒所有等待的线程，处理新任务
            condition.notify_all();  
            return futures;  // 返回future列表
        }

        const std::vector<std::chrono::microseconds>& getThreadDurations() const;

        const std::vector<size_t>& getTaskCounts() const; // 新增获取任务计数的方法

        const int getThreadNum() const;

        void shutdown();

        static void PinRoundRobin(std::thread& thread, unsigned rotate_id);
        static void PinRoundRobin(pthread_t& thread, unsigned rotate_id);
        static void PinRoundRobin(std::jthread& thread, unsigned rotate_id);

    private:
        
        std::vector<std::thread> workers;
        // std::queue<std::function<void()>> tasks;
        // std::priority_queue<Task> tasks;
        std::queue<std::function<void()>> highPriorityTasks; // 高优先级任务队列
        std::queue<std::function<void()>> lowPriorityTasks;  // 低优先级任务队列

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