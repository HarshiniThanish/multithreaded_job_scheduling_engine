#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>
#include <unordered_map>
#include <sqlite3.h>

struct Job {
    std::int64_t id{};
    std::string name;
    int priority{0};
    int retries_left{2};
    int simulated_failures{0};
    std::chrono::steady_clock::time_point created_at{std::chrono::steady_clock::now()};
};
struct JobCompare {
    bool operator()(const Job& a, const Job& b) const {
        if (a.priority == b.priority) return a.id > b.id;
        return a.priority < b.priority; // higher priority first
    }
};
struct JobStats {
    std::uint64_t submitted=0, succeeded=0, failed=0, retried=0;
    std::size_t queued=0, running=0;
    double avg_latency_ms=0, p95_latency_ms=0;
};

class Scheduler {
public:
    explicit Scheduler(std::size_t workers, const std::string& db_path);
    ~Scheduler();
    std::int64_t submit(std::string name, int priority, int failures);
    std::vector<std::string> jobs_json();
    JobStats stats();
    void stop();
private:
    void worker_loop();
    void persist(const Job& job, const std::string& status, const std::string& error="");
    std::mutex mutex_;
    std::condition_variable cv_;
    std::priority_queue<Job,std::vector<Job>,JobCompare> queue_;
    std::vector<std::thread> workers_;
    bool stopping_=false;
    std::size_t running_=0;
    std::int64_t next_id_=1;
    std::uint64_t submitted_=0, succeeded_=0, failed_=0, retried_=0;
    std::vector<double> latencies_;
    sqlite3* db_=nullptr;
    std::mutex db_mutex_;
};
