// A work-stealing task scheduler.
//
//   stealpool::Scheduler sched;              // one worker per core
//   stealpool::TaskGroup g(sched);
//   g.Spawn([] { ... });
//   g.Spawn([] { ... });
//   g.Wait();                                 // helps run tasks while waiting
//
//   stealpool::ParallelFor(sched, 0, n, 1024, [&](size_t i) { ... });
//   auto f = sched.Async([] { return 42; });  // f.Get()
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "stealpool/deque.h"

namespace stealpool {

class Scheduler;
class TaskGroup;

namespace detail {

struct Task {
  virtual ~Task() = default;
  virtual void Run() = 0;
};

template <typename F>
struct FnTask final : Task {
  explicit FnTask(F&& f) : fn(std::move(f)) {}
  void Run() override { fn(); }
  F fn;
};

template <typename F>
Task* MakeTask(F&& f) {
  return new FnTask<std::decay_t<F>>(std::forward<F>(f));
}

}  // namespace detail

struct SchedulerStats {
  uint64_t tasks_run = 0;
  uint64_t steals = 0;         // tasks taken from another worker's deque
  uint64_t failed_steals = 0;  // attempts that found nothing or lost the race
  uint64_t injected = 0;       // tasks submitted from outside the pool
  uint64_t parks = 0;          // times a worker went to sleep
};

class Scheduler {
 public:
  explicit Scheduler(unsigned num_threads = 0);  // 0 = hardware_concurrency
  ~Scheduler();                                  // finishes queued work first
  Scheduler(const Scheduler&) = delete;
  Scheduler& operator=(const Scheduler&) = delete;

  // Fire and forget. Prefer TaskGroup when you need to know it finished.
  template <typename F>
  void Spawn(F&& f) {
    Submit(detail::MakeTask(std::forward<F>(f)));
  }

  template <typename F>
  auto Async(F&& f);

  unsigned num_threads() const { return unsigned(workers_.size()); }
  SchedulerStats stats() const;
  // Workers currently parked. Mostly for tests.
  int sleeping() const { return sleepers_.load(std::memory_order_relaxed); }

  // Runs one pending task on the calling thread if there is one. Used by
  // waits so a blocked worker keeps the pool busy instead of idling.
  bool RunOneTask();

  // Index of the worker the calling thread is, or -1.
  int CurrentWorker() const;

 private:
  friend class TaskGroup;
  struct Worker;

  void Submit(detail::Task* t);
  void WorkerLoop(unsigned index);
  detail::Task* FindWork(Worker* self);
  bool TrySteal(Worker* self, detail::Task** out);
  void Execute(Worker* self, detail::Task* t);
  void WakeOne();
  bool AnyWorkVisible() const;

  std::vector<std::unique_ptr<Worker>> workers_;
  std::vector<std::thread> threads_;

  // Tasks from threads outside the pool.
  mutable std::mutex inject_mu_;
  std::deque<detail::Task*> injected_;
  std::atomic<size_t> injected_size_{0};

  // Parking. `epoch_` is an event count: a worker reads it, re-checks for
  // work, and only sleeps if nobody bumped it in between.
  std::mutex park_mu_;
  std::condition_variable park_cv_;
  std::atomic<uint64_t> epoch_{0};
  std::atomic<int> sleepers_{0};
  std::atomic<bool> stop_{false};

  std::atomic<uint64_t> injected_count_{0};
};

// A set of tasks you can wait on. Exceptions thrown by tasks are captured and
// the first one is rethrown from Wait().
class TaskGroup {
 public:
  explicit TaskGroup(Scheduler& s) : sched_(s) {}
  ~TaskGroup() { WaitNoThrow(); }
  TaskGroup(const TaskGroup&) = delete;
  TaskGroup& operator=(const TaskGroup&) = delete;

  template <typename F>
  void Spawn(F&& f) {
    pending_.fetch_add(1, std::memory_order_relaxed);
    sched_.Submit(detail::MakeTask([this, fn = std::forward<F>(f)]() mutable {
      try {
        fn();
      } catch (...) {
        std::lock_guard<std::mutex> lk(mu_);
        if (!error_) error_ = std::current_exception();
      }
      Done();
    }));
  }

  // Blocks until every task spawned so far has finished. On a worker thread
  // it runs other tasks meanwhile, so nested waits can't deadlock the pool.
  void Wait();

  Scheduler& scheduler() { return sched_; }

 private:
  void Done();
  void WaitNoThrow();

  Scheduler& sched_;
  std::atomic<int64_t> pending_{0};
  // Tasks currently inside Done(). Wait() doesn't return until this is 0, so
  // a task can't still be touching the group when the owner destroys it.
  std::atomic<int64_t> finishing_{0};
  std::mutex mu_;
  std::condition_variable cv_;
  std::exception_ptr error_;
};

template <typename T>
class Future {
 public:
  Future() = default;
  bool Ready() const { return state_->ready.load(std::memory_order_acquire); }
  // Waits (helping if on a worker) and returns the value or rethrows.
  T Get();

 private:
  friend class Scheduler;
  struct State {
    std::atomic<bool> ready{false};
    std::optional<T> value;
    std::exception_ptr error;
    std::mutex mu;
    std::condition_variable cv;
  };
  explicit Future(std::shared_ptr<State> s, Scheduler* sched) : state_(std::move(s)), sched_(sched) {}
  std::shared_ptr<State> state_;
  Scheduler* sched_ = nullptr;
};

template <typename F>
auto Scheduler::Async(F&& f) {
  using T = std::invoke_result_t<std::decay_t<F>>;
  static_assert(!std::is_void_v<T>, "Async needs a value; use TaskGroup for void work");
  auto st = std::make_shared<typename Future<T>::State>();
  Submit(detail::MakeTask([st, fn = std::forward<F>(f)]() mutable {
    try {
      st->value.emplace(fn());
    } catch (...) {
      st->error = std::current_exception();
    }
    {
      std::lock_guard<std::mutex> lk(st->mu);
      st->ready.store(true, std::memory_order_release);
    }
    st->cv.notify_all();
  }));
  return Future<T>(st, this);
}

template <typename T>
T Future<T>::Get() {
  if (sched_->CurrentWorker() >= 0) {
    while (!Ready()) {
      if (!sched_->RunOneTask()) std::this_thread::yield();
    }
  } else {
    std::unique_lock<std::mutex> lk(state_->mu);
    state_->cv.wait(lk, [&] { return Ready(); });
  }
  if (state_->error) std::rethrow_exception(state_->error);
  return std::move(*state_->value);
}

// Calls f(i) for every i in [begin, end). Splits the range in halves down to
// `grain`, so the first steal takes half the remaining work.
template <typename F>
void ParallelFor(Scheduler& s, size_t begin, size_t end, size_t grain, F&& f) {
  if (grain == 0) grain = 1;
  TaskGroup g(s);
  std::function<void(size_t, size_t)> split = [&](size_t lo, size_t hi) {
    while (hi - lo > grain) {
      const size_t mid = lo + (hi - lo) / 2;
      g.Spawn([&split, mid, hi] { split(mid, hi); });
      hi = mid;
    }
    for (size_t i = lo; i < hi; ++i) f(i);
  };
  if (begin < end) split(begin, end);
  g.Wait();
}

// Maps [begin, end) in chunks of `grain` with chunk(lo, hi) -> T and folds the
// results with combine. combine must be associative.
template <typename T, typename Chunk, typename Combine>
T ParallelReduce(Scheduler& s, size_t begin, size_t end, size_t grain, T identity, Chunk&& chunk,
                 Combine&& combine) {
  if (grain == 0) grain = 1;
  const size_t n = end > begin ? (end - begin + grain - 1) / grain : 0;
  std::vector<T> partial(n, identity);
  ParallelFor(s, 0, n, 1, [&](size_t c) {
    const size_t lo = begin + c * grain;
    partial[c] = chunk(lo, std::min(end, lo + grain));
  });
  T acc = identity;
  for (auto& p : partial) acc = combine(acc, p);
  return acc;
}

}  // namespace stealpool
