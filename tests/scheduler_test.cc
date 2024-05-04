#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <numeric>
#include <stdexcept>
#include <thread>
#include <vector>

#include "stealpool/scheduler.h"

using namespace stealpool;

namespace {

uint64_t Fib(Scheduler& s, int n) {
  if (n < 2) return uint64_t(n);
  if (n < 12) return Fib(s, n - 1) + Fib(s, n - 2);  // below this, spawning costs more than it saves
  uint64_t a = 0, b = 0;
  TaskGroup g(s);
  g.Spawn([&] { a = Fib(s, n - 1); });
  b = Fib(s, n - 2);
  g.Wait();
  return a + b;
}

}  // namespace

TEST(Scheduler, RunsEveryTaskOnce) {
  Scheduler s(4);
  constexpr int kTasks = 100000;
  std::vector<std::atomic<int>> hits(kTasks);
  for (auto& h : hits) h.store(0);
  {
    TaskGroup g(s);
    for (int i = 0; i < kTasks; ++i) g.Spawn([&hits, i] { hits[size_t(i)].fetch_add(1); });
    g.Wait();
  }
  for (int i = 0; i < kTasks; ++i) ASSERT_EQ(hits[size_t(i)].load(), 1) << i;
  EXPECT_GE(s.stats().tasks_run, uint64_t(kTasks));
}

TEST(Scheduler, NestedForkJoin) {
  Scheduler s(4);
  EXPECT_EQ(Fib(s, 27), 196418u);  // from outside the pool
  // From inside: all spawns land in one worker's deque, so the only way the
  // other workers get any of it is by stealing.
  const uint64_t before = s.stats().steals;
  EXPECT_EQ(s.Async([&] { return Fib(s, 30); }).Get(), 832040u);
  if (s.num_threads() > 1) {
    EXPECT_GT(s.stats().steals, before);
  }
}


TEST(Scheduler, NestedWaitWithOneThreadDoesNotDeadlock) {
  // With a single worker, a task waiting on its children must run them itself.
  Scheduler s(1);
  EXPECT_EQ(Fib(s, 20), 6765u);
  auto f = s.Async([&] { return Fib(s, 18); });
  EXPECT_EQ(f.Get(), 2584u);
}

TEST(Scheduler, ParallelForCoversRangeExactlyOnce) {
  Scheduler s(3);
  for (size_t n : {0u, 1u, 7u, 1000u, 12345u}) {
    for (size_t grain : {1u, 16u, 5000u}) {
      std::vector<std::atomic<int>> hits(n);
      for (auto& h : hits) h.store(0);
      ParallelFor(s, 0, n, grain, [&](size_t i) { hits[i].fetch_add(1); });
      for (size_t i = 0; i < n; ++i) ASSERT_EQ(hits[i].load(), 1) << "n=" << n << " grain=" << grain << " i=" << i;
    }
  }
  // Non-zero begin.
  std::atomic<size_t> sum{0};
  ParallelFor(s, 100, 200, 8, [&](size_t i) { sum += i; });
  EXPECT_EQ(sum.load(), size_t(14950));
}

TEST(Scheduler, ParallelReduce) {
  Scheduler s(4);
  std::vector<uint64_t> v(1 << 20);
  std::iota(v.begin(), v.end(), 1);
  const uint64_t total = ParallelReduce(
      s, 0, v.size(), 4096, uint64_t{0},
      [&](size_t lo, size_t hi) { return std::accumulate(v.begin() + long(lo), v.begin() + long(hi), uint64_t{0}); },
      [](uint64_t a, uint64_t b) { return a + b; });
  EXPECT_EQ(total, uint64_t(v.size()) * (v.size() + 1) / 2);
}

TEST(Scheduler, FirstExceptionIsRethrownAndOtherTasksStillRun) {
  Scheduler s(4);
  std::atomic<int> ran{0};
  TaskGroup g(s);
  for (int i = 0; i < 100; ++i) {
    g.Spawn([&, i] {
      ran++;
      if (i % 10 == 3) throw std::runtime_error("task " + std::to_string(i));
    });
  }
  EXPECT_THROW(g.Wait(), std::runtime_error);
  EXPECT_EQ(ran.load(), 100);
  // The error was consumed; the group is reusable.
  g.Spawn([&] { ran++; });
  EXPECT_NO_THROW(g.Wait());
  EXPECT_EQ(ran.load(), 101);
}

TEST(Scheduler, AsyncReturnsValuesAndExceptions) {
  Scheduler s(2);
  auto a = s.Async([] { return std::string("hello"); });
  auto b = s.Async([]() -> int { throw std::logic_error("nope"); });
  EXPECT_EQ(a.Get(), "hello");
  EXPECT_THROW(b.Get(), std::logic_error);
}

TEST(Scheduler, ManyExternalThreadsSubmitting) {
  Scheduler s(3);
  std::atomic<int> count{0};
  std::vector<std::thread> clients;
  for (int c = 0; c < 6; ++c) {
    clients.emplace_back([&] {
      TaskGroup g(s);
      for (int i = 0; i < 5000; ++i) g.Spawn([&] { count.fetch_add(1, std::memory_order_relaxed); });
      g.Wait();
    });
  }
  for (auto& c : clients) c.join();
  EXPECT_EQ(count.load(), 30000);
  EXPECT_EQ(s.stats().injected, 30000u);
}

TEST(Scheduler, IdleWorkersParkAndWakeUpForNewWork) {
  Scheduler s(4);
  auto all_parked = [&] {
    for (int i = 0; i < 400; ++i) {
      if (s.sleeping() == int(s.num_threads())) return true;
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return false;
  };
  ASSERT_TRUE(all_parked()) << "idle workers should sleep, not spin";
  // Work submitted after everyone is asleep must still run.
  for (int round = 0; round < 50; ++round) {
    std::atomic<int> n{0};
    TaskGroup g(s);
    for (int i = 0; i < 10; ++i) g.Spawn([&] { n++; });
    g.Wait();
    ASSERT_EQ(n.load(), 10);
  }
  EXPECT_TRUE(all_parked());
  EXPECT_GT(s.stats().parks, 0u);
}

TEST(Scheduler, DestructorFinishesQueuedWork) {
  std::atomic<int> n{0};
  {
    Scheduler s(2);
    for (int i = 0; i < 1000; ++i) s.Spawn([&] {
        std::this_thread::sleep_for(std::chrono::microseconds(10));
        n++;
      });
  }
  EXPECT_EQ(n.load(), 1000);
}

TEST(Scheduler, GroupDestroyedRightAfterWait) {
  // Regression guard for the finishing_ counter: tasks must be completely done
  // with the group before Wait() returns and the group goes away.
  Scheduler s(4);
  for (int round = 0; round < 2000; ++round) {
    auto g = std::make_unique<TaskGroup>(s);
    std::atomic<int> n{0};
    for (int i = 0; i < 4; ++i) g->Spawn([&] { n++; });
    g->Wait();
    g.reset();
    ASSERT_EQ(n.load(), 4);
  }
}
