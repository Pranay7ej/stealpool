// Compares stealpool against running serially and against a plain thread pool
// with one mutex-protected queue (the usual first thing people write).
//
//   bench [--threads N] [--reps R]
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

#include "stealpool/scheduler.h"

using Clock = std::chrono::steady_clock;

namespace {

// ---------------------------------------------------------------- baseline

class CentralPool {
 public:
  explicit CentralPool(unsigned n) {
    for (unsigned i = 0; i < n; ++i)
      threads_.emplace_back([this] {
        tls_pool = this;
        std::unique_lock<std::mutex> lk(mu_);
        while (true) {
          cv_.wait(lk, [&] { return stop_ || !q_.empty(); });
          if (q_.empty()) return;
          auto f = std::move(q_.front());
          q_.pop_front();
          lk.unlock();
          f();
          lk.lock();
        }
      });
  }
  ~CentralPool() {
    {
      std::lock_guard<std::mutex> lk(mu_);
      stop_ = true;
    }
    cv_.notify_all();
    for (auto& t : threads_) t.join();
  }
  void Push(std::function<void()> f) {
    {
      std::lock_guard<std::mutex> lk(mu_);
      q_.push_back(std::move(f));
    }
    cv_.notify_one();
  }
  bool RunOne() {
    std::function<void()> f;
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (q_.empty()) return false;
      f = std::move(q_.front());
      q_.pop_front();
    }
    f();
    return true;
  }
  static thread_local CentralPool* tls_pool;

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<std::function<void()>> q_;
  std::vector<std::thread> threads_;
  bool stop_ = false;
};
thread_local CentralPool* CentralPool::tls_pool = nullptr;

// Same shape as stealpool::TaskGroup so the workloads can be written once.
class CentralGroup {
 public:
  explicit CentralGroup(CentralPool& p) : p_(p) {}
  template <typename F>
  void Spawn(F&& f) {
    pending_++;
    p_.Push([this, f = std::forward<F>(f)]() mutable {
      f();
      pending_--;
    });
  }
  void Wait() {
    while (pending_.load() > 0)
      if (!p_.RunOne()) std::this_thread::yield();
  }

 private:
  CentralPool& p_;
  std::atomic<int> pending_{0};
};

// ---------------------------------------------------------------- workloads

uint64_t FibSerial(int n) { return n < 2 ? uint64_t(n) : FibSerial(n - 1) + FibSerial(n - 2); }

template <typename Pool, typename Group>
uint64_t Fib(Pool& p, int n) {
  if (n < 20) return FibSerial(n);  // same leaf code as the serial run, so only scheduling differs
  uint64_t a = 0;
  Group g(p);
  g.Spawn([&] { a = Fib<Pool, Group>(p, n - 1); });
  uint64_t b = Fib<Pool, Group>(p, n - 2);
  g.Wait();
  return a + b;
}

// An unbalanced tree: each node does a bit of work and has a random number of
// children, with some subtrees much deeper than others. Static splitting can't
// balance this; stealing can.
uint64_t Work(uint64_t seed) {
  uint64_t x = seed;
  for (int i = 0; i < 2000; ++i) x = x * 6364136223846793005ULL + 1442695040888963407ULL;
  return x;
}

int Children(uint64_t seed, int depth) {
  if (depth > 18) return 0;
  const uint64_t r = (seed >> 33) % 100;
  return r < 30 ? 0 : (r < 70 ? 2 : 3);
}

template <typename Pool, typename Group>
uint64_t Tree(Pool& p, uint64_t seed, int depth) {
  const uint64_t base = Work(seed);
  const int kids = Children(base, depth);
  if (kids == 0) return base;
  std::vector<uint64_t> results(size_t(kids), 0);
  Group g(p);
  for (int i = 1; i < kids; ++i)
    g.Spawn([&, i] { results[size_t(i)] = Tree<Pool, Group>(p, base + uint64_t(i), depth + 1); });
  results[0] = Tree<Pool, Group>(p, base, depth + 1);
  g.Wait();
  uint64_t acc = base;
  for (auto r : results) acc ^= r;
  return acc;
}

uint64_t TreeSerial(uint64_t seed, int depth) {
  const uint64_t base = Work(seed);
  const int kids = Children(base, depth);
  uint64_t acc = base;
  for (int i = 0; i < kids; ++i) acc ^= TreeSerial(base + uint64_t(i), depth + 1);
  return acc;
}

template <typename F>
double Best(int reps, F&& f) {
  double best = 1e18;
  for (int r = 0; r < reps; ++r) {
    auto t0 = Clock::now();
    f();
    best = std::min(best, std::chrono::duration<double, std::milli>(Clock::now() - t0).count());
  }
  return best;
}

volatile uint64_t g_sink;
volatile uint64_t g_seed = 42;  // read at runtime so the compiler can't fold the serial runs
volatile int g_fib_n = 36;

void Check(const char* what, uint64_t expected, uint64_t got) {
  if (expected != got) {
    std::fprintf(stderr, "%s: wrong result %llu, expected %llu\n", what, static_cast<unsigned long long>(got),
                 static_cast<unsigned long long>(expected));
    std::exit(1);
  }
}

}  // namespace

int main(int argc, char** argv) {
  unsigned threads = std::max(1u, std::thread::hardware_concurrency());
  int reps = 5;
  for (int i = 1; i < argc; ++i) {
    if (!std::strcmp(argv[i], "--threads") && i + 1 < argc) threads = unsigned(std::atoi(argv[++i]));
    else if (!std::strcmp(argv[i], "--reps") && i + 1 < argc) reps = std::atoi(argv[++i]);
  }
  std::printf("threads: %u (hardware: %u), best of %d\n\n", threads, std::thread::hardware_concurrency(), reps);

  stealpool::Scheduler sp(threads);
  CentralPool cp(threads);
  const int fib_n = g_fib_n;

  std::printf("| workload | serial ms | central queue ms | stealpool ms | stealpool speedup |\n");
  std::printf("|---|---:|---:|---:|---:|\n");

  {
    const uint64_t want = FibSerial(fib_n);
    const double s = Best(reps, [&] { g_sink = FibSerial(g_fib_n); });
    const double c = Best(reps, [&] { g_sink = Fib<CentralPool, CentralGroup>(cp, fib_n); });
    Check("fib central", want, g_sink);
    const double w = Best(reps, [&] {
      g_sink = sp.Async([&] { return Fib<stealpool::Scheduler, stealpool::TaskGroup>(sp, fib_n); }).Get();
    });
    Check("fib stealpool", want, g_sink);
    std::printf("| fib(%d), cutoff 20 | %.1f | %.1f | %.1f | %.2fx |\n", fib_n, s, c, w, s / w);
  }
  {
    const uint64_t want = TreeSerial(g_seed, 0);
    const double s = Best(reps, [] { g_sink = TreeSerial(g_seed, 0); });
    const double c = Best(reps, [&] { g_sink = Tree<CentralPool, CentralGroup>(cp, g_seed, 0); });
    Check("tree central", want, g_sink);
    const double w = Best(reps, [&] {
      g_sink = sp.Async([&] { return Tree<stealpool::Scheduler, stealpool::TaskGroup>(sp, g_seed, 0); }).Get();
    });
    Check("tree stealpool", want, g_sink);
    std::printf("| unbalanced tree | %.1f | %.1f | %.1f | %.2fx |\n", s, c, w, s / w);
  }
  {
    // Pure scheduling overhead: a million tasks that do nothing.
    constexpr int kTiny = 1000000;
    std::atomic<int> n{0};
    const double c = Best(reps, [&] {
      CentralGroup g(cp);
      for (int i = 0; i < kTiny; ++i) g.Spawn([&] { n.fetch_add(1, std::memory_order_relaxed); });
      g.Wait();
    });
    const double w = Best(reps, [&] {
      sp.Async([&] {
          stealpool::TaskGroup g(sp);
          for (int i = 0; i < kTiny; ++i) g.Spawn([&] { n.fetch_add(1, std::memory_order_relaxed); });
          g.Wait();
          return 0;
        }).Get();
    });
    std::printf("| 1M empty tasks, spawned in a loop | - | %.1f | %.1f | %.1fx vs central |\n", c, w, c / w);
    const double pf = Best(reps, [&] {
      stealpool::ParallelFor(sp, 0, kTiny, 1024, [&](size_t) { n.fetch_add(1, std::memory_order_relaxed); });
    });
    std::printf("| same 1M items via ParallelFor, grain 1024 | - | - | %.1f | %.0fx vs loop |\n", pf, w / pf);
  }
  {
    std::vector<double> v(1 << 24);
    for (size_t i = 0; i < v.size(); ++i) v[i] = double(i % 1000) * 0.001;
    auto chunk = [&](size_t lo, size_t hi) {
      double a = 0;
      for (size_t i = lo; i < hi; ++i) a += std::sqrt(v[i]) * std::sin(v[i]);
      return a;
    };
    const double s = Best(reps, [&] { g_sink = uint64_t(chunk(0, v.size())); });
    const double w = Best(reps, [&] {
      g_sink = uint64_t(stealpool::ParallelReduce(sp, 0, v.size(), 1 << 14, 0.0, chunk,
                                                  [](double a, double b) { return a + b; }));
    });
    std::printf("| reduce 16M doubles | %.1f | - | %.1f | %.2fx |\n", s, w, s / w);
  }

  auto st = sp.stats();
  std::printf("\nstealpool: %llu tasks run, %llu steals, %llu failed steal attempts, %llu parks\n",
              static_cast<unsigned long long>(st.tasks_run), static_cast<unsigned long long>(st.steals),
              static_cast<unsigned long long>(st.failed_steals), static_cast<unsigned long long>(st.parks));

  std::printf("\nscaling, fib(%d):\n| threads | ms | speedup |\n|---:|---:|---:|\n", fib_n);
  const double serial = Best(reps, [&] { g_sink = FibSerial(g_fib_n); });
  for (unsigned t = 1; t <= threads; t *= 2) {
    stealpool::Scheduler s(t);
    const double ms = Best(reps, [&] {
      g_sink = s.Async([&] { return Fib<stealpool::Scheduler, stealpool::TaskGroup>(s, fib_n); }).Get();
    });
    std::printf("| %u | %.1f | %.2fx |\n", t, ms, serial / ms);
    if (t * 2 > threads && t != threads) t = threads / 2;  // make sure the last row is `threads`
  }
  return 0;
}
