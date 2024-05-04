// Parallel quicksort: the classic fork-join example. Partition, sort one half
// in a spawned task and the other half right here.
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <random>
#include <vector>

#include "stealpool/scheduler.h"

template <typename It>
void QuickSort(stealpool::Scheduler& s, It lo, It hi) {
  if (hi - lo < 4096) {
    std::sort(lo, hi);
    return;
  }
  auto pivot = *(lo + (hi - lo) / 2);
  It mid1 = std::partition(lo, hi, [&](const auto& x) { return x < pivot; });
  It mid2 = std::partition(mid1, hi, [&](const auto& x) { return !(pivot < x); });
  stealpool::TaskGroup g(s);
  g.Spawn([&] { QuickSort(s, lo, mid1); });
  QuickSort(s, mid2, hi);
  g.Wait();
}

int main() {
  std::vector<int> v(20'000'000);
  std::mt19937 rng(1);
  for (auto& x : v) x = int(rng());
  auto copy = v;

  auto t0 = std::chrono::steady_clock::now();
  std::sort(copy.begin(), copy.end());
  auto t1 = std::chrono::steady_clock::now();

  stealpool::Scheduler s;
  s.Async([&] {
     QuickSort(s, v.begin(), v.end());
     return 0;
   }).Get();
  auto t2 = std::chrono::steady_clock::now();

  std::printf("std::sort: %.0f ms, parallel quicksort on %u threads: %.0f ms, same result: %s\n",
              std::chrono::duration<double, std::milli>(t1 - t0).count(), s.num_threads(),
              std::chrono::duration<double, std::milli>(t2 - t1).count(), v == copy ? "yes" : "NO");
  return v == copy ? 0 : 1;
}
