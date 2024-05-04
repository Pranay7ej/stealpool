#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "stealpool/deque.h"

using stealpool::WorkStealingDeque;
using Result = WorkStealingDeque<int>::StealResult;

TEST(Deque, OwnerIsLifoThievesAreFifo) {
  WorkStealingDeque<int> d(4);
  for (int i = 1; i <= 3; ++i) d.Push(i);
  EXPECT_EQ(d.Pop().value(), 3);
  int x = 0;
  ASSERT_EQ(d.Steal(&x), Result::kSuccess);
  EXPECT_EQ(x, 1);
  EXPECT_EQ(d.Pop().value(), 2);
  EXPECT_FALSE(d.Pop().has_value());
  EXPECT_EQ(d.Steal(&x), Result::kEmpty);
}

TEST(Deque, GrowsAndKeepsOrder) {
  WorkStealingDeque<int> d(2);
  for (int i = 0; i < 1000; ++i) d.Push(i);
  EXPECT_GE(d.capacity(), 1000u);
  EXPECT_EQ(d.SizeApprox(), 1000u);
  int x = 0;
  for (int i = 0; i < 500; ++i) {
    ASSERT_EQ(d.Steal(&x), Result::kSuccess);
    EXPECT_EQ(x, i);
  }
  for (int i = 999; i >= 500; --i) EXPECT_EQ(d.Pop().value(), i);
  EXPECT_FALSE(d.Pop().has_value());
}

TEST(Deque, WrapsAroundWithoutGrowing) {
  WorkStealingDeque<int> d(8);
  int x = 0;
  for (int round = 0; round < 1000; ++round) {
    for (int i = 0; i < 6; ++i) d.Push(round * 10 + i);
    ASSERT_EQ(d.Steal(&x), Result::kSuccess);
    EXPECT_EQ(x, round * 10);
    for (int i = 5; i >= 1; --i) EXPECT_EQ(d.Pop().value(), round * 10 + i);
  }
  EXPECT_EQ(d.capacity(), 8u);
}

// The real test: one owner pushing and popping, several thieves stealing,
// every item must come out exactly once.
TEST(Deque, EveryItemTakenExactlyOnceUnderContention) {
  constexpr int kItems = 200000;
  constexpr int kThieves = 3;
  WorkStealingDeque<int> d(16);  // small, so it grows while thieves are reading
  std::vector<std::atomic<int>> seen(kItems);
  for (auto& s : seen) s.store(0);
  std::atomic<bool> done{false};
  std::atomic<int> stolen{0};

  std::vector<std::thread> thieves;
  for (int t = 0; t < kThieves; ++t) {
    thieves.emplace_back([&] {
      int x;
      while (!done.load(std::memory_order_acquire) || !d.EmptyApprox()) {
        if (d.Steal(&x) == Result::kSuccess) {
          seen[size_t(x)].fetch_add(1);
          stolen.fetch_add(1);
        }
      }
    });
  }

  int popped = 0;
  for (int i = 0; i < kItems; ++i) {
    d.Push(i);
    if (i % 3 == 0) {  // pop sometimes, fighting the thieves for the last item
      if (auto x = d.Pop()) {
        seen[size_t(*x)].fetch_add(1);
        ++popped;
      }
    }
  }
  while (auto x = d.Pop()) {
    seen[size_t(*x)].fetch_add(1);
    ++popped;
  }
  done.store(true, std::memory_order_release);
  for (auto& t : thieves) t.join();

  int dup = 0, missing = 0;
  for (auto& s : seen) {
    if (s.load() == 0) ++missing;
    if (s.load() > 1) ++dup;
  }
  EXPECT_EQ(missing, 0);
  EXPECT_EQ(dup, 0);
  EXPECT_EQ(popped + stolen.load(), kItems);
  EXPECT_GT(stolen.load(), 0) << "thieves never got anything; test isn't testing much";
}
