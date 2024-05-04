// Chase-Lev work-stealing deque.
//
// The owner thread pushes and pops at the bottom (LIFO, cache friendly),
// thieves steal from the top (FIFO, so they take the oldest and usually
// biggest piece of work). Only the last element is contended, and that race
// is settled with one CAS on `top`.
//
// Based on "Correct and Efficient Work-Stealing for Weak Memory Models"
// (Le, Pop, Cohen, Zappa Nardelli, PPoPP 2013), with the fences replaced by
// seq_cst operations on top/bottom. Same guarantees, and ThreadSanitizer
// understands it (it doesn't model standalone fences).
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <type_traits>
#include <vector>

namespace stealpool {

template <typename T>
class WorkStealingDeque {
  static_assert(std::is_trivially_copyable_v<T>, "elements are copied racily; keep them trivial (e.g. pointers)");

 public:
  explicit WorkStealingDeque(size_t initial_capacity = 256) {
    size_t cap = 1;
    while (cap < initial_capacity) cap <<= 1;
    auto a = std::make_unique<Array>(cap);
    array_.store(a.get(), std::memory_order_relaxed);
    arrays_.push_back(std::move(a));
  }
  WorkStealingDeque(const WorkStealingDeque&) = delete;
  WorkStealingDeque& operator=(const WorkStealingDeque&) = delete;

  // Owner only.
  void Push(T x) {
    const int64_t b = bottom_.load(std::memory_order_relaxed);
    const int64_t t = top_.load(std::memory_order_acquire);
    Array* a = array_.load(std::memory_order_relaxed);
    if (b - t > int64_t(a->capacity()) - 1) a = Grow(a, b, t);
    a->Put(b, x);
    // Release: a thief that sees the new bottom also sees the element (and
    // whatever the element points to).
    bottom_.store(b + 1, std::memory_order_release);
  }

  // Owner only.
  std::optional<T> Pop() {
    const int64_t b = bottom_.load(std::memory_order_relaxed) - 1;
    Array* a = array_.load(std::memory_order_relaxed);
    bottom_.store(b, std::memory_order_seq_cst);  // must be ordered before reading top
    int64_t t = top_.load(std::memory_order_seq_cst);
    if (t > b) {  // empty
      bottom_.store(b + 1, std::memory_order_relaxed);
      return std::nullopt;
    }
    T x = a->Get(b);
    if (t == b) {
      // Last element: race any thief for it.
      const bool won = top_.compare_exchange_strong(t, t + 1, std::memory_order_seq_cst, std::memory_order_relaxed);
      bottom_.store(b + 1, std::memory_order_relaxed);
      if (!won) return std::nullopt;
    }
    return x;
  }

  enum class StealResult { kSuccess, kEmpty, kLostRace };

  // Any thread.
  StealResult Steal(T* out) {
    int64_t t = top_.load(std::memory_order_seq_cst);
    const int64_t b = bottom_.load(std::memory_order_seq_cst);
    if (t >= b) return StealResult::kEmpty;
    Array* a = array_.load(std::memory_order_acquire);
    T x = a->Get(t);
    if (!top_.compare_exchange_strong(t, t + 1, std::memory_order_seq_cst, std::memory_order_relaxed))
      return StealResult::kLostRace;
    *out = x;
    return StealResult::kSuccess;
  }

  // Approximate; only exact when nobody else is touching the deque.
  size_t SizeApprox() const {
    const int64_t b = bottom_.load(std::memory_order_relaxed);
    const int64_t t = top_.load(std::memory_order_relaxed);
    return b > t ? size_t(b - t) : 0;
  }
  bool EmptyApprox() const { return SizeApprox() == 0; }
  size_t capacity() const { return array_.load(std::memory_order_relaxed)->capacity(); }

 private:
  class Array {
   public:
    explicit Array(size_t cap) : mask_(cap - 1), slots_(new std::atomic<T>[cap]) {}
    size_t capacity() const { return mask_ + 1; }
    // Slots are atomics so the owner overwriting a slot while a thief reads a
    // stale copy of it isn't UB; the CAS on top decides whose copy counts.
    void Put(int64_t i, T x) { slots_[size_t(i) & mask_].store(x, std::memory_order_relaxed); }
    T Get(int64_t i) const { return slots_[size_t(i) & mask_].load(std::memory_order_relaxed); }

   private:
    size_t mask_;
    std::unique_ptr<std::atomic<T>[]> slots_;
  };

  Array* Grow(Array* old, int64_t b, int64_t t) {
    auto bigger = std::make_unique<Array>(old->capacity() * 2);
    for (int64_t i = t; i < b; ++i) bigger->Put(i, old->Get(i));
    Array* raw = bigger.get();
    // Thieves may still be reading the old array, so it isn't freed until the
    // deque dies. Growth doubles, so this wastes at most as much as the live array.
    arrays_.push_back(std::move(bigger));
    array_.store(raw, std::memory_order_release);
    return raw;
  }

  alignas(64) std::atomic<int64_t> top_{0};
  alignas(64) std::atomic<int64_t> bottom_{0};  // separate cache lines: owner hammers bottom, thieves top
  alignas(64) std::atomic<Array*> array_{nullptr};
  std::vector<std::unique_ptr<Array>> arrays_;  // owner only
};

}  // namespace stealpool
