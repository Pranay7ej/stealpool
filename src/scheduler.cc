#include "stealpool/scheduler.h"

#include <random>

namespace stealpool {

struct Scheduler::Worker {
  explicit Worker(unsigned i) : index(i), rng(0x9E3779B9u * (i + 1)) {}
  unsigned index;
  WorkStealingDeque<detail::Task*> deque;
  std::minstd_rand rng;
  // Written by the owning thread only; read (racily, relaxed) by stats().
  std::atomic<uint64_t> tasks_run{0}, steals{0}, failed_steals{0}, parks{0};
};

namespace {
thread_local Scheduler* tls_sched = nullptr;
thread_local unsigned tls_index = 0;

// Spin attempts before parking. Short: parking is cheap next to burning a
// core, but not so short that a worker sleeps between two bursts of spawns.
constexpr int kSpinRounds = 64;

void Bump(std::atomic<uint64_t>& c) { c.store(c.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed); }
}  // namespace

Scheduler::Scheduler(unsigned num_threads) {
  if (num_threads == 0) num_threads = std::max(1u, std::thread::hardware_concurrency());
  for (unsigned i = 0; i < num_threads; ++i) workers_.push_back(std::make_unique<Worker>(i));
  for (unsigned i = 0; i < num_threads; ++i) threads_.emplace_back([this, i] { WorkerLoop(i); });
}

Scheduler::~Scheduler() {
  {
    std::lock_guard<std::mutex> lk(park_mu_);
    stop_.store(true, std::memory_order_seq_cst);
    epoch_.fetch_add(1, std::memory_order_seq_cst);
  }
  park_cv_.notify_all();
  for (auto& t : threads_) t.join();
  // Workers drain everything before exiting, so nothing should be left.
  for (auto* t : injected_) delete t;
}

int Scheduler::CurrentWorker() const { return tls_sched == this ? int(tls_index) : -1; }

void Scheduler::Submit(detail::Task* t) {
  if (tls_sched == this) {
    workers_[tls_index]->deque.Push(t);
  } else {
    std::lock_guard<std::mutex> lk(inject_mu_);
    injected_.push_back(t);
    injected_size_.fetch_add(1, std::memory_order_seq_cst);
    injected_count_.fetch_add(1, std::memory_order_relaxed);
  }
  WakeOne();
}

void Scheduler::WakeOne() {
  // Pairs with the fence in WorkerLoop: either we see the sleeper, or the
  // sleeper's re-check sees our task. Never neither.
  std::atomic_thread_fence(std::memory_order_seq_cst);
  if (sleepers_.load(std::memory_order_relaxed) == 0) return;
  {
    std::lock_guard<std::mutex> lk(park_mu_);
    epoch_.fetch_add(1, std::memory_order_relaxed);
  }
  park_cv_.notify_one();
}

bool Scheduler::AnyWorkVisible() const {
  if (injected_size_.load(std::memory_order_seq_cst) > 0) return true;
  for (const auto& w : workers_)
    if (!w->deque.EmptyApprox()) return true;
  return false;
}

bool Scheduler::TrySteal(Worker* self, detail::Task** out) {
  const size_t n = workers_.size();
  if (n > 1) {
    // Start at a random victim so thieves don't all pile onto worker 0.
    const size_t start = self ? self->rng() % n : 0;
    for (size_t k = 0; k < n; ++k) {
      Worker* v = workers_[(start + k) % n].get();
      if (v == self) continue;
      for (;;) {
        auto r = v->deque.Steal(out);
        if (r == WorkStealingDeque<detail::Task*>::StealResult::kSuccess) {
          if (self) Bump(self->steals);
          return true;
        }
        if (r == WorkStealingDeque<detail::Task*>::StealResult::kEmpty) break;
        // Lost a race with another thief or the owner: there may be more, retry.
      }
    }
  }
  if (injected_size_.load(std::memory_order_relaxed) > 0) {
    std::lock_guard<std::mutex> lk(inject_mu_);
    if (!injected_.empty()) {
      *out = injected_.front();
      injected_.pop_front();
      injected_size_.fetch_sub(1, std::memory_order_relaxed);
      return true;
    }
  }
  if (self) Bump(self->failed_steals);
  return false;
}

detail::Task* Scheduler::FindWork(Worker* self) {
  if (self) {
    if (auto t = self->deque.Pop()) return *t;
  }
  detail::Task* t = nullptr;
  return TrySteal(self, &t) ? t : nullptr;
}

void Scheduler::Execute(Worker* self, detail::Task* t) {
  t->Run();
  delete t;
  if (self) Bump(self->tasks_run);
}

bool Scheduler::RunOneTask() {
  Worker* self = tls_sched == this ? workers_[tls_index].get() : nullptr;
  detail::Task* t = FindWork(self);
  if (!t) return false;
  Execute(self, t);
  return true;
}

void Scheduler::WorkerLoop(unsigned index) {
  tls_sched = this;
  tls_index = index;
  Worker* self = workers_[index].get();
  int idle = 0;
  for (;;) {
    if (detail::Task* t = FindWork(self)) {
      Execute(self, t);
      idle = 0;
      continue;
    }
    if (++idle < kSpinRounds) {
      std::this_thread::yield();
      continue;
    }
    // Park. Announce ourselves, then look one more time: a Submit that raced
    // with us either sees sleepers_ > 0 and bumps the epoch, or its task is
    // visible to this re-check.
    const uint64_t key = epoch_.load(std::memory_order_seq_cst);
    sleepers_.fetch_add(1, std::memory_order_seq_cst);
    std::atomic_thread_fence(std::memory_order_seq_cst);
    if (AnyWorkVisible()) {
      sleepers_.fetch_sub(1, std::memory_order_seq_cst);
      idle = 0;
      continue;
    }
    if (stop_.load(std::memory_order_seq_cst)) {
      sleepers_.fetch_sub(1, std::memory_order_seq_cst);
      break;  // nothing left anywhere, and we've been asked to exit
    }
    {
      std::unique_lock<std::mutex> lk(park_mu_);
      Bump(self->parks);
      park_cv_.wait(lk, [&] { return epoch_.load(std::memory_order_relaxed) != key; });
    }
    sleepers_.fetch_sub(1, std::memory_order_seq_cst);
    idle = 0;
  }
  // Tell the others too, in case they're parked with work gone.
  park_cv_.notify_all();
  tls_sched = nullptr;
}

SchedulerStats Scheduler::stats() const {
  SchedulerStats s;
  for (const auto& w : workers_) {
    s.tasks_run += w->tasks_run.load(std::memory_order_relaxed);
    s.steals += w->steals.load(std::memory_order_relaxed);
    s.failed_steals += w->failed_steals.load(std::memory_order_relaxed);
    s.parks += w->parks.load(std::memory_order_relaxed);
  }
  s.injected = injected_count_.load(std::memory_order_relaxed);
  return s;
}

// ------------------------------------------------------------------ TaskGroup

void TaskGroup::Done() {
  finishing_.fetch_add(1, std::memory_order_relaxed);
  if (pending_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    std::lock_guard<std::mutex> lk(mu_);
    cv_.notify_all();
  }
  finishing_.fetch_sub(1, std::memory_order_release);  // last touch of *this
}

void TaskGroup::WaitNoThrow() {
  auto all_done = [&] { return pending_.load(std::memory_order_acquire) == 0; };
  if (sched_.CurrentWorker() >= 0) {
    // On a worker: keep the pool moving by running tasks ourselves. This is
    // what makes recursive fork-join work without extra threads.
    while (!all_done()) {
      if (!sched_.RunOneTask()) std::this_thread::yield();
    }
  } else {
    std::unique_lock<std::mutex> lk(mu_);
    cv_.wait(lk, all_done);
  }
  while (finishing_.load(std::memory_order_acquire) != 0) std::this_thread::yield();
}

void TaskGroup::Wait() {
  WaitNoThrow();
  std::exception_ptr e;
  {
    std::lock_guard<std::mutex> lk(mu_);
    std::swap(e, error_);
  }
  if (e) std::rethrow_exception(e);
}

}  // namespace stealpool
