# stealpool

Work-stealing task scheduler in C++17. Same idea as the schedulers inside TBB, Cilk, Rust's rayon or Go's runtime: every worker has its own deque, pushes and pops its own work at one end, and when it runs out it steals from the other end of someone else's.

Mainly wanted to get the lock-free deque and the sleep/wake logic right myself, since that's where all the subtle bugs live.

```cpp
stealpool::Scheduler sched;            // one worker per core

stealpool::TaskGroup g(sched);
g.Spawn([] { work_a(); });
g.Spawn([] { work_b(); });
g.Wait();                              // rethrows the first exception, if any

stealpool::ParallelFor(sched, 0, n, 1024, [&](size_t i) { out[i] = f(in[i]); });

auto fut = sched.Async([] { return 42; });
int x = fut.Get();
```

## how it works

**the deque** (`include/stealpool/deque.h`) is a Chase-Lev deque, following the C11 version from Lê et al. (PPoPP 2013):
- owner pushes/pops at the bottom (LIFO, so it keeps working on hot data), thieves steal from the top (FIFO, so they grab the oldest and usually biggest piece of work)
- the only contended case is when owner and thief both go for the last element. that's decided with one CAS on `top`
- it grows by doubling. old arrays aren't freed until the deque dies, because a thief might still be reading one
- the paper uses standalone fences. I used seq_cst loads/stores on top/bottom instead, which gives the same ordering but ThreadSanitizer actually understands it
- top and bottom are on separate cache lines since the owner hammers one and thieves the other

**workers** pop from their own deque, then try stealing from a random victim, then check a mutex-protected queue for tasks submitted from outside the pool. after 64 failed rounds they park.

**parking** was the trickiest part. a worker can decide to sleep at the same moment someone pushes a task, and if you get it wrong the task just sits there. it uses an eventcount: the worker reads an epoch, announces it's sleeping, checks every queue one last time, and only sleeps if the epoch hasn't changed. a submitter pushes the task, then wakes someone if anyone is sleeping. a seq_cst fence on both sides guarantees that either the submitter sees the sleeper or the sleeper sees the task.

**waiting** on a worker thread doesn't block, it runs other tasks until the group is done. that's what makes recursive fork-join (like fib or quicksort) work without deadlocking, even with 1 thread.

**TaskGroup** has one detail that's easy to get wrong: after the last task decrements the counter, it still has to touch the group to notify the waiter. if `Wait()` returns right after seeing 0, the owner can destroy the group while that task is still inside it. there's a second counter for tasks still inside `Done()`, and `Wait()` waits for both. there's a test that creates and destroys groups 2000 times to catch it.

## build

```
cmake -S . -B build
cmake --build build -j
ctest --test-dir build
./build/stealpool_bench
./build/quicksort
```

CI runs the tests plain, with ASan and with TSan, and repeats the whole suite 20 times since these bugs are timing dependent.

## tests

- deque: LIFO/FIFO order, growing, wrap-around, and a stress test with one owner pushing/popping and 3 thieves stealing where every one of 200k items has to come out exactly once
- scheduler: 100k tasks each run exactly once, nested fork-join, nested waits with a single thread, ParallelFor coverage for different sizes/grains, ParallelReduce, exceptions, Async, 6 outside threads submitting at once, workers actually parking when idle and waking for new work, destructor finishing queued work

## numbers

`stealpool_bench` compares against running serially and against the usual first-attempt thread pool (one `std::deque` behind a mutex). best of 3 on a 2 core VM:

| workload | serial ms | central queue ms | stealpool ms | speedup vs serial |
|---|---:|---:|---:|---:|
| fib(36), cutoff 20 | 31.3 | 17.1 | 16.6 | 1.89x |
| unbalanced tree | 106.5 | 59.4 | 53.7 | 1.98x |
| 1M empty tasks, spawned in a loop | - | 317.5 | 334.3 | 0.9x vs central |
| same 1M items via ParallelFor, grain 1024 | - | - | 16.0 | 21x vs loop |
| reduce 16M doubles | 154.9 | - | 62.4 | 2.48x |

(the 2.48x on reduce is above 2 on 2 cores, which is probably noise on a shared VM.)

what I learned from this:
- on 2 cores the central queue is honestly not much worse. the lock only really hurts with more cores fighting over it
- spawning a million tiny tasks in a flat loop is the worst case for work stealing: one worker pushes, the other steals almost every single task, so you pay a CAS plus a cross-thread free per task. ParallelFor splits the range in halves instead, so there are only a few steals and it's 21x faster
- per-task cost is dominated by `new`/`delete` of the task object. a per-worker free list would be the next thing to do

## not done
- no task priorities or affinity
- no per-worker allocator for tasks
- `Scheduler::Spawn` tasks must not throw (use TaskGroup if they might)
- stealing picks a random victim, no smarter heuristics
