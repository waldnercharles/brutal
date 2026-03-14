# arch_ecs.h Parallel Iteration Implementation Plan

**Goal:** Add `ecs_parallel_for` to arch_ecs.h — dispatch system functions across a pluggable thread pool with per-task kill lists.

**Architecture:** `ecs_task` struct carries SoA component pointers (named fields mirroring the archetype), entity IDs, and a world pointer. The `ecs_parallel_for` macro walks matching archetypes, splits entity ranges into tasks, enqueues via user-provided callbacks, waits, then flushes per-task kill lists on the main thread. Falls back to single-threaded when no callbacks are set.

**Tech Stack:** C23, arch_ecs.h (single-header ECS), brutal_tpool.h (lock-free MPMC thread pool for tests), `_Thread_local` for per-task kill routing.

---

### Task 1: Add thread config defines and ecs_task struct to arch_ecs.h

**Files:**

- Modify: `include/arch_ecs.h:42-47` (after ECS_MAX_ARCHETYPES define)
- Modify: `include/arch_ecs.h:153-166` (after archetype struct, before ecs_world)

**Step 1: Add defines after existing config defines (line 47)**

After the `ECS_MAX_ARCHETYPES` define block, add:

```c
#ifndef ECS_MT_MAX_TASKS
#define ECS_MT_MAX_TASKS 64
#endif
#ifndef ECS_MT_KILL_LIST_SIZE
#define ECS_MT_KILL_LIST_SIZE 256
#endif
```

**Step 2: Add callback typedefs and ecs_task struct**

After the `ecs_archetype` struct definition (line 151) and before `ecs_world`, add:

```c
/* --- thread pool callbacks ----------------------------------- */

typedef int (*ecs_enqueue_fn)(int (*fn)(void *), void *arg, void *udata);
typedef void (*ecs_wait_fn)(void *udata);

/* --- task (parallel iteration slice) ------------------------- */

typedef struct ecs_task ecs_task;
struct ecs_task
{
    void (*fn)(ecs_task *);
    ecs_world *w;
    int n;
    int *entity_ids;
    int task_index;
#define ECS_TASK_PTR_(T, f) T *f;
    ECS_COMPONENTS(ECS_TASK_PTR_)
#undef ECS_TASK_PTR_
};
```

Note: `ecs_task` references `ecs_world` which is defined after it. We need a forward declaration. Add `typedef struct ecs_world_ ecs_world;` before the task struct and change the world struct to `struct ecs_world_`.

Actually, the current `ecs_world` is an anonymous struct via `typedef struct { ... } ecs_world;`. To forward-declare it, change to:

```c
typedef struct ecs_world ecs_world;
```

before `ecs_task`, then define:

```c
struct ecs_world
{
    ...
};
```

**Step 3: Add threading fields to ecs_world**

Add these fields at the end of the `ecs_world` struct, after `deferred_dead`:

```c
    /* threading */
    ecs_enqueue_fn enqueue;
    ecs_wait_fn wait;
    void *task_udata;
    int max_tasks;
    int min_entities_per_task;
    ecs_task par_tasks[ECS_MT_MAX_TASKS];
    bool par_iterating;
    int par_kills[ECS_MT_MAX_TASKS][ECS_MT_KILL_LIST_SIZE];
    int par_kill_counts[ECS_MT_MAX_TASKS];
```

**Step 4: Run tests to verify nothing broke**

Run: `make test`
Expected: All 84 tests PASS. The new fields are zero-initialized, no behavior change.

**Step 5: Commit**

```
git add include/arch_ecs.h
git commit -m "Add ecs_task struct and threading fields to ecs_world"
```

---

### Task 2: Add ecs_set_threads, ecs_set_min_entities_per_task, ecs_kill_id

**Files:**

- Modify: `include/arch_ecs.h` (after `ecs_world_destroy`, before `#endif`)

**Step 1: Add setup functions and kill helper**

After the `ecs_world_destroy` function, add:

```c
/* --- thread setup --------------------------------------------- */

static inline void ecs_set_threads(ecs_world *w, ecs_enqueue_fn enqueue,
                                   ecs_wait_fn wait, void *udata,
                                   int max_tasks)
{
    w->enqueue = enqueue;
    w->wait = wait;
    w->task_udata = udata;
    w->max_tasks = max_tasks > ECS_MT_MAX_TASKS ? ECS_MT_MAX_TASKS : max_tasks;
}

static inline void ecs_set_min_entities_per_task(ecs_world *w, int min_count)
{
    w->min_entities_per_task = min_count;
}

/* --- kill by id (for parallel iteration) ---------------------- */

static inline void ecs_kill_id(ecs_world *w, int id)
{
    ecs_kill(w, (ecs_entity){ .id = id, .gen = w->entities[id].gen });
}
```

**Step 2: Run tests**

Run: `make test`
Expected: All 84 tests PASS.

**Step 3: Commit**

```
git add include/arch_ecs.h
git commit -m "Add ecs_set_threads, ecs_set_min_entities_per_task, ecs_kill_id"
```

---

### Task 3: Add par_iterating kill path to ecs_kill and par flush

**Files:**

- Modify: `include/arch_ecs.h:258-290` (ecs_kill function)
- Modify: `include/arch_ecs.h` (add ecs_par_flush_kills_ before ecs_kill)

**Step 1: Add ecs_par_flush_kills_ function**

Add before `ecs_kill` (after `ecs_flush_kills_`):

```c
/* --- parallel kill flush ------------------------------------- */

static inline void ecs_par_flush_kills_(ecs_world *w)
{
    for (int t = 0; t < w->max_tasks; t++) {
        for (int k = 0; k < w->par_kill_counts[t]; k++) {
            int id = w->par_kills[t][k];
            ecs_entity_info *info = &w->entities[id];
            ecs_archetype *arch = &w->archetypes[info->archetype];
            int row = info->row;
            int last = arch->count - 1;

            if (row != last) {
                int swapped_id = arch->entity_ids[last];
                arch->entity_ids[row] = swapped_id;
#define ECS_SWAP_(T, f)                                                        \
    if (arch->mask & ECS_MASK(f)) arch->f[row] = arch->f[last];
                ECS_COMPONENTS(ECS_SWAP_)
#undef ECS_SWAP_
                w->entities[swapped_id].row = row;
            }

            arch->count--;
            info->archetype = -1;
            w->free_list[w->free_count++] = id;
        }
        w->par_kill_counts[t] = 0;
    }
}
```

**Step 2: Add par_iterating branch to ecs_kill**

In `ecs_kill`, after the existing `if (w->iterating)` block and before the non-deferred kill path, add:

```c
    if (w->par_iterating) {
        info->gen++;
        int task = ecs_tls_task_index_;
        assert(w->par_kill_counts[task] < ECS_MT_KILL_LIST_SIZE);
        w->par_kills[task][w->par_kill_counts[task]++] = e.id;
        w->alive_count--;
        return;
    }
```

This requires `ecs_tls_task_index_` to exist. Add it as a `_Thread_local` variable near the top of the implementation section (after the `ECS_P_` define, before the enum):

```c
static _Thread_local int ecs_tls_task_index_;
```

**Step 3: Run tests**

Run: `make test`
Expected: All 84 tests PASS. The `par_iterating` branch is never hit in existing tests.

**Step 4: Commit**

```
git add include/arch_ecs.h
git commit -m "Add par_iterating kill path and ecs_par_flush_kills_"
```

---

### Task 4: Add trampoline and ecs_parallel_for macro

**Files:**

- Modify: `include/arch_ecs.h` (after ECS_NONE_OF, before cleanup section)

**Step 1: Add trampoline function**

After the `ECS_NONE_OF` macro and its `// clang-format on` comment, add:

```c
/* --- parallel iteration --------------------------------------- */

static int ecs_par_trampoline_(void *arg)
{
    ecs_task *t = arg;
    ecs_tls_task_index_ = t->task_index;
    t->fn(t);
    return 0;
}

static inline int ecs_compute_tasks_(ecs_world *w, int count)
{
    int min_per = w->min_entities_per_task > 0 ? w->min_entities_per_task : 64;
    int max = w->max_tasks > 0 ? w->max_tasks : 1;
    int tc = (count + min_per - 1) / min_per;
    if (tc < 1) tc = 1;
    if (tc > max) tc = max;
    return tc;
}
```

**Step 2: Add ecs_parallel_for macro**

```c
// clang-format off

#define ecs_parallel_for(world, sys_fn, ...) do {                                                  \
    ecs_world *ECS_P_(w) = (world);                                                                \
    uint64_t ECS_P_(mask) = ECS_TMASK_ALL_TYPES(__VA_ARGS__);                                      \
    ECS_P_(w)->par_iterating = true;                                                               \
    for (int ECS_P_(a) = 0; ECS_P_(a) < ECS_P_(w)->archetype_count; ECS_P_(a)++) {                  \
        ecs_archetype *ECS_P_(arch) = &ECS_P_(w)->archetypes[ECS_P_(a)];                            \
        if ((ECS_P_(arch)->mask & ECS_P_(mask)) != ECS_P_(mask)) continue;                          \
        if (ECS_P_(arch)->count == 0) continue;                                                    \
        int ECS_P_(tc) = ecs_compute_tasks_(ECS_P_(w), ECS_P_(arch)->count);                        \
        for (int ECS_P_(t) = 0; ECS_P_(t) < ECS_P_(tc); ECS_P_(t)++) {                              \
            int ECS_P_(start) = (ECS_P_(arch)->count * ECS_P_(t)) / ECS_P_(tc);                     \
            int ECS_P_(end) = (ECS_P_(arch)->count * (ECS_P_(t) + 1)) / ECS_P_(tc);                 \
            ecs_task *ECS_P_(task) = &ECS_P_(w)->par_tasks[ECS_P_(t)];                              \
            ECS_P_(task)->fn = (sys_fn);                                                           \
            ECS_P_(task)->w = ECS_P_(w);                                                           \
            ECS_P_(task)->n = ECS_P_(end) - ECS_P_(start);                                         \
            ECS_P_(task)->entity_ids = &ECS_P_(arch)->entity_ids[ECS_P_(start)];                    \
            ECS_P_(task)->task_index = ECS_P_(t);                                                  \
            ECS_PFILL_SET_(ECS_P_(arch), ECS_P_(start), ECS_P_(t))                                 \
            if (ECS_P_(w)->enqueue)                                                                \
                ECS_P_(w)->enqueue(ecs_par_trampoline_, ECS_P_(task), ECS_P_(w)->task_udata);       \
            else {                                                                                 \
                ecs_tls_task_index_ = ECS_P_(t);                                                   \
                (sys_fn)(ECS_P_(task));                                                             \
            }                                                                                      \
        }                                                                                          \
        if (ECS_P_(w)->enqueue) ECS_P_(w)->wait(ECS_P_(w)->task_udata);                             \
    }                                                                                              \
    ecs_par_flush_kills_(ECS_P_(w));                                                               \
    ECS_P_(w)->par_iterating = false;                                                              \
} while (0)

/* Fill task component pointers from archetype + start offset. */
#define ECS_PFILL_IMPL_(T, f)                                                                      \
    ECS_P_(w)->par_tasks[ECS_P_(tidx)].f =                                                        \
        (ECS_P_(parch)->mask & ECS_MASK(f)) ? &ECS_P_(parch)->f[ECS_P_(pstart)] : NULL;
#define ECS_PFILL_SET_(arch, start, tidx)                                                          \
    for (ecs_archetype *ECS_P_(parch) = (arch); ECS_P_(parch); ECS_P_(parch) = NULL)               \
    for (int ECS_P_(pstart) = (start), ECS_P_(tidx) = (tidx), ECS_P_(pdone) = 0;                   \
         !ECS_P_(pdone); ECS_P_(pdone) = 1)                                                       \
    { ECS_COMPONENTS(ECS_PFILL_IMPL_) }

// clang-format on
```

Note: `ECS_PFILL_IMPL_` must stay `#define`'d (not `#undef`'d) because it is expanded at each `ecs_parallel_for` call site via `ECS_COMPONENTS(ECS_PFILL_IMPL_)`.

**Step 3: Run tests**

Run: `make test`
Expected: All 84 tests PASS. No test calls `ecs_parallel_for` yet.

**Step 4: Commit**

```
git add include/arch_ecs.h
git commit -m "Add ecs_parallel_for macro with trampoline and single-threaded fallback"
```

---

### Task 5: Write test for single-threaded fallback

**Files:**

- Modify: `test/test_arch_ecs.c` (add test before suite entry point)

**Step 1: Add test**

Before the `arch_ecs_suite` function, add:

```c
/* ---- ecs_parallel_for (single-threaded fallback) ---- */

static void par_move(ecs_task *t)
{
    for (int i = 0; i < t->n; i++)
        t->position[i].x += t->velocity[i].vx;
}

TEST_CASE(test_arch_parallel_for_fallback)
{
    ecs_world world = {0};
    ecs_entity e1 = ecs_spawn(&world, Position, Velocity);
    ecs_set(&world, e1, (Position){ 0.f, 0.f });
    ecs_set(&world, e1, (Velocity){ 1.f, 0.f });
    ecs_entity e2 = ecs_spawn(&world, Position);
    ecs_set(&world, e2, (Position){ 5.f, 5.f });

    /* no thread callbacks set — runs single-threaded */
    ecs_parallel_for(&world, par_move, Position, Velocity);

    REQUIRE(ecs_get_position(&world, e1)->x == 1.f);
    REQUIRE(ecs_get_position(&world, e2)->x == 5.f);

    ecs_world_destroy(&world);
    return true;
}
```

**Step 2: Register in suite**

Add to `arch_ecs_suite()`:

```c
    /* parallel_for */
    RUN_TEST_CASE(test_arch_parallel_for_fallback);
```

**Step 3: Run tests**

Run: `make test`
Expected: All 85 tests PASS (84 existing + 1 new).

**Step 4: Commit**

```
git add test/test_arch_ecs.c
git commit -m "Add test for ecs_parallel_for single-threaded fallback"
```

---

### Task 6: Write test for parallel_for kill (single-threaded fallback)

**Files:**

- Modify: `test/test_arch_ecs.c`

**Step 1: Add test**

```c
static void par_cleanup(ecs_task *t)
{
    for (int i = 0; i < t->n; i++)
        if (t->health[i].hp <= 0)
            ecs_kill_id(t->w, t->entity_ids[i]);
}

TEST_CASE(test_arch_parallel_for_kill_fallback)
{
    ecs_world world = {0};
    ecs_entity e1 = ecs_spawn(&world, Health);
    ecs_set(&world, e1, (Health){ 0 });
    ecs_entity e2 = ecs_spawn(&world, Health);
    ecs_set(&world, e2, (Health){ 100 });
    ecs_entity e3 = ecs_spawn(&world, Health);
    ecs_set(&world, e3, (Health){ 0 });

    ecs_parallel_for(&world, par_cleanup, Health);

    REQUIRE(world.alive_count == 1);
    REQUIRE(!ecs_alive(&world, e1));
    REQUIRE(ecs_alive(&world, e2));
    REQUIRE(!ecs_alive(&world, e3));

    ecs_world_destroy(&world);
    return true;
}
```

**Step 2: Register in suite**

```c
    RUN_TEST_CASE(test_arch_parallel_for_kill_fallback);
```

**Step 3: Run tests**

Run: `make test`
Expected: All 86 tests PASS.

**Step 4: Commit**

```
git add test/test_arch_ecs.c
git commit -m "Add test for ecs_parallel_for kill with single-threaded fallback"
```

---

### Task 7: Write test for multi-threaded ecs_parallel_for with tpool

**Files:**

- Modify: `test/test_arch_ecs.c`

The `arch_ecs_test_O2` target doesn't link brutal_tpool, and the main `test` target does include test_arch_ecs.c but doesn't compile brutal_tpool implementation. We need to include tpool in test_arch_ecs.c directly.

**Step 1: Add tpool include and adapter callbacks at top of test file**

After the existing `#include "pico_unit.h"`, add:

```c
#define BRUTAL_TPOOL_IMPLEMENTATION
#include "brutal_tpool.h"

/* Thread pool adapter callbacks for ecs_parallel_for */
static tpool_t *test_pool_;

static int test_enqueue_(int (*fn)(void *), void *arg, void *udata)
{
    tpool_enqueue(udata, fn, arg);
    return 0;
}

static void test_wait_(void *udata)
{
    tpool_wait(udata);
}
```

**Step 2: Add multi-threaded parallel_for test**

```c
TEST_CASE(test_arch_parallel_for_threaded)
{
    ecs_world world = {0};
    test_pool_ = tpool_new(4, 256);

    /* spawn enough entities to be split across tasks */
    for (int i = 0; i < 200; i++) {
        ecs_entity e = ecs_spawn(&world, Position, Velocity);
        ecs_set(&world, e, (Position){ (float)i, 0.f });
        ecs_set(&world, e, (Velocity){ 1.f, 0.f });
    }

    ecs_set_threads(&world, test_enqueue_, test_wait_, test_pool_, 4);
    ecs_set_min_entities_per_task(&world, 32);

    ecs_parallel_for(&world, par_move, Position, Velocity);

    /* verify all entities moved */
    for (int i = 0; i < 200; i++) {
        ecs_entity e = { .id = i + 1, .gen = 1 };
        REQUIRE(ecs_get_position(&world, e)->x == (float)i + 1.f);
    }

    tpool_destroy(test_pool_);
    ecs_world_destroy(&world);
    return true;
}
```

**Step 3: Add multi-threaded kill test**

```c
TEST_CASE(test_arch_parallel_for_kill_threaded)
{
    ecs_world world = {0};
    test_pool_ = tpool_new(4, 256);

    for (int i = 0; i < 200; i++) {
        ecs_entity e = ecs_spawn(&world, Health);
        ecs_set(&world, e, (Health){ (i % 2 == 0) ? 0 : 100 });
    }

    ecs_set_threads(&world, test_enqueue_, test_wait_, test_pool_, 4);
    ecs_set_min_entities_per_task(&world, 32);

    ecs_parallel_for(&world, par_cleanup, Health);

    REQUIRE(world.alive_count == 100);

    tpool_destroy(test_pool_);
    ecs_world_destroy(&world);
    return true;
}
```

**Step 4: Register in suite**

```c
    RUN_TEST_CASE(test_arch_parallel_for_threaded);
    RUN_TEST_CASE(test_arch_parallel_for_kill_threaded);
```

**Step 5: Run tests**

Run: `make test`
Expected: All 88 tests PASS.

If there are linker errors about pthread, the arch_ecs_test_O2 CMake target may need `-lpthread`. Check the CMakeLists.txt and add if needed:

```cmake
target_link_libraries(arch_ecs_test_O2 PRIVATE pthread)
```

**Step 6: Commit**

```
git add test/test_arch_ecs.c test/CMakeLists.txt
git commit -m "Add multi-threaded ecs_parallel_for tests with tpool"
```

---

### Task 8: Update header doc comment

**Files:**

- Modify: `include/arch_ecs.h:1-35` (header doc comment)

**Step 1: Add parallel iteration to doc comment**

Add after the `ECS_NONE_OF` example in the header comment:

```c
    PARALLEL ITERATION
        void move(ecs_task *t) {
            for (int i = 0; i < t->n; i++)
                t->position[i].x += t->velocity[i].vx;
        }
        ecs_set_threads(&world, enqueue, wait, pool, 4);
        ecs_parallel_for(&world, move, Position, Velocity);
```

Add to HANDLES section:

```
    Kill-during-parallel-iteration uses per-task kill lists (no atomics).
```

**Step 2: Run tests one final time**

Run: `make test`
Expected: All tests PASS.

**Step 3: Commit**

```
git add include/arch_ecs.h
git commit -m "Update arch_ecs.h header docs with parallel iteration examples"
```
