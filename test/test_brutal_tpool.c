#include "brutal_tpool.h"
#include "pico_unit.h"

#include <pthread.h>
#include <stdatomic.h>
#include <unistd.h>

static int add_one(void *arg)
{
    atomic_fetch_add((atomic_int *)arg, 1);
    return 0;
}

typedef struct
{
    atomic_int *counter;
    int value;
} task_arg;

static int add_value(void *arg)
{
    task_arg *a = (task_arg *)arg;
    atomic_fetch_add(a->counter, a->value);
    return 0;
}

typedef struct
{
    tpool_t *pool;
    atomic_int *counter;
    int count;
} submitter_arg;

static void *submitter_thread(void *arg)
{
    submitter_arg *a = (submitter_arg *)arg;
    for (int i = 0; i < a->count; i++) {
        tpool_enqueue(a->pool, add_one, a->counter);
    }
    return NULL;
}

static int sleep_and_add(void *arg)
{
    usleep(5000);
    atomic_fetch_add((atomic_int *)arg, 1);
    return 0;
}

TEST_CASE(test_pool_basic_submit_and_wait)
{
    tpool_t *tp = tpool_new(4, 0);

    atomic_int counter = 0;
    enum
    {
        TASKS = 64
    };

    for (int i = 0; i < TASKS; i++) tpool_enqueue(tp, add_one, &counter);

    tpool_wait(tp);
    REQUIRE(atomic_load(&counter) == TASKS);

    tpool_destroy(tp);
    return true;
}

TEST_CASE(test_pool_submit_null_fn)
{
    tpool_t *tp = tpool_new(2, 0);

    tpool_enqueue(tp, NULL, NULL);

    tpool_destroy(tp);
    return true;
}

TEST_CASE(test_pool_single_thread)
{
    tpool_t *tp = tpool_new(1, 0);

    atomic_int counter = 0;
    enum
    {
        TASKS = 128
    };

    for (int i = 0; i < TASKS; i++) tpool_enqueue(tp, add_one, &counter);

    tpool_wait(tp);
    REQUIRE(atomic_load(&counter) == TASKS);

    tpool_destroy(tp);
    return true;
}

TEST_CASE(test_pool_destroy_drains_work)
{
    tpool_t *tp = tpool_new(2, 0);

    atomic_int counter = 0;
    enum
    {
        TASKS = 32
    };

    for (int i = 0; i < TASKS; i++) tpool_enqueue(tp, add_one, &counter);

    tpool_destroy(tp);
    REQUIRE(atomic_load(&counter) == TASKS);

    return true;
}

TEST_CASE(test_pool_multiple_wait_cycles)
{
    tpool_t *tp = tpool_new(4, 0);
    atomic_int counter = 0;
    enum
    {
        TASKS = 32
    };

    for (int round = 0; round < 3; round++) {
        for (int i = 0; i < TASKS; i++) tpool_enqueue(tp, add_one, &counter);
        tpool_wait(tp);
    }

    REQUIRE(atomic_load(&counter) == TASKS * 3);

    tpool_destroy(tp);
    return true;
}

TEST_CASE(test_pool_wait_with_no_pending_work)
{
    tpool_t *tp = tpool_new(2, 0);

    tpool_wait(tp);

    tpool_destroy(tp);
    return true;
}

TEST_CASE(test_pool_values_carried_through_arg)
{
    tpool_t *tp = tpool_new(4, 0);
    atomic_int counter = 0;

    enum
    {
        TASKS = 16
    };
    task_arg args[TASKS];

    for (int i = 0; i < TASKS; i++) {
        args[i] = (task_arg){ .counter = &counter, .value = i + 1 };
        tpool_enqueue(tp, add_value, &args[i]);
    }

    tpool_wait(tp);

    // sum 1..16 = 136
    REQUIRE(atomic_load(&counter) == (TASKS * (TASKS + 1)) / 2);

    tpool_destroy(tp);
    return true;
}

TEST_CASE(test_pool_high_contention)
{
    tpool_t *tp = tpool_new(8, 0);
    atomic_int counter = 0;

    enum
    {
        TASKS = 4096
    };

    for (int i = 0; i < TASKS; i++) tpool_enqueue(tp, add_one, &counter);

    tpool_wait(tp);
    REQUIRE(atomic_load(&counter) == TASKS);

    tpool_destroy(tp);
    return true;
}

TEST_CASE(test_pool_concurrent_submitters)
{
    tpool_t *tp = tpool_new(4, 0);
    atomic_int counter = 0;

    enum
    {
        SUBMITTERS = 4,
        JOBS_PER = 512
    };
    pthread_t threads[SUBMITTERS];
    submitter_arg sargs[SUBMITTERS];

    for (int i = 0; i < SUBMITTERS; i++) {
        sargs[i] = (submitter_arg){ .pool = tp, .counter = &counter, .count = JOBS_PER };
        pthread_create(&threads[i], NULL, submitter_thread, &sargs[i]);
    }

    for (int i = 0; i < SUBMITTERS; i++) pthread_join(threads[i], NULL);

    tpool_wait(tp);
    REQUIRE(atomic_load(&counter) == SUBMITTERS * JOBS_PER);

    tpool_destroy(tp);
    return true;
}

TEST_CASE(test_pool_init_zero_threads_clamped)
{
    tpool_t *tp = tpool_new(0, 0);

    atomic_int counter = 0;
    tpool_enqueue(tp, add_one, &counter);
    tpool_wait(tp);
    REQUIRE(atomic_load(&counter) == 1);

    tpool_destroy(tp);
    return true;
}

TEST_CASE(test_pool_destroy_null_is_safe)
{
    tpool_destroy(NULL);
    return true;
}

TEST_CASE(test_pool_inline_execution_on_full_queue)
{
    // Tiny queue: 4 slots, 2 workers. Submit 32 jobs.
    // Many will overflow and run inline on the calling thread.
    enum
    {
        CAP = 4,
        TASKS = 32
    };
    tpool_t *tp = tpool_new(2, CAP);
    atomic_int counter = 0;

    for (int i = 0; i < TASKS; i++) tpool_enqueue(tp, add_one, &counter);

    tpool_wait(tp);
    REQUIRE(atomic_load(&counter) == TASKS);

    tpool_destroy(tp);
    return true;
}

TEST_CASE(test_pool_wait_steals_work)
{
    // 1 worker with a slow job blocking it. Submit fast jobs after.
    // tpool_wait must steal and run them to make progress.
    enum
    {
        CAP = 8
    };
    tpool_t *tp = tpool_new(1, CAP);
    atomic_int counter = 0;

    // Block the single worker with a slow job.
    tpool_enqueue(tp, sleep_and_add, &counter);

    // Submit fast jobs that the worker can't pick up while sleeping.
    enum
    {
        FAST = 4
    };
    for (int i = 0; i < FAST; i++) tpool_enqueue(tp, add_one, &counter);

    tpool_wait(tp);
    REQUIRE(atomic_load(&counter) == 1 + FAST);

    tpool_destroy(tp);
    return true;
}

TEST_SUITE(tpool_suite)
{
    RUN_TEST_CASE(test_pool_basic_submit_and_wait);
    RUN_TEST_CASE(test_pool_submit_null_fn);
    RUN_TEST_CASE(test_pool_single_thread);
    RUN_TEST_CASE(test_pool_destroy_drains_work);
    RUN_TEST_CASE(test_pool_multiple_wait_cycles);
    RUN_TEST_CASE(test_pool_wait_with_no_pending_work);
    RUN_TEST_CASE(test_pool_values_carried_through_arg);
    RUN_TEST_CASE(test_pool_high_contention);
    RUN_TEST_CASE(test_pool_concurrent_submitters);
    RUN_TEST_CASE(test_pool_init_zero_threads_clamped);
    RUN_TEST_CASE(test_pool_destroy_null_is_safe);
    RUN_TEST_CASE(test_pool_inline_execution_on_full_queue);
    RUN_TEST_CASE(test_pool_wait_steals_work);
}
