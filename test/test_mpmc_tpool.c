#include "pico_unit.h"

#include "mpmc_tpool.h"

#include <pthread.h>
#include <stdatomic.h>
#include <unistd.h>

// ---- helpers ----------------------------------------------------------------

static void mpmc_add_one(void *arg)
{
    atomic_fetch_add((atomic_int *)arg, 1);
}

typedef struct
{
    atomic_int *counter;
    int value;
} mpmc_task_arg;

static void mpmc_add_value(void *arg)
{
    mpmc_task_arg *a = (mpmc_task_arg *)arg;
    atomic_fetch_add(a->counter, a->value);
}

typedef struct
{
    tpool_queue_t *q;
    int count;
    atomic_int *produced;
} mpmc_producer_arg;

static void *mpmc_producer_thread(void *arg)
{
    mpmc_producer_arg *a = (mpmc_producer_arg *)arg;
    for (int i = 0; i < a->count; i++) {
        tpool_job_t job = { mpmc_add_one, NULL };
        while (!try_enqueue(a->q, &job)) tpool_relax();
        atomic_fetch_add(a->produced, 1);
    }
    return NULL;
}

typedef struct
{
    tpool_queue_t *q;
    atomic_bool *done;
    atomic_int *consumed;
} mpmc_consumer_arg;

static void *mpmc_consumer_thread(void *arg)
{
    mpmc_consumer_arg *a = (mpmc_consumer_arg *)arg;
    for (;;) {
        tpool_job_t out;
        if (try_dequeue(a->q, &out)) {
            atomic_fetch_add(a->consumed, 1);
            continue;
        }
        if (atomic_load(a->done)) {
            // Drain remaining items after producers finished.
            while (try_dequeue(a->q, &out)) atomic_fetch_add(a->consumed, 1);
            break;
        }
        tpool_relax();
    }
    return NULL;
}

typedef struct
{
    tpool_t *pool;
    atomic_int *counter;
    int count;
} mpmc_submitter_arg;

static void *mpmc_submitter_thread(void *arg)
{
    mpmc_submitter_arg *a = (mpmc_submitter_arg *)arg;
    for (int i = 0; i < a->count; i++) {
        while (!tpool_submit(a->pool, mpmc_add_one, a->counter)) tpool_relax();
    }
    return NULL;
}

static void mpmc_sleep_and_add(void *arg)
{
    usleep(5000);
    atomic_fetch_add((atomic_int *)arg, 1);
}

// ---- queue tests ------------------------------------------------------------

TEST_CASE(test_mpmc_queue_single_enqueue_dequeue)
{
    tpool_queue_t q;
    queue_init(&q, 0);

    tpool_job_t in = { mpmc_add_one, NULL };
    REQUIRE(try_enqueue(&q, &in));

    tpool_job_t out = { 0 };
    REQUIRE(try_dequeue(&q, &out));
    REQUIRE(out.fn == mpmc_add_one);
    REQUIRE(out.arg == NULL);

    free(q.slots);
    return true;
}

TEST_CASE(test_mpmc_queue_empty_dequeue_fails)
{
    tpool_queue_t q;
    queue_init(&q, 0);

    tpool_job_t out;
    REQUIRE(!try_dequeue(&q, &out));

    free(q.slots);
    return true;
}

TEST_CASE(test_mpmc_queue_fifo_order)
{
    tpool_queue_t q;
    queue_init(&q, 0);

    int tags[4];
    for (int i = 0; i < 4; i++) {
        tpool_job_t job = { mpmc_add_one, &tags[i] };
        REQUIRE(try_enqueue(&q, &job));
    }

    for (int i = 0; i < 4; i++) {
        tpool_job_t out;
        REQUIRE(try_dequeue(&q, &out));
        REQUIRE(out.arg == &tags[i]);
    }

    free(q.slots);
    return true;
}

TEST_CASE(test_mpmc_queue_full_returns_false)
{
    enum { CAP = 16 };
    tpool_queue_t q;
    queue_init(&q, CAP);

    tpool_job_t job = { mpmc_add_one, NULL };
    for (size_t i = 0; i < CAP; i++) REQUIRE(try_enqueue(&q, &job));

    REQUIRE(!try_enqueue(&q, &job));

    free(q.slots);
    return true;
}

TEST_CASE(test_mpmc_queue_reuse_after_drain)
{
    enum { CAP = 16 };
    tpool_queue_t q;
    queue_init(&q, CAP);

    tpool_job_t job = { mpmc_add_one, NULL };

    for (size_t i = 0; i < CAP; i++) REQUIRE(try_enqueue(&q, &job));
    for (size_t i = 0; i < CAP; i++) {
        tpool_job_t out;
        REQUIRE(try_dequeue(&q, &out));
    }

    // Should accept new items after draining.
    for (size_t i = 0; i < CAP; i++) REQUIRE(try_enqueue(&q, &job));

    free(q.slots);
    return true;
}

// ---- pool tests -------------------------------------------------------------

TEST_CASE(test_mpmc_pool_basic_submit_and_wait)
{
    tpool_t *tp = tpool_init(4, 0);

    atomic_int counter = 0;
    enum { TASKS = 64 };

    for (int i = 0; i < TASKS; i++) REQUIRE(tpool_submit(tp, mpmc_add_one, &counter));

    tpool_wait(tp);
    REQUIRE(atomic_load(&counter) == TASKS);

    tpool_destroy(tp);
    return true;
}

TEST_CASE(test_mpmc_pool_submit_null_fn_rejected)
{
    tpool_t *tp = tpool_init(2, 0);

    REQUIRE(!tpool_submit(tp, NULL, NULL));

    tpool_destroy(tp);
    return true;
}

TEST_CASE(test_mpmc_pool_single_thread)
{
    tpool_t *tp = tpool_init(1, 0);

    atomic_int counter = 0;
    enum { TASKS = 128 };

    for (int i = 0; i < TASKS; i++) REQUIRE(tpool_submit(tp, mpmc_add_one, &counter));

    tpool_wait(tp);
    REQUIRE(atomic_load(&counter) == TASKS);

    tpool_destroy(tp);
    return true;
}

TEST_CASE(test_mpmc_pool_destroy_drains_work)
{
    tpool_t *tp = tpool_init(2, 0);

    atomic_int counter = 0;
    enum { TASKS = 32 };

    for (int i = 0; i < TASKS; i++) REQUIRE(tpool_submit(tp, mpmc_add_one, &counter));

    tpool_destroy(tp);
    REQUIRE(atomic_load(&counter) == TASKS);

    return true;
}

TEST_CASE(test_mpmc_pool_multiple_wait_cycles)
{
    tpool_t *tp = tpool_init(4, 0);
    atomic_int counter = 0;
    enum { TASKS = 32 };

    for (int round = 0; round < 3; round++) {
        for (int i = 0; i < TASKS; i++) REQUIRE(tpool_submit(tp, mpmc_add_one, &counter));
        tpool_wait(tp);
    }

    REQUIRE(atomic_load(&counter) == TASKS * 3);

    tpool_destroy(tp);
    return true;
}

TEST_CASE(test_mpmc_pool_wait_with_no_pending_work)
{
    tpool_t *tp = tpool_init(2, 0);

    tpool_wait(tp);

    tpool_destroy(tp);
    return true;
}

TEST_CASE(test_mpmc_pool_values_carried_through_arg)
{
    tpool_t *tp = tpool_init(4, 0);
    atomic_int counter = 0;

    enum { TASKS = 16 };
    mpmc_task_arg args[TASKS];

    for (int i = 0; i < TASKS; i++) {
        args[i] = (mpmc_task_arg){ .counter = &counter, .value = i + 1 };
        REQUIRE(tpool_submit(tp, mpmc_add_value, &args[i]));
    }

    tpool_wait(tp);

    // sum 1..16 = 136
    REQUIRE(atomic_load(&counter) == (TASKS * (TASKS + 1)) / 2);

    tpool_destroy(tp);
    return true;
}

TEST_CASE(test_mpmc_pool_high_contention)
{
    tpool_t *tp = tpool_init(8, 0);
    atomic_int counter = 0;

    enum { TASKS = 4096 };

    for (int i = 0; i < TASKS; i++) REQUIRE(tpool_submit(tp, mpmc_add_one, &counter));

    tpool_wait(tp);
    REQUIRE(atomic_load(&counter) == TASKS);

    tpool_destroy(tp);
    return true;
}

TEST_CASE(test_mpmc_queue_concurrent_producers_consumers)
{
    tpool_queue_t q;
    queue_init(&q, 0);

    enum { PRODUCERS = 4, CONSUMERS = 4, ITEMS_PER = 2048 };
    atomic_int produced = 0;
    atomic_int consumed = 0;
    atomic_bool done = false;

    pthread_t prod[PRODUCERS], cons[CONSUMERS];
    mpmc_producer_arg pargs[PRODUCERS];
    mpmc_consumer_arg cargs[CONSUMERS];

    for (int i = 0; i < CONSUMERS; i++) {
        cargs[i] = (mpmc_consumer_arg){ .q = &q, .done = &done, .consumed = &consumed };
        pthread_create(&cons[i], NULL, mpmc_consumer_thread, &cargs[i]);
    }

    for (int i = 0; i < PRODUCERS; i++) {
        pargs[i] = (mpmc_producer_arg){ .q = &q, .count = ITEMS_PER, .produced = &produced };
        pthread_create(&prod[i], NULL, mpmc_producer_thread, &pargs[i]);
    }

    for (int i = 0; i < PRODUCERS; i++) pthread_join(prod[i], NULL);
    atomic_store(&done, true);
    for (int i = 0; i < CONSUMERS; i++) pthread_join(cons[i], NULL);

    REQUIRE(atomic_load(&produced) == PRODUCERS * ITEMS_PER);
    REQUIRE(atomic_load(&consumed) == PRODUCERS * ITEMS_PER);

    free(q.slots);
    return true;
}

TEST_CASE(test_mpmc_queue_multiple_wrap_around_laps)
{
    enum { CAP = 16, LAPS = 8 };
    tpool_queue_t q;
    queue_init(&q, CAP);

    tpool_job_t job = { mpmc_add_one, NULL };

    for (int lap = 0; lap < LAPS; lap++) {
        for (size_t i = 0; i < CAP; i++) REQUIRE(try_enqueue(&q, &job));
        REQUIRE(!try_enqueue(&q, &job));
        for (size_t i = 0; i < CAP; i++) {
            tpool_job_t out;
            REQUIRE(try_dequeue(&q, &out));
        }
        REQUIRE(!try_dequeue(&q, &job));
    }

    free(q.slots);
    return true;
}

TEST_CASE(test_mpmc_pool_concurrent_submitters)
{
    tpool_t *tp = tpool_init(4, 0);
    atomic_int counter = 0;

    enum { SUBMITTERS = 4, JOBS_PER = 512 };
    pthread_t threads[SUBMITTERS];
    mpmc_submitter_arg sargs[SUBMITTERS];

    for (int i = 0; i < SUBMITTERS; i++) {
        sargs[i] = (mpmc_submitter_arg){ .pool = tp, .counter = &counter, .count = JOBS_PER };
        pthread_create(&threads[i], NULL, mpmc_submitter_thread, &sargs[i]);
    }

    for (int i = 0; i < SUBMITTERS; i++) pthread_join(threads[i], NULL);

    tpool_wait(tp);
    REQUIRE(atomic_load(&counter) == SUBMITTERS * JOBS_PER);

    tpool_destroy(tp);
    return true;
}

TEST_CASE(test_mpmc_pool_init_zero_threads_clamped)
{
    tpool_t *tp = tpool_init(0, 0);
    REQUIRE(tp->nthreads == 1);

    atomic_int counter = 0;
    REQUIRE(tpool_submit(tp, mpmc_add_one, &counter));
    tpool_wait(tp);
    REQUIRE(atomic_load(&counter) == 1);

    tpool_destroy(tp);
    return true;
}

TEST_CASE(test_mpmc_pool_destroy_null_is_safe)
{
    tpool_destroy(NULL);
    return true;
}

TEST_CASE(test_mpmc_pool_inline_execution_on_full_queue)
{
    // Tiny queue: 4 slots, 2 workers. Submit 32 jobs.
    // Many will overflow and run inline on the calling thread.
    enum { CAP = 4, TASKS = 32 };
    tpool_t *tp = tpool_init(2, CAP);
    atomic_int counter = 0;

    for (int i = 0; i < TASKS; i++) REQUIRE(tpool_submit(tp, mpmc_add_one, &counter));

    tpool_wait(tp);
    REQUIRE(atomic_load(&counter) == TASKS);

    tpool_destroy(tp);
    return true;
}

TEST_CASE(test_mpmc_pool_wait_steals_work)
{
    // 1 worker with a slow job blocking it. Submit fast jobs after.
    // tpool_wait must steal and run them to make progress.
    enum { CAP = 8 };
    tpool_t *tp = tpool_init(1, CAP);
    atomic_int counter = 0;

    // Block the single worker with a slow job.
    REQUIRE(tpool_submit(tp, mpmc_sleep_and_add, &counter));

    // Submit fast jobs that the worker can't pick up while sleeping.
    enum { FAST = 4 };
    for (int i = 0; i < FAST; i++) REQUIRE(tpool_submit(tp, mpmc_add_one, &counter));

    tpool_wait(tp);
    REQUIRE(atomic_load(&counter) == 1 + FAST);

    tpool_destroy(tp);
    return true;
}

TEST_SUITE(mpmc_tpool_suite)
{
    RUN_TEST_CASE(test_mpmc_queue_single_enqueue_dequeue);
    RUN_TEST_CASE(test_mpmc_queue_empty_dequeue_fails);
    RUN_TEST_CASE(test_mpmc_queue_fifo_order);
    RUN_TEST_CASE(test_mpmc_queue_full_returns_false);
    RUN_TEST_CASE(test_mpmc_queue_reuse_after_drain);
    RUN_TEST_CASE(test_mpmc_pool_basic_submit_and_wait);
    RUN_TEST_CASE(test_mpmc_pool_submit_null_fn_rejected);
    RUN_TEST_CASE(test_mpmc_pool_single_thread);
    RUN_TEST_CASE(test_mpmc_pool_destroy_drains_work);
    RUN_TEST_CASE(test_mpmc_pool_multiple_wait_cycles);
    RUN_TEST_CASE(test_mpmc_pool_wait_with_no_pending_work);
    RUN_TEST_CASE(test_mpmc_pool_values_carried_through_arg);
    RUN_TEST_CASE(test_mpmc_pool_high_contention);
    RUN_TEST_CASE(test_mpmc_queue_concurrent_producers_consumers);
    RUN_TEST_CASE(test_mpmc_queue_multiple_wrap_around_laps);
    RUN_TEST_CASE(test_mpmc_pool_concurrent_submitters);
    RUN_TEST_CASE(test_mpmc_pool_init_zero_threads_clamped);
    RUN_TEST_CASE(test_mpmc_pool_destroy_null_is_safe);
    RUN_TEST_CASE(test_mpmc_pool_inline_execution_on_full_queue);
    RUN_TEST_CASE(test_mpmc_pool_wait_steals_work);
}
