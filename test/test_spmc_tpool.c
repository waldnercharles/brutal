#include "pico_unit.h"

#define SPMC_TPOOL_STATIC
#define SPMC_TPOOL_IMPLEMENTATION
#include "spmc_tpool_box2d.h"

#include <stdatomic.h>
#include <unistd.h>

typedef struct
{
    atomic_int *counter;
    int value;
    useconds_t delay_us;
} spmc_task_arg;

static int spmc_add_task(void *arg_v)
{
    spmc_task_arg *arg = (spmc_task_arg *)arg_v;
    if (arg->delay_us) usleep(arg->delay_us);
    atomic_fetch_add(arg->counter, arg->value);
    return 0;
}

typedef struct
{
    atomic_int *slots;
} spmc_b2_ctx;

static void spmc_b2_range(int start_index, int end_index, void *task_context)
{
    spmc_b2_ctx *ctx = (spmc_b2_ctx *)task_context;
    for (int i = start_index; i < end_index; i++)
        atomic_fetch_add(&ctx->slots[i], 1);
}

TEST_CASE(test_spmc_handles_wait_for_each_task)
{
    tpool_t *tp = tpool_create(4);
    REQUIRE(tp != NULL);

    atomic_int counter = 0;
    enum
    {
        TASKS = 32
    };
    tpool_task_handle *handles[TASKS];
    spmc_task_arg args[TASKS];

    for (int i = 0; i < TASKS; i++) {
        args[i] = (spmc_task_arg){ .counter = &counter, .value = 1, .delay_us = 0 };
        handles[i] = tpool_add_work(tp, spmc_add_task, &args[i]);
        REQUIRE(handles[i] != NULL);
    }

    // Wait on one handle first to verify auto-kick behavior.
    tpool_wait_task(tp, handles[0]);
    for (int i = 1; i < TASKS; i++) tpool_wait_task(tp, handles[i]);

    REQUIRE(atomic_load(&counter) == TASKS);

    for (int i = 0; i < TASKS; i++) tpool_handle_destroy(handles[i]);
    tpool_destroy(tp);
    return true;
}

TEST_CASE(test_spmc_enqueue_wait_all_without_handles)
{
    tpool_t *tp = tpool_create(4);
    REQUIRE(tp != NULL);

    atomic_int counter = 0;
    enum
    {
        TASKS = 64
    };
    spmc_task_arg args[TASKS];

    for (int i = 0; i < TASKS; i++) {
        args[i] = (spmc_task_arg){ .counter = &counter, .value = 2, .delay_us = 0 };
        REQUIRE(tpool_enqueue(tp, spmc_add_task, &args[i]) == 0);
    }

    tpool_wait(tp);
    REQUIRE(atomic_load(&counter) == TASKS * 2);

    tpool_destroy(tp);
    return true;
}

TEST_CASE(test_spmc_destroy_drains_inflight_work)
{
    tpool_t *tp = tpool_create(2);
    REQUIRE(tp != NULL);

    atomic_int counter = 0;
    enum
    {
        TASKS = 8
    };
    spmc_task_arg args[TASKS];

    for (int i = 0; i < TASKS; i++) {
        args[i] = (spmc_task_arg){ .counter = &counter, .value = 1, .delay_us = 1000 };
        REQUIRE(tpool_enqueue(tp, spmc_add_task, &args[i]) == 0);
    }

    // Destroy should block until all queued tasks are complete.
    tpool_destroy(tp);
    REQUIRE(atomic_load(&counter) == TASKS);

    return true;
}

TEST_CASE(test_spmc_b2_enqueue_parallel_for_uses_single_handle)
{
    tpool_t *tp = tpool_create(4);
    REQUIRE(tp != NULL);
    tpool_b2_bridge bridge;
    tpool_b2_bridge_init(&bridge, tp);

    enum
    {
        ITEMS = 257
    };
    atomic_int slots[ITEMS];
    for (int i = 0; i < ITEMS; i++) atomic_init(&slots[i], 0);

    spmc_b2_ctx ctx = { .slots = slots };
    void *task = tpool_b2_enqueue_task_(spmc_b2_range, ITEMS, 17, &ctx, &bridge);
    REQUIRE(task != NULL);

    tpool_b2_finish_task_(task, &bridge);

    for (int i = 0; i < ITEMS; i++) REQUIRE(atomic_load(&slots[i]) == 1);

    tpool_b2_bridge_destroy(&bridge);
    tpool_destroy(tp);
    return true;
}

TEST_SUITE(spmc_tpool_suite)
{
    RUN_TEST_CASE(test_spmc_handles_wait_for_each_task);
    RUN_TEST_CASE(test_spmc_enqueue_wait_all_without_handles);
    RUN_TEST_CASE(test_spmc_destroy_drains_inflight_work);
    RUN_TEST_CASE(test_spmc_b2_enqueue_parallel_for_uses_single_handle);
}
