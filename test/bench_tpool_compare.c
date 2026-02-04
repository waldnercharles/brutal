#define CACHE_SIZE 128

#define PICO_BENCH_IMPLEMENTATION
#include "pico_bench.h"

// Pull in legacy tpool.h under a renamed symbol namespace.
#define tpool_s legacy_tpool_s
#define tpool_t legacy_tpool_t
#define tpool_task_fn legacy_tpool_task_fn
#define tpool_queue_is_full legacy_tpool_queue_is_full
#define tpool_queue_is_empty legacy_tpool_queue_is_empty
#define tpool_work_get legacy_tpool_work_get
#define tpool_worker legacy_tpool_worker
#define tpool_create legacy_tpool_create
#define tpool_destroy legacy_tpool_destroy
#define tpool_add_work legacy_tpool_add_work
#define tpool_wait legacy_tpool_wait
#define TPOOL_IMPLEMENTATION
#include "tpool.h"
#undef tpool_s
#undef tpool_t
#undef tpool_task_fn
#undef tpool_queue_is_full
#undef tpool_queue_is_empty
#undef tpool_work_get
#undef tpool_worker
#undef tpool_create
#undef tpool_destroy
#undef tpool_add_work
#undef tpool_wait
#undef TPOOL_IMPLEMENTATION

#define SPMC_TPOOL_STATIC
#define SPMC_TPOOL_IMPLEMENTATION
#include "spmc_tpool.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct
{
    int work;
    uint32_t seed;
    uint64_t *out;
} bench_task_arg;

typedef struct
{
    int num_threads;
    int num_tasks;
    int work;

    bench_task_arg *args;
    uint64_t *out;
    uint64_t expected_sum;

    legacy_tpool_t *legacy;
    tpool_t *spmc;
} tpool_bench_ctx;

static uint64_t bench_compute_(uint32_t seed, int work)
{
    uint64_t x = seed;
    for (int i = 0; i < work; i++) {
        x = x * 1664525u + 1013904223u;
        x ^= x >> 13;
    }
    return x;
}

static int bench_task(void *arg_v)
{
    bench_task_arg *arg = (bench_task_arg *)arg_v;
    *arg->out = bench_compute_(arg->seed, arg->work);
    return 0;
}

static uint64_t bench_sum_(uint64_t *arr, int count)
{
    uint64_t sum = 0;
    for (int i = 0; i < count; i++) sum += arr[i];
    return sum;
}

BENCH_SETUP(setup_legacy)
{
    tpool_bench_ctx *ctx = (tpool_bench_ctx *)pb_run_ctx->udata;
    memset(ctx->out, 0, (size_t)ctx->num_tasks * sizeof(*ctx->out));
    ctx->legacy = legacy_tpool_create((size_t)ctx->num_threads);
    BENCH_REQUIRE(ctx->legacy != NULL);
}

BENCH_TEARDOWN(teardown_legacy)
{
    tpool_bench_ctx *ctx = (tpool_bench_ctx *)pb_run_ctx->udata;
    legacy_tpool_destroy(ctx->legacy);
    ctx->legacy = NULL;
}

BENCH_CASE(bench_tpool)
{
    tpool_bench_ctx *ctx = (tpool_bench_ctx *)pb_run_ctx->udata;
    for (int i = 0; i < ctx->num_tasks; i++) {
        BENCH_REQUIRE(legacy_tpool_add_work(ctx->legacy, bench_task, &ctx->args[i]) == 0);
    }

    legacy_tpool_wait(ctx->legacy);
    BENCH_REQUIRE(bench_sum_(ctx->out, ctx->num_tasks) == ctx->expected_sum);
}

BENCH_SETUP(setup_spmc)
{
    tpool_bench_ctx *ctx = (tpool_bench_ctx *)pb_run_ctx->udata;
    memset(ctx->out, 0, (size_t)ctx->num_tasks * sizeof(*ctx->out));
    ctx->spmc = tpool_create((size_t)ctx->num_threads);
    BENCH_REQUIRE(ctx->spmc != NULL);
}

BENCH_TEARDOWN(teardown_spmc)
{
    tpool_bench_ctx *ctx = (tpool_bench_ctx *)pb_run_ctx->udata;
    tpool_destroy(ctx->spmc);
    ctx->spmc = NULL;
}

BENCH_CASE(bench_lock_free_tpool)
{
    tpool_bench_ctx *ctx = (tpool_bench_ctx *)pb_run_ctx->udata;
    for (int i = 0; i < ctx->num_tasks; i++) {
        BENCH_REQUIRE(tpool_enqueue(ctx->spmc, bench_task, &ctx->args[i]) == 0);
    }

    tpool_wait(ctx->spmc);
    BENCH_REQUIRE(bench_sum_(ctx->out, ctx->num_tasks) == ctx->expected_sum);
}

BENCH_SUITE(tpool_compare_suite)
{
    tpool_bench_ctx *ctx = (tpool_bench_ctx *)pb_suite_ctx;

    RUN_BENCH_CASE(bench_tpool, setup_legacy, teardown_legacy, ctx);
    RUN_BENCH_CASE(bench_lock_free_tpool, setup_spmc, teardown_spmc, ctx);
}

int main()
{
    tpool_bench_ctx ctx = { 0 };
    ctx.num_threads = 14;
    ctx.num_tasks = 8192;
    ctx.work = 64;

    ctx.args = (bench_task_arg *)calloc((size_t)ctx.num_tasks, sizeof(*ctx.args));
    ctx.out = (uint64_t *)calloc((size_t)ctx.num_tasks, sizeof(*ctx.out));
    if (!ctx.args || !ctx.out) return 1;

    for (int i = 0; i < ctx.num_tasks; i++) {
        ctx.args[i].work = ctx.work;
        ctx.args[i].seed = (uint32_t)(i + 1);
        ctx.args[i].out = &ctx.out[i];
        ctx.expected_sum += bench_compute_(ctx.args[i].seed, ctx.args[i].work);
    }

    pb_display_colors(true);
    pb_display_cpu_time(true);
    pb_set_warmup(3);
    pb_set_iterations(64);

    RUN_BENCH_SUITE(tpool_compare_suite, &ctx);
    pb_print_stats();

    free(ctx.out);
    free(ctx.args);
    return pb_failed() ? 1 : 0;
}
