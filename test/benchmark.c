#include "brutal_bench.h"
#include "brutal_ecs.h"
#include "brutal_tpool.h"

#define MAX_ENTITIES (1024 * 1024)

typedef struct
{
    int num_threads;
    int use_tpool;
} bench_ctx;

static ecs_t *ecs = NULL;
static tpool_t *tpool = NULL;

// System types
static ecs_sys_t MovementSystem;
static ecs_sys_t ComflabSystem;
static ecs_sys_t BoundsSystem;
static ecs_sys_t QueueDestroySystem;

// Component types
static ecs_comp_t PosComponent;
static ecs_comp_t DirComponent;
static ecs_comp_t RectComponent;
static ecs_comp_t ComflabComponent;

typedef struct
{
    float x, y;
} v2d_t;

typedef struct
{
    int x, y, w, h;
} rect_t;

typedef struct
{
    float thingy;
    bool mingy;
    int dingy;
} comflab_t;

static int bench_enqueue_cb(int (*fn)(void *args), void *fn_args, void *udata)
{
    (void)udata;
    tpool_enqueue(tpool, fn, fn_args);
    return 0;
}

static void bench_wait_cb(void *udata)
{
    (void)udata;
    tpool_wait(tpool);
}

int movement_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;

    for (int i = 0; i < view->count; i++) {
        ecs_entity entity = view->entities[i];

        v2d_t *pos = ecs_get(ecs, entity, PosComponent);
        v2d_t *dir = ecs_get(ecs, entity, DirComponent);

        pos->x += pos->x + dir->x * 1.f / 60.f;
        pos->y += pos->y + dir->y * 1.f / 60.f;
    }

    return 0;
}

int comflab_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;

    for (int i = 0; i < view->count; i++) {
        ecs_entity entity = view->entities[i];

        comflab_t *comflab = ecs_get(ecs, entity, ComflabComponent);
        comflab->thingy *= 1.000001f;
        comflab->mingy = !comflab->mingy;
        comflab->dingy++;
    }

    return 0;
}

int bounds_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;

    for (int i = 0; i < view->count; i++) {
        ecs_entity entity = view->entities[i];

        rect_t *bounds = ecs_get(ecs, entity, RectComponent);

        bounds->x = 1;
        bounds->y = 1;
        bounds->w = 1;
        bounds->h = 1;
    }

    return 0;
}

int queue_destroy_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;

    for (int i = 0; i < view->count; i++) {
        ecs_destroy(ecs, view->entities[i]);
    }

    return 0;
}

// -----------------------------------------------------------------------------
//  Setup

BENCH_SETUP(setup)
{
    bench_ctx *ctx = bench_run_ctx->udata;

    ecs = ecs_new();
    if (ctx->use_tpool && ctx->num_threads > 1) {
        tpool = tpool_new(ctx->num_threads, 0);
        ecs_set_task_callbacks(ecs, bench_enqueue_cb, bench_wait_cb, NULL, ctx->num_threads);
    }

    PosComponent = ECS_COMPONENT(ecs, v2d_t);
    RectComponent = ECS_COMPONENT(ecs, rect_t);
}

BENCH_SETUP(setup_destroy_with_two_components)
{
    bench_ctx *ctx = bench_run_ctx->udata;

    ecs = ecs_new();
    if (ctx->use_tpool && ctx->num_threads > 1) {
        tpool = tpool_new(ctx->num_threads, 0);
        ecs_set_task_callbacks(ecs, bench_enqueue_cb, bench_wait_cb, NULL, ctx->num_threads);
    }

    PosComponent = ECS_COMPONENT(ecs, v2d_t);
    RectComponent = ECS_COMPONENT(ecs, rect_t);

    for (size_t i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity entity = ecs_create(ecs);
        ecs_add(ecs, entity, PosComponent);
        ecs_add(ecs, entity, RectComponent);
    }
}

BENCH_SETUP(setup_get)
{
    bench_ctx *ctx = bench_run_ctx->udata;

    ecs = ecs_new();
    if (ctx->use_tpool && ctx->num_threads > 1) {
        tpool = tpool_new(ctx->num_threads, 0);
        ecs_set_task_callbacks(ecs, bench_enqueue_cb, bench_wait_cb, NULL, ctx->num_threads);
    }

    PosComponent = ECS_COMPONENT(ecs, v2d_t);

    for (size_t i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity entity = ecs_create(ecs);
        ecs_add(ecs, entity, PosComponent);
    }
}

BENCH_SETUP(setup_three_systems)
{
    bench_ctx *ctx = bench_run_ctx->udata;

    ecs = ecs_new();
    if (ctx->use_tpool && ctx->num_threads > 1) {
        tpool = tpool_new(ctx->num_threads, 0);
        ecs_set_task_callbacks(ecs, bench_enqueue_cb, bench_wait_cb, NULL, ctx->num_threads);
    }

    PosComponent = ECS_COMPONENT(ecs, v2d_t);
    DirComponent = ECS_COMPONENT(ecs, v2d_t);
    ComflabComponent = ECS_COMPONENT(ecs, comflab_t);
    RectComponent = ECS_COMPONENT(ecs, rect_t);

    MovementSystem = ecs_sys_create(ecs, movement_system, NULL);
    ecs_sys_require(ecs, MovementSystem, PosComponent);
    ecs_sys_require(ecs, MovementSystem, DirComponent);

    ComflabSystem = ecs_sys_create(ecs, comflab_system, NULL);
    ecs_sys_require(ecs, ComflabSystem, ComflabComponent);

    BoundsSystem = ecs_sys_create(ecs, bounds_system, NULL);
    ecs_sys_require(ecs, BoundsSystem, RectComponent);

    for (size_t i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity entity = ecs_create(ecs);

        v2d_t *pos = ecs_add(ecs, entity, PosComponent);
        v2d_t *dir = ecs_add(ecs, entity, DirComponent);
        rect_t *bounds = ecs_add(ecs, entity, RectComponent);

        if (i % 2 == 0) {
            comflab_t *comflab = ecs_add(ecs, entity, ComflabComponent);
            *comflab = (comflab_t){ 0 };
        }

        *pos = (v2d_t){ 0 };
        *dir = (v2d_t){ 0 };
        *bounds = (rect_t){ 0 };
    }
}

// -----------------------------------------------------------------------------
//  Teardown function

BENCH_TEARDOWN(teardown)
{
    (void)bench_run_ctx;

    if (tpool) {
        tpool_destroy(tpool);
        tpool = NULL;
    }

    ecs_free(ecs);
    ecs = NULL;
}

// -----------------------------------------------------------------------------
//  Benchmark cases

BENCH_CASE(bench_create)
{
    (void)bench_run_ctx;
    for (size_t i = 0; i < MAX_ENTITIES; i++) ecs_create(ecs);
}

BENCH_CASE(bench_create_destroy)
{
    (void)bench_run_ctx;
    for (size_t i = 0; i < MAX_ENTITIES; i++) ecs_destroy(ecs, ecs_create(ecs));
}

BENCH_CASE(bench_destroy_with_two_components)
{
    (void)bench_run_ctx;
    for (size_t i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity entity = (ecs_entity)(i + 1);
        ecs_destroy(ecs, entity);
    }
}

BENCH_CASE(bench_create_with_two_components)
{
    (void)bench_run_ctx;
    for (size_t i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity entity = ecs_create(ecs);
        ecs_add(ecs, entity, PosComponent);
        ecs_add(ecs, entity, RectComponent);
    }
}

BENCH_CASE(bench_add_remove)
{
    (void)bench_run_ctx;
    for (size_t i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity entity = ecs_create(ecs);
        ecs_add(ecs, entity, PosComponent);
        ecs_remove(ecs, entity, PosComponent);
    }
}

BENCH_CASE(bench_add_assign)
{
    (void)bench_run_ctx;
    for (size_t i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity entity = ecs_create(ecs);

        v2d_t *pos = (v2d_t *)ecs_add(ecs, entity, PosComponent);
        rect_t *rect = (rect_t *)ecs_add(ecs, entity, RectComponent);

        *pos = (v2d_t){ 1, 2 };
        *rect = (rect_t){ 1, 2, 3, 4 };
    }
}

BENCH_CASE(bench_get)
{
    (void)bench_run_ctx;
    for (size_t i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity entity = (ecs_entity)(i + 1);
        ecs_get(ecs, entity, PosComponent);
    }
}

BENCH_CASE(bench_queue_destroy)
{
    (void)bench_run_ctx;

    QueueDestroySystem = ecs_sys_create(ecs, queue_destroy_system, NULL);
    ecs_sys_require(ecs, QueueDestroySystem, PosComponent);
    ecs_sys_require(ecs, QueueDestroySystem, RectComponent);

    for (size_t i = 0; i < MAX_ENTITIES; i++) { ecs_create(ecs); }

    ecs_run_system(ecs, QueueDestroySystem, 0);
}

BENCH_CASE(bench_three_systems)
{
    (void)bench_run_ctx;
    ecs_run_system(ecs, MovementSystem, 0);
    ecs_run_system(ecs, ComflabSystem, 0);
    ecs_run_system(ecs, BoundsSystem, 0);
}

BENCH_CASE(bench_three_systems_scheduler)
{
    (void)bench_run_ctx;
    ecs_progress(ecs, 0);
}

/*=============================================================================
 * Suite runner helper
 *============================================================================*/

static void run_ecs_benchmarks(bench_ctx *ctx)
{
    RUN_BENCH_CASE(bench_create, setup, teardown, ctx);
    RUN_BENCH_CASE(bench_create_destroy, setup, teardown, ctx);
    RUN_BENCH_CASE(bench_create_with_two_components, setup, teardown, ctx);
    RUN_BENCH_CASE(bench_destroy_with_two_components, setup_destroy_with_two_components, teardown, ctx);
    RUN_BENCH_CASE(bench_add_remove, setup, teardown, ctx);
    RUN_BENCH_CASE(bench_add_assign, setup, teardown, ctx);
    RUN_BENCH_CASE(bench_get, setup_get, teardown, ctx);
    RUN_BENCH_CASE(bench_queue_destroy, setup, teardown, ctx);
    RUN_BENCH_CASE(bench_three_systems, setup_three_systems, teardown, ctx);
    RUN_BENCH_CASE(bench_three_systems_scheduler, setup_three_systems, teardown, ctx);
}

/*=============================================================================
 * Suites
 *============================================================================*/

BENCH_SUITE(suite_single_threaded)
{
    run_ecs_benchmarks(bench_suite_ctx);
}

BENCH_SUITE(suite_multi_threaded)
{
    run_ecs_benchmarks(bench_suite_ctx);
}

/*=============================================================================
 * Main
 *============================================================================*/

int main()
{
    bench_set_iterations(32);
    bench_set_warmup(4);

    bench_ctx single = { .num_threads = 1, .use_tpool = 0 };
    bench_ctx multi = { .num_threads = 14, .use_tpool = 1 };

    RUN_BENCH_SUITE(suite_single_threaded, &single);
    RUN_BENCH_SUITE(suite_multi_threaded, &multi);

    bench_print_stats();
    return bench_failed() ? 1 : 0;
}
