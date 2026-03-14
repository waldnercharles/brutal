#define BRUTAL_BENCH_IMPLEMENTATION
#include "brutal_bench.h"

typedef struct { float x, y; } Position;
typedef struct { float vx, vy; } Velocity;
typedef struct { int x, y, w, h; } Bounds;
typedef struct { float thingy; bool mingy; int dingy; } Comflab;

#define ECS_MAX_ENTITIES (1024 * 1024)

#define ECS_COMPONENTS(X) \
    X(Position, position)  \
    X(Velocity, velocity)  \
    X(Bounds,   bounds)    \
    X(Comflab,  comflab)

#include "arch_ecs.h"
#include "brutal_tpool.h"

#include <stdio.h>

#define MAX_ENTITIES (ECS_MAX_ENTITIES - 1)

typedef struct
{
    int num_threads;
} bench_ctx;

static ecs_world world;
static tpool_t *pool;

/* ---- thread pool adapter ---- */

static int bench_enqueue(int (*fn)(void *), void *arg, void *udata)
{
    tpool_enqueue(udata, fn, arg);
    return 0;
}

static void bench_wait(void *udata)
{
    tpool_wait(udata);
}

/* ---- system functions (ecs_parallel_for callbacks) ---- */

static void movement_sys(ecs_task *t)
{
    for (int i = 0; i < t->n; i++) {
        t->position[i].x += t->position[i].x + t->velocity[i].vx * (1.f / 60.f);
        t->position[i].y += t->position[i].y + t->velocity[i].vy * (1.f / 60.f);
    }
}

static void comflab_sys(ecs_task *t)
{
    for (int i = 0; i < t->n; i++) {
        t->comflab[i].thingy *= 1.000001f;
        t->comflab[i].mingy = !t->comflab[i].mingy;
        t->comflab[i].dingy++;
    }
}

static void bounds_sys(ecs_task *t)
{
    for (int i = 0; i < t->n; i++) {
        t->bounds[i].x = 1;
        t->bounds[i].y = 1;
        t->bounds[i].w = 1;
        t->bounds[i].h = 1;
    }
}

/* ---- ECS_FOR system functions (sequential iteration) ---- */

static void movement_for(ecs_world *w)
{
    ECS_FOR(w, Position, pos, Velocity, vel) {
        pos->x += pos->x + vel->vx * (1.f / 60.f);
        pos->y += pos->y + vel->vy * (1.f / 60.f);
    }
}

static void comflab_for(ecs_world *w)
{
    ECS_FOR(w, Comflab, c) {
        c->thingy *= 1.000001f;
        c->mingy = !c->mingy;
        c->dingy++;
    }
}

static void bounds_for(ecs_world *w)
{
    ECS_FOR(w, Bounds, b) {
        b->x = 1;
        b->y = 1;
        b->w = 1;
        b->h = 1;
    }
}

/* ============================================================================
 * Setups
 * ========================================================================= */

BENCH_SETUP(setup_empty)
{
    (void)bench_run_ctx;
    world = (ecs_world){0};
}

BENCH_SETUP(setup_with_entities)
{
    (void)bench_run_ctx;
    world = (ecs_world){0};

    for (int i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity e = ecs_spawn(&world, Position, Bounds);
        ecs_set(&world, e, (Position){0});
        ecs_set(&world, e, (Bounds){0});
    }
}

BENCH_SETUP(setup_with_pos)
{
    (void)bench_run_ctx;
    world = (ecs_world){0};

    for (int i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity e = ecs_spawn(&world, Position);
        ecs_set(&world, e, (Position){0});
    }
}

BENCH_SETUP(setup_three_systems)
{
    bench_ctx *ctx = bench_run_ctx->udata;
    world = (ecs_world){0};

    if (ctx->num_threads > 1) {
        pool = tpool_new(ctx->num_threads, 0);
        ecs_set_threads(&world, bench_enqueue, bench_wait, pool, ctx->num_threads);
    }

    for (int i = 0; i < MAX_ENTITIES; i++) {
        if (i % 2 == 0) {
            ecs_entity e = ecs_spawn(&world, Position, Velocity, Bounds);
            ecs_set(&world, e, (Position){0});
            ecs_set(&world, e, (Velocity){0});
            ecs_set(&world, e, (Bounds){0});
        } else {
            ecs_entity e = ecs_spawn(&world, Position, Velocity, Bounds, Comflab);
            ecs_set(&world, e, (Position){0});
            ecs_set(&world, e, (Velocity){0});
            ecs_set(&world, e, (Bounds){0});
            ecs_set(&world, e, (Comflab){0});
        }
    }
}

BENCH_TEARDOWN(teardown)
{
    (void)bench_run_ctx;
    if (pool) {
        tpool_destroy(pool);
        pool = NULL;
    }
    ecs_world_destroy(&world);
}

BENCH_TEARDOWN(teardown_noop)
{
    (void)bench_run_ctx;
}

/* ============================================================================
 * Benchmark cases
 * ========================================================================= */

/* -- Entity lifecycle -- */

BENCH_CASE(bench_create)
{
    (void)bench_run_ctx;
    for (int i = 0; i < MAX_ENTITIES; i++)
        ecs_spawn(&world, Position);
}

BENCH_CASE(bench_create_destroy)
{
    (void)bench_run_ctx;
    for (int i = 0; i < MAX_ENTITIES; i++)
        ecs_kill(&world, ecs_spawn(&world, Position));
}

BENCH_CASE(bench_create_with_two_components)
{
    (void)bench_run_ctx;
    for (int i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity e = ecs_spawn(&world, Position, Bounds);
        ecs_set(&world, e, (Position){ 1, 2 });
        ecs_set(&world, e, (Bounds){ 1, 2, 3, 4 });
    }
}

BENCH_CASE(bench_destroy_with_two_components)
{
    (void)bench_run_ctx;
    for (int i = 0; i < MAX_ENTITIES; i++)
        ecs_kill(&world, (ecs_entity){ .id = i + 1, .gen = 1 });
}

BENCH_CASE(bench_get)
{
    (void)bench_run_ctx;
    for (int i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity e = { .id = i + 1, .gen = 1 };
        ecs_get_position(&world, e);
    }
}

/* -- Systems -- */

BENCH_CASE(bench_three_systems_for)
{
    (void)bench_run_ctx;
    movement_for(&world);
    comflab_for(&world);
    bounds_for(&world);
}

BENCH_CASE(bench_three_systems_parallel)
{
    (void)bench_run_ctx;
    ecs_parallel_for(&world, movement_sys, Position, Velocity);
    ecs_parallel_for(&world, comflab_sys, Comflab);
    ecs_parallel_for(&world, bounds_sys, Bounds);
}

/* ============================================================================
 * Suite runner
 * ========================================================================= */

static void run_benchmarks(bench_ctx *ctx)
{
    RUN_BENCH_CASE(bench_create, setup_empty, teardown, ctx);
    RUN_BENCH_CASE(bench_create_destroy, setup_empty, teardown, ctx);
    RUN_BENCH_CASE(bench_create_with_two_components, setup_empty, teardown, ctx);
    RUN_BENCH_CASE(bench_destroy_with_two_components, setup_with_entities, teardown, ctx);
    RUN_BENCH_CASE(bench_get, setup_with_pos, teardown_noop, ctx);
    if (ctx->num_threads <= 1) {
        RUN_BENCH_CASE(bench_three_systems_for, setup_three_systems, teardown, ctx);
    }
    RUN_BENCH_CASE(bench_three_systems_parallel, setup_three_systems, teardown, ctx);
}

/* ============================================================================
 * Suites
 * ========================================================================= */

BENCH_SUITE(suite_single_threaded)
{
    run_benchmarks(bench_suite_ctx);
}

BENCH_SUITE(suite_multi_threaded)
{
    run_benchmarks(bench_suite_ctx);
}

/* ============================================================================
 * Main
 * ========================================================================= */

int main()
{
    bench_set_iterations(32);
    bench_set_warmup(4);

    bench_ctx single = { .num_threads = 1 };
    bench_ctx multi  = { .num_threads = 8 };

    RUN_BENCH_SUITE(suite_single_threaded, &single);
    RUN_BENCH_SUITE(suite_multi_threaded, &multi);

    bench_print_stats();
    return bench_failed() ? 1 : 0;
}
