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

#include <stdio.h>

#define MAX_ENTITIES (ECS_MAX_ENTITIES - 1)

static ecs_world world;

/* ---- ECS_FOR system functions ---- */

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
    ecs_world_init(&world);
}

BENCH_SETUP(setup_with_entities)
{
    (void)bench_run_ctx;
    ecs_world_init(&world);

    for (int i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity e = ecs_create(&world, Position, Bounds);
        ecs_set_position(&world, e, (Position){0});
        ecs_set_bounds(&world, e, (Bounds){0});
    }
}

BENCH_SETUP(setup_with_pos)
{
    (void)bench_run_ctx;
    ecs_world_init(&world);

    for (int i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity e = ecs_create(&world, Position);
        ecs_set_position(&world, e, (Position){0});
    }
}

BENCH_SETUP(setup_three_systems)
{
    (void)bench_run_ctx;
    ecs_world_init(&world);

    for (int i = 0; i < MAX_ENTITIES; i++) {
        if (i % 2 == 0) {
            ecs_entity e = ecs_create(&world, Position, Velocity, Bounds);
            ecs_set_position(&world, e, (Position){0});
            ecs_set_velocity(&world, e, (Velocity){0});
            ecs_set_bounds(&world, e, (Bounds){0});
        } else {
            ecs_entity e = ecs_create(&world, Position, Velocity, Bounds, Comflab);
            ecs_set_position(&world, e, (Position){0});
            ecs_set_velocity(&world, e, (Velocity){0});
            ecs_set_bounds(&world, e, (Bounds){0});
            ecs_set_comflab(&world, e, (Comflab){0});
        }
    }
}

BENCH_TEARDOWN(teardown)
{
    (void)bench_run_ctx;
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
        ecs_create(&world, Position);
}

BENCH_CASE(bench_create_destroy)
{
    (void)bench_run_ctx;
    for (int i = 0; i < MAX_ENTITIES; i++)
        ecs_destroy(&world, ecs_create(&world, Position));
}

BENCH_CASE(bench_create_with_two_components)
{
    (void)bench_run_ctx;
    for (int i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity e = ecs_create(&world, Position, Bounds);
        ecs_set_position(&world, e, (Position){ 1, 2 });
        ecs_set_bounds(&world, e, (Bounds){ 1, 2, 3, 4 });
    }
}

BENCH_CASE(bench_destroy_with_two_components)
{
    (void)bench_run_ctx;
    for (int i = 0; i < MAX_ENTITIES; i++)
        ecs_destroy(&world, ecs_pack_(i + 1, 1));
}

BENCH_CASE(bench_get)
{
    (void)bench_run_ctx;
    for (int i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity e = ecs_pack_(i + 1, 1);
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

/* ============================================================================
 * Suite runner
 * ========================================================================= */

BENCH_SUITE(suite_single_threaded)
{
    RUN_BENCH_CASE(bench_create, setup_empty, teardown, bench_suite_ctx);
    RUN_BENCH_CASE(bench_create_destroy, setup_empty, teardown, bench_suite_ctx);
    RUN_BENCH_CASE(bench_create_with_two_components, setup_empty, teardown, bench_suite_ctx);
    RUN_BENCH_CASE(bench_destroy_with_two_components, setup_with_entities, teardown, bench_suite_ctx);
    RUN_BENCH_CASE(bench_get, setup_with_pos, teardown_noop, bench_suite_ctx);
    RUN_BENCH_CASE(bench_three_systems_for, setup_three_systems, teardown, bench_suite_ctx);
}

/* ============================================================================
 * Main
 * ========================================================================= */

int main()
{
    bench_set_iterations(32);
    bench_set_warmup(4);

    RUN_BENCH_SUITE(suite_single_threaded, NULL);

    bench_print_stats();
    return bench_failed() ? 1 : 0;
}
