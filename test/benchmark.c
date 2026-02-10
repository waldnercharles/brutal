#include "brutal_bench.h"
#include "brutal_ecs.h"
#include "brutal_tpool.h"

#include <stdio.h>

#define MAX_ENTITIES (1024 * 1024)
#define NUM_READER_SYSTEMS 20
#define NUM_WRITER_SYSTEMS 10
#define NUM_MIXED_SYSTEMS 10
#define NUM_DEFERRED_SYSTEMS 8

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
static ecs_sys_t ReaderSystems[NUM_READER_SYSTEMS];
static ecs_sys_t WriterSystems[NUM_WRITER_SYSTEMS];
static ecs_sys_t MixedSystems[NUM_MIXED_SYSTEMS];
static ecs_sys_t DeferredSystems[NUM_DEFERRED_SYSTEMS];

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
        ecs_set_task_callbacks(ecs, bench_enqueue_cb, bench_wait_cb, NULL, ctx->num_threads * 64);
    }

    PosComponent = ecs_register_component(ecs, sizeof(v2d_t));
    RectComponent = ecs_register_component(ecs, sizeof(rect_t));
}

BENCH_SETUP(setup_destroy_with_two_components)
{
    bench_ctx *ctx = bench_run_ctx->udata;

    ecs = ecs_new();
    if (ctx->use_tpool && ctx->num_threads > 1) {
        tpool = tpool_new(ctx->num_threads, 0);
        ecs_set_task_callbacks(ecs, bench_enqueue_cb, bench_wait_cb, NULL, ctx->num_threads * 64);
    }

    PosComponent = ecs_register_component(ecs, sizeof(v2d_t));
    RectComponent = ecs_register_component(ecs, sizeof(rect_t));

    for (int i = 0; i < MAX_ENTITIES; i++) {
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
        ecs_set_task_callbacks(ecs, bench_enqueue_cb, bench_wait_cb, NULL, ctx->num_threads * 64);
    }

    PosComponent = ecs_register_component(ecs, sizeof(v2d_t));

    for (int i = 0; i < MAX_ENTITIES; i++) {
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
        ecs_set_task_callbacks(ecs, bench_enqueue_cb, bench_wait_cb, NULL, ctx->num_threads * 32);
    }

    PosComponent = ecs_register_component(ecs, sizeof(v2d_t));
    DirComponent = ecs_register_component(ecs, sizeof(v2d_t));
    ComflabComponent = ecs_register_component(ecs, sizeof(comflab_t));
    RectComponent = ecs_register_component(ecs, sizeof(rect_t));

    MovementSystem = ecs_sys_create(ecs, movement_system, NULL);
    ecs_sys_require(ecs, MovementSystem, PosComponent);
    ecs_sys_require(ecs, MovementSystem, DirComponent);
    if (ctx->use_tpool && ctx->num_threads > 1) {
        ecs_sys_set_parallel(ecs, MovementSystem, true);
    }

    ComflabSystem = ecs_sys_create(ecs, comflab_system, NULL);
    ecs_sys_require(ecs, ComflabSystem, ComflabComponent);
    if (ctx->use_tpool && ctx->num_threads > 1) {
        ecs_sys_set_parallel(ecs, ComflabSystem, true);
    }

    BoundsSystem = ecs_sys_create(ecs, bounds_system, NULL);
    ecs_sys_require(ecs, BoundsSystem, RectComponent);
    if (ctx->use_tpool && ctx->num_threads > 1) {
        ecs_sys_set_parallel(ecs, BoundsSystem, true);
    }

    for (int i = 0; i < MAX_ENTITIES; i++) {
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

    // Pre-build schedule for scheduler benchmarks
    ecs_progress(ecs, 0);
}

// Read-only system for many_readers benchmarks
static int reader_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;
    volatile float sum = 0;
    for (int i = 0; i < view->count; i++) {
        v2d_t *pos = ecs_get(ecs, view->entities[i], PosComponent);
        sum += pos->x + pos->y;
    }
    return 0;
}

BENCH_SETUP(setup_many_readers)
{
    bench_ctx *ctx = bench_run_ctx->udata;

    ecs = ecs_new();
    if (ctx->use_tpool && ctx->num_threads > 1) {
        tpool = tpool_new(ctx->num_threads, 0);
        ecs_set_task_callbacks(ecs, bench_enqueue_cb, bench_wait_cb, NULL, ctx->num_threads);
    }

    PosComponent = ecs_register_component(ecs, sizeof(v2d_t));

    for (int i = 0; i < NUM_READER_SYSTEMS; i++) {
        ReaderSystems[i] = ecs_sys_create(ecs, reader_system, NULL);
        ecs_sys_require(ecs, ReaderSystems[i], PosComponent);
        if (ctx->use_tpool && ctx->num_threads > 1) {
            ecs_sys_set_parallel(ecs, ReaderSystems[i], true);
        }
    }

    // Create entities
    for (int i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity entity = ecs_create(ecs);
        v2d_t *pos = ecs_add(ecs, entity, PosComponent);
        *pos = (v2d_t){ (float)i, (float)i };
    }

    // Pre-build schedule for scheduler benchmarks
    ecs_progress(ecs, 0);
}

// Write system for dependency_chain benchmarks
static int writer_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;
    for (int i = 0; i < view->count; i++) {
        v2d_t *pos = ecs_get(ecs, view->entities[i], PosComponent);
        pos->x += 1.0f;
        pos->y += 1.0f;
    }
    return 0;
}

BENCH_SETUP(setup_dependency_chain)
{
    bench_ctx *ctx = bench_run_ctx->udata;

    ecs = ecs_new();
    if (ctx->use_tpool && ctx->num_threads > 1) {
        tpool = tpool_new(ctx->num_threads, 0);
        ecs_set_task_callbacks(ecs, bench_enqueue_cb, bench_wait_cb, NULL, ctx->num_threads);
    }

    PosComponent = ecs_register_component(ecs, sizeof(v2d_t));

    for (int i = 0; i < NUM_WRITER_SYSTEMS; i++) {
        WriterSystems[i] = ecs_sys_create(ecs, writer_system, NULL);
        ecs_sys_require(ecs, WriterSystems[i], PosComponent);
        if (ctx->use_tpool && ctx->num_threads > 1) {
            ecs_sys_set_parallel(ecs, WriterSystems[i], true);
        }
    }

    // Create entities
    for (int i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity entity = ecs_create(ecs);
        v2d_t *pos = ecs_add(ecs, entity, PosComponent);
        *pos = (v2d_t){ 0.0f, 0.0f };
    }

    // Pre-build schedule for scheduler benchmarks
    ecs_progress(ecs, 0);
}

// Mixed read/write systems for realistic workload benchmark
static int pos_reader_1(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;
    volatile float sum = 0;
    for (int i = 0; i < view->count; i++) {
        v2d_t *pos = ecs_get(ecs, view->entities[i], PosComponent);
        sum += pos->x + pos->y;
    }
    return 0;
}

static int pos_reader_2(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;
    volatile float sum = 0;
    for (int i = 0; i < view->count; i++) {
        v2d_t *pos = ecs_get(ecs, view->entities[i], PosComponent);
        sum += pos->x * pos->y;
    }
    return 0;
}

static int dir_reader_1(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;
    volatile float sum = 0;
    for (int i = 0; i < view->count; i++) {
        v2d_t *dir = ecs_get(ecs, view->entities[i], DirComponent);
        sum += dir->x + dir->y;
    }
    return 0;
}

static int dir_reader_2(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;
    volatile float sum = 0;
    for (int i = 0; i < view->count; i++) {
        v2d_t *dir = ecs_get(ecs, view->entities[i], DirComponent);
        sum += dir->x * dir->y;
    }
    return 0;
}

static int rect_reader(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;
    volatile int sum = 0;
    for (int i = 0; i < view->count; i++) {
        rect_t *rect = ecs_get(ecs, view->entities[i], RectComponent);
        sum += rect->w * rect->h;
    }
    return 0;
}

static int comflab_reader(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;
    volatile float sum = 0;
    for (int i = 0; i < view->count; i++) {
        comflab_t *comflab = ecs_get(ecs, view->entities[i], ComflabComponent);
        sum += comflab->thingy;
    }
    return 0;
}

static int pos_writer(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;
    for (int i = 0; i < view->count; i++) {
        v2d_t *pos = ecs_get(ecs, view->entities[i], PosComponent);
        pos->x += 1.0f;
        pos->y += 1.0f;
    }
    return 0;
}

static int dir_writer(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;
    for (int i = 0; i < view->count; i++) {
        v2d_t *dir = ecs_get(ecs, view->entities[i], DirComponent);
        dir->x *= 0.99f;
        dir->y *= 0.99f;
    }
    return 0;
}

static int rect_writer(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;
    for (int i = 0; i < view->count; i++) {
        rect_t *rect = ecs_get(ecs, view->entities[i], RectComponent);
        rect->w++;
        rect->h++;
    }
    return 0;
}

static int comflab_writer(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;
    for (int i = 0; i < view->count; i++) {
        comflab_t *comflab = ecs_get(ecs, view->entities[i], ComflabComponent);
        comflab->thingy *= 1.01f;
        comflab->dingy++;
    }
    return 0;
}

BENCH_SETUP(setup_mixed_workload)
{
    bench_ctx *ctx = bench_run_ctx->udata;

    ecs = ecs_new();
    if (ctx->use_tpool && ctx->num_threads > 1) {
        tpool = tpool_new(ctx->num_threads, 0);
        ecs_set_task_callbacks(ecs, bench_enqueue_cb, bench_wait_cb, NULL, ctx->num_threads);
    }

    PosComponent = ecs_register_component(ecs, sizeof(v2d_t));
    DirComponent = ecs_register_component(ecs, sizeof(v2d_t));
    RectComponent = ecs_register_component(ecs, sizeof(rect_t));
    ComflabComponent = ecs_register_component(ecs, sizeof(comflab_t));

    // Stage 0: 6 readers (all non-conflicting)
    MixedSystems[0] = ecs_sys_create(ecs, pos_reader_1, NULL);
    ecs_sys_require(ecs, MixedSystems[0], PosComponent);
    if (ctx->use_tpool && ctx->num_threads > 1) {
        ecs_sys_set_parallel(ecs, MixedSystems[0], true);
    }

    MixedSystems[1] = ecs_sys_create(ecs, pos_reader_2, NULL);
    ecs_sys_require(ecs, MixedSystems[1], PosComponent);
    if (ctx->use_tpool && ctx->num_threads > 1) {
        ecs_sys_set_parallel(ecs, MixedSystems[1], true);
    }

    MixedSystems[2] = ecs_sys_create(ecs, dir_reader_1, NULL);
    ecs_sys_require(ecs, MixedSystems[2], DirComponent);
    if (ctx->use_tpool && ctx->num_threads > 1) {
        ecs_sys_set_parallel(ecs, MixedSystems[2], true);
    }

    MixedSystems[3] = ecs_sys_create(ecs, dir_reader_2, NULL);
    ecs_sys_require(ecs, MixedSystems[3], DirComponent);
    if (ctx->use_tpool && ctx->num_threads > 1) {
        ecs_sys_set_parallel(ecs, MixedSystems[3], true);
    }

    MixedSystems[4] = ecs_sys_create(ecs, rect_reader, NULL);
    ecs_sys_require(ecs, MixedSystems[4], RectComponent);
    if (ctx->use_tpool && ctx->num_threads > 1) {
        ecs_sys_set_parallel(ecs, MixedSystems[4], true);
    }

    MixedSystems[5] = ecs_sys_create(ecs, comflab_reader, NULL);
    ecs_sys_require(ecs, MixedSystems[5], ComflabComponent);
    if (ctx->use_tpool && ctx->num_threads > 1) {
        ecs_sys_set_parallel(ecs, MixedSystems[5], true);
    }

    // Stage 1: 4 writers (conflict with earlier readers)
    MixedSystems[6] = ecs_sys_create(ecs, pos_writer, NULL);
    ecs_sys_require(ecs, MixedSystems[6], PosComponent);
    if (ctx->use_tpool && ctx->num_threads > 1) {
        ecs_sys_set_parallel(ecs, MixedSystems[6], true);
    }

    MixedSystems[7] = ecs_sys_create(ecs, dir_writer, NULL);
    ecs_sys_require(ecs, MixedSystems[7], DirComponent);
    if (ctx->use_tpool && ctx->num_threads > 1) {
        ecs_sys_set_parallel(ecs, MixedSystems[7], true);
    }

    MixedSystems[8] = ecs_sys_create(ecs, rect_writer, NULL);
    ecs_sys_require(ecs, MixedSystems[8], RectComponent);
    if (ctx->use_tpool && ctx->num_threads > 1) {
        ecs_sys_set_parallel(ecs, MixedSystems[8], true);
    }

    MixedSystems[9] = ecs_sys_create(ecs, comflab_writer, NULL);
    ecs_sys_require(ecs, MixedSystems[9], ComflabComponent);
    if (ctx->use_tpool && ctx->num_threads > 1) {
        ecs_sys_set_parallel(ecs, MixedSystems[9], true);
    }

    // Create entities with all 4 components
    for (int i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity entity = ecs_create(ecs);
        v2d_t *pos = ecs_add(ecs, entity, PosComponent);
        v2d_t *dir = ecs_add(ecs, entity, DirComponent);
        rect_t *rect = ecs_add(ecs, entity, RectComponent);
        comflab_t *comflab = ecs_add(ecs, entity, ComflabComponent);

        *pos = (v2d_t){ 0.0f, 0.0f };
        *dir = (v2d_t){ 1.0f, 1.0f };
        *rect = (rect_t){ 0, 0, 10, 10 };
        *comflab = (comflab_t){ 1.0f, false, 0 };
    }

    // Pre-build schedule for scheduler benchmarks
    ecs_progress(ecs, 0);
}

// -----------------------------------------------------------------------------
//  Deferred commands workload systems - simulates realistic game scenario

static ecs_comp_t HealthComponent;
static ecs_comp_t VelComponent;
static ecs_comp_t EffectComponent;
static ecs_comp_t LifetimeComponent;

typedef struct
{
    int hp;
    int max_hp;
} health_t;
typedef struct
{
    int effect_type;
    int amount;
} effect_t;
typedef struct
{
    int ticks_remaining;
} lifetime_t;

// Stage 0: Readers + conditional spawners (all can run in parallel)

static int deferred_movement_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;
    // Read Pos + Vel, write Pos
    for (int i = 0; i < view->count; i++) {
        v2d_t *pos = ecs_get(ecs, view->entities[i], PosComponent);
        v2d_t *vel = ecs_get(ecs, view->entities[i], VelComponent);
        pos->x += vel->x * 0.016f;
        pos->y += vel->y * 0.016f;
    }
    return 0;
}

static int deferred_collision_checker(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;
    // Read Pos, conditionally spawn damage effect entities
    for (int i = 0; i < view->count; i += 500) {
        v2d_t *pos = ecs_get(ecs, view->entities[i], PosComponent);
        if ((int)pos->x % 100 < 50) { // Collision detected
            ecs_entity effect_ent = ecs_create(ecs);
            effect_t *effect = ecs_add(ecs, effect_ent, EffectComponent);
            effect->effect_type = 0; // damage
            effect->amount = 10;
            ecs_add(ecs, effect_ent, LifetimeComponent);
        }
    }
    return 0;
}

static int deferred_health_checker(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;
    // Read Health, conditionally spawn healing effects
    for (int i = 0; i < view->count; i += 800) {
        health_t *health = ecs_get(ecs, view->entities[i], HealthComponent);
        if (health->hp < health->max_hp / 2) { // Low health
            ecs_entity heal_ent = ecs_create(ecs);
            effect_t *effect = ecs_add(ecs, heal_ent, EffectComponent);
            effect->effect_type = 1; // heal
            effect->amount = 20;
            ecs_add(ecs, heal_ent, LifetimeComponent);
        }
    }
    return 0;
}

// Stage 1: Writers + deferred operations (conflict with stage 0)

static int deferred_effect_processor(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;
    // Read Effect, add/modify Health component (deferred add)
    for (int i = 0; i < view->count; i++) {
        // Simulate finding target entity and modifying health
        if (i % 2 == 0) {
            // Add Health component to entities that don't have it
            ecs_entity target = view->entities[i];
            if (!ecs_has(ecs, target, HealthComponent)) {
                health_t *h = ecs_add(ecs, target, HealthComponent);
                h->hp = 100;
                h->max_hp = 100;
            }
        }
    }
    return 0;
}

static int deferred_damage_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;
    // Read/write Health, destroy entities with hp <= 0
    for (int i = 0; i < view->count; i++) {
        health_t *health = ecs_get(ecs, view->entities[i], HealthComponent);
        health->hp -= 1; // Constant damage over time
        if (health->hp <= 0) { ecs_destroy(ecs, view->entities[i]); }
    }
    return 0;
}

static int deferred_lifetime_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;
    // Read/write Lifetime, destroy expired entities
    for (int i = 0; i < view->count; i++) {
        lifetime_t *lifetime = ecs_get(ecs, view->entities[i], LifetimeComponent);
        lifetime->ticks_remaining--;
        if (lifetime->ticks_remaining <= 0) {
            ecs_destroy(ecs, view->entities[i]);
        }
    }
    return 0;
}

static int deferred_velocity_damping(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;
    // Read/write Vel
    for (int i = 0; i < view->count; i++) {
        v2d_t *vel = ecs_get(ecs, view->entities[i], VelComponent);
        vel->x *= 0.99f;
        vel->y *= 0.99f;
    }
    return 0;
}

static int deferred_position_boundary(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;
    // Read Pos, conditionally remove Vel component
    for (int i = 0; i < view->count; i += 300) {
        v2d_t *pos = ecs_get(ecs, view->entities[i], PosComponent);
        if (pos->x > 1000.0f || pos->y > 1000.0f) {
            // Out of bounds - stop movement by removing velocity
            if (ecs_has(ecs, view->entities[i], VelComponent)) {
                ecs_remove(ecs, view->entities[i], VelComponent);
            }
        }
    }
    return 0;
}

BENCH_SETUP(setup_deferred_workload)
{
    bench_ctx *ctx = bench_run_ctx->udata;

    ecs = ecs_new();
    if (ctx->use_tpool && ctx->num_threads > 1) {
        tpool = tpool_new(ctx->num_threads, 0);
        ecs_set_task_callbacks(ecs, bench_enqueue_cb, bench_wait_cb, NULL, ctx->num_threads);
    }

    PosComponent = ecs_register_component(ecs, sizeof(v2d_t));
    VelComponent = ecs_register_component(ecs, sizeof(v2d_t));
    HealthComponent = ecs_register_component(ecs, sizeof(health_t));
    EffectComponent = ecs_register_component(ecs, sizeof(effect_t));
    LifetimeComponent = ecs_register_component(ecs, sizeof(lifetime_t));

    // Stage 0: Readers + conditional spawners (all parallel, no writes to shared components)
    DeferredSystems[0] = ecs_sys_create(ecs, deferred_movement_system, NULL);
    ecs_sys_require(ecs, DeferredSystems[0], PosComponent);
    ecs_sys_require(ecs, DeferredSystems[0], VelComponent);
    if (ctx->use_tpool && ctx->num_threads > 1) {
        ecs_sys_set_parallel(ecs, DeferredSystems[0], true);
    }

    DeferredSystems[1] = ecs_sys_create(ecs, deferred_collision_checker, NULL);
    ecs_sys_require(ecs, DeferredSystems[1], PosComponent);
    if (ctx->use_tpool && ctx->num_threads > 1) {
        ecs_sys_set_parallel(ecs, DeferredSystems[1], true);
    }

    DeferredSystems[2] = ecs_sys_create(ecs, deferred_health_checker, NULL);
    ecs_sys_require(ecs, DeferredSystems[2], HealthComponent);
    if (ctx->use_tpool && ctx->num_threads > 1) {
        ecs_sys_set_parallel(ecs, DeferredSystems[2], true);
    }

    // Stage 1: Writers + deferred operations (conflict with stage 0)
    DeferredSystems[3] = ecs_sys_create(ecs, deferred_effect_processor, NULL);
    ecs_sys_require(ecs, DeferredSystems[3], EffectComponent);
    if (ctx->use_tpool && ctx->num_threads > 1) {
        ecs_sys_set_parallel(ecs, DeferredSystems[3], true);
    }

    DeferredSystems[4] = ecs_sys_create(ecs, deferred_damage_system, NULL);
    ecs_sys_require(ecs, DeferredSystems[4], HealthComponent);
    if (ctx->use_tpool && ctx->num_threads > 1) {
        ecs_sys_set_parallel(ecs, DeferredSystems[4], true);
    }

    DeferredSystems[5] = ecs_sys_create(ecs, deferred_lifetime_system, NULL);
    ecs_sys_require(ecs, DeferredSystems[5], LifetimeComponent);
    if (ctx->use_tpool && ctx->num_threads > 1) {
        ecs_sys_set_parallel(ecs, DeferredSystems[5], true);
    }

    DeferredSystems[6] = ecs_sys_create(ecs, deferred_velocity_damping, NULL);
    ecs_sys_require(ecs, DeferredSystems[6], VelComponent);
    if (ctx->use_tpool && ctx->num_threads > 1) {
        ecs_sys_set_parallel(ecs, DeferredSystems[6], true);
    }

    DeferredSystems[7] = ecs_sys_create(ecs, deferred_position_boundary, NULL);
    ecs_sys_require(ecs, DeferredSystems[7], PosComponent);
    if (ctx->use_tpool && ctx->num_threads > 1) {
        ecs_sys_set_parallel(ecs, DeferredSystems[7], true);
    }

    // Create entities with various component combinations
    for (int i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity entity = ecs_create(ecs);

        // All entities have position
        v2d_t *pos = ecs_add(ecs, entity, PosComponent);
        *pos = (v2d_t){ (float)(i % 1000), (float)(i % 1000) };

        // 80% have velocity
        if (i % 5 != 0) {
            v2d_t *vel = ecs_add(ecs, entity, VelComponent);
            *vel = (v2d_t){ 10.0f, 10.0f };
        }

        // 60% have health
        if (i % 5 < 3) {
            health_t *health = ecs_add(ecs, entity, HealthComponent);
            health->hp = 50 + (i % 50);
            health->max_hp = 100;
        }

        // 5% start as effect entities
        if (i % 20 == 0) {
            effect_t *effect = ecs_add(ecs, entity, EffectComponent);
            effect->effect_type = i % 2;
            effect->amount = 15;

            lifetime_t *lifetime = ecs_add(ecs, entity, LifetimeComponent);
            lifetime->ticks_remaining = 10;
        }
    }

    // Pre-build schedule for scheduler benchmarks
    ecs_progress(ecs, 0);
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
    for (int i = 0; i < MAX_ENTITIES; i++) ecs_create(ecs);
}

BENCH_CASE(bench_create_destroy)
{
    (void)bench_run_ctx;
    for (int i = 0; i < MAX_ENTITIES; i++) ecs_destroy(ecs, ecs_create(ecs));
}

BENCH_CASE(bench_destroy_with_two_components)
{
    (void)bench_run_ctx;
    for (int i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity entity = (ecs_entity)(i + 1);
        ecs_destroy(ecs, entity);
    }
}

BENCH_CASE(bench_create_with_two_components)
{
    (void)bench_run_ctx;
    for (int i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity entity = ecs_create(ecs);
        ecs_add(ecs, entity, PosComponent);
        ecs_add(ecs, entity, RectComponent);
    }
}

BENCH_CASE(bench_add_remove)
{
    (void)bench_run_ctx;
    for (int i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity entity = ecs_create(ecs);
        ecs_add(ecs, entity, PosComponent);
        ecs_remove(ecs, entity, PosComponent);
    }
}

BENCH_CASE(bench_add_assign)
{
    (void)bench_run_ctx;
    for (int i = 0; i < MAX_ENTITIES; i++) {
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
    for (int i = 0; i < MAX_ENTITIES; i++) {
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

    for (int i = 0; i < MAX_ENTITIES; i++) { ecs_create(ecs); }

    ecs_run_system(ecs, QueueDestroySystem);
}

BENCH_CASE(bench_three_systems)
{
    (void)bench_run_ctx;
    ecs_run_system(ecs, MovementSystem);
    ecs_run_system(ecs, ComflabSystem);
    ecs_run_system(ecs, BoundsSystem);
}

BENCH_CASE(bench_three_systems_scheduler)
{
    (void)bench_run_ctx;
    ecs_progress(ecs, 0);
}

BENCH_CASE(bench_many_readers)
{
    (void)bench_run_ctx;
    for (int i = 0; i < NUM_READER_SYSTEMS; i++)
        ecs_run_system(ecs, ReaderSystems[i]);
}

BENCH_CASE(bench_many_readers_scheduler)
{
    (void)bench_run_ctx;
    ecs_progress(ecs, 0);
}

BENCH_CASE(bench_dependency_chain)
{
    (void)bench_run_ctx;
    for (int i = 0; i < NUM_WRITER_SYSTEMS; i++)
        ecs_run_system(ecs, WriterSystems[i]);
}

BENCH_CASE(bench_dependency_chain_scheduler)
{
    (void)bench_run_ctx;
    ecs_progress(ecs, 0);
}

BENCH_CASE(bench_mixed_workload)
{
    (void)bench_run_ctx;
    for (int i = 0; i < NUM_MIXED_SYSTEMS; i++)
        ecs_run_system(ecs, MixedSystems[i]);
}

BENCH_CASE(bench_mixed_workload_scheduler)
{
    (void)bench_run_ctx;
    ecs_progress(ecs, 0);
}

BENCH_CASE(bench_deferred_workload)
{
    (void)bench_run_ctx;
    for (int i = 0; i < NUM_DEFERRED_SYSTEMS; i++)
        ecs_run_system(ecs, DeferredSystems[i]);
}

BENCH_CASE(bench_deferred_workload_scheduler)
{
    (void)bench_run_ctx;
    ecs_progress(ecs, 0);
}

/*=============================================================================
 * Suite runner helper
 *============================================================================*/

static void run_ecs_benchmarks(bench_ctx *ctx)
{
    /* RUN_BENCH_CASE(bench_create, setup, teardown, ctx); */
    /* RUN_BENCH_CASE(bench_create_destroy, setup, teardown, ctx); */
    /* RUN_BENCH_CASE(bench_create_with_two_components, setup, teardown, ctx); */
    /* RUN_BENCH_CASE(bench_destroy_with_two_components, setup_destroy_with_two_components, teardown, ctx); */
    /* RUN_BENCH_CASE(bench_add_remove, setup, teardown, ctx); */
    /* RUN_BENCH_CASE(bench_add_assign, setup, teardown, ctx); */
    /* RUN_BENCH_CASE(bench_get, setup_get, teardown, ctx); */
    /* RUN_BENCH_CASE(bench_queue_destroy, setup, teardown, ctx); */
    RUN_BENCH_CASE(bench_three_systems, setup_three_systems, teardown, ctx);
    // RUN_BENCH_CASE(bench_three_systems_scheduler, setup_three_systems, teardown, ctx);
    /* RUN_BENCH_CASE(bench_many_readers, setup_many_readers, teardown, ctx); */
    /* RUN_BENCH_CASE(bench_many_readers_scheduler, setup_many_readers, teardown, ctx); */
    /* RUN_BENCH_CASE(bench_dependency_chain, setup_dependency_chain, teardown, ctx); */
    /* RUN_BENCH_CASE(bench_dependency_chain_scheduler, setup_dependency_chain, teardown, ctx); */
    /* RUN_BENCH_CASE(bench_mixed_workload, setup_mixed_workload, teardown, ctx); */
    /* RUN_BENCH_CASE(bench_mixed_workload_scheduler, setup_mixed_workload, teardown, ctx); */
    // RUN_BENCH_CASE(bench_deferred_workload, setup_deferred_workload, teardown, ctx);
    // RUN_BENCH_CASE(bench_deferred_workload_scheduler, setup_deferred_workload, teardown, ctx);
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
    bench_ctx multi = { .num_threads = 8, .use_tpool = 1 };

    RUN_BENCH_SUITE(suite_single_threaded, &single);
    RUN_BENCH_SUITE(suite_multi_threaded, &multi);

    bench_print_stats();
    return bench_failed() ? 1 : 0;
}
