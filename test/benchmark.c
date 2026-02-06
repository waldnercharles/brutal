#include "brutal_ecs.h"
#include "brutal_tpool.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

/*=============================================================================
 * Preamble
 *============================================================================*/

int random_int(int min, int max)
{
    return rand() % (max + 1 - min) + min;
}

#define MIN_ENTITIES (1 * 1024)
#define MAX_ENTITIES (1024 * 1024)

static double bench_wall_start_ms = 0.0;
static double bench_cpu_start_ms = 0.0;
static ecs_t *ecs = NULL;

static _Atomic uint64_t bench_last_progress_ms = 0;

static void setup();
static void teardown();

static double now_wall_ms()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1000000.0;
}

static double now_cpu_ms()
{
    return 1000.0 * (double)clock() / CLOCKS_PER_SEC;
}

static void bench_progress(const char *label)
{
    (void)label;
    atomic_store(&bench_last_progress_ms, (uint64_t)now_wall_ms());
}

static void bench_begin(const char *name)
{
    printf("---------------------------------------------------------------\n");
    printf("Running: %s\n", name);
    bench_progress(name);
    bench_wall_start_ms = now_wall_ms();
    bench_cpu_start_ms = now_cpu_ms();
}

static void bench_end()
{
    double wall_elapsed = now_wall_ms() - bench_wall_start_ms;
    double cpu_elapsed = now_cpu_ms() - bench_cpu_start_ms;
    printf("Wall time %f ms | CPU time %f ms\n", wall_elapsed, cpu_elapsed);
}

#define BENCH_RUN(fp, setup_fp, teardown_fp)                                   \
    setup_fp();                                                                \
    bench_begin(#fp);                                                          \
    fp();                                                                      \
    bench_end();                                                               \
    teardown_fp();

/*=============================================================================
 * Systems/components
 *============================================================================*/

// System types
ecs_sys_t MovementSystem;
ecs_sys_t ComflabSystem;
ecs_sys_t BoundsSystem;
ecs_sys_t QueueDestroySystem;

// Component types
ecs_comp_t PosComponent;
ecs_comp_t DirComponent;
ecs_comp_t RectComponent;
ecs_comp_t ComflabComponent;

// Position component
typedef struct
{
    float x, y;
} v2d_t;

// Rectangle component
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

/*=============================================================================
 * Multithreading
 *============================================================================*/

static int num_threads = 1;
static int use_tpool = 0;
static tpool_t *tpool = NULL;

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

/*=============================================================================
 * Setup / teardown functions
 *============================================================================*/

int movement_system(ecs_t *ecs, ecs_view *view, void *udata);
int comflab_system(ecs_t *ecs, ecs_view *view, void *udata);
int bounds_system(ecs_t *ecs, ecs_view *view, void *udata);

static void populate_three_systems_entities()
{
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

static void setup()
{
    ecs = ecs_new();
    if (use_tpool && num_threads > 1) {
        tpool = tpool_new(num_threads, 0);
        ecs_set_task_callbacks(ecs, bench_enqueue_cb, bench_wait_cb, NULL, num_threads);
    }

    PosComponent = ECS_COMPONENT(ecs, v2d_t);
    RectComponent = ECS_COMPONENT(ecs, rect_t);
}

static void setup_destroy_with_two_components()
{
    ecs = ecs_new();
    if (use_tpool && num_threads > 1) {
        tpool = tpool_new(num_threads, 0);
        ecs_set_task_callbacks(ecs, bench_enqueue_cb, bench_wait_cb, NULL, num_threads);
    }

    PosComponent = ECS_COMPONENT(ecs, v2d_t);
    RectComponent = ECS_COMPONENT(ecs, rect_t);

    for (size_t i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity entity = ecs_create(ecs);
        ecs_add(ecs, entity, PosComponent);
        ecs_add(ecs, entity, RectComponent);
    }
}

static void setup_three_systems_min()
{
    ecs = ecs_new();
    if (use_tpool && num_threads > 1) {
        tpool = tpool_new(num_threads, 0);
        ecs_set_task_callbacks(ecs, bench_enqueue_cb, bench_wait_cb, NULL, num_threads);
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

    populate_three_systems_entities();
}

static void setup_three_systems_max()
{
    ecs = ecs_new();
    if (use_tpool && num_threads > 1) {
        tpool = tpool_new(num_threads, 0);
        ecs_set_task_callbacks(ecs, bench_enqueue_cb, bench_wait_cb, NULL, num_threads);
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

    populate_three_systems_entities();
}

static void teardown()
{
    if (tpool) {
        tpool_destroy(tpool);
        tpool = NULL;
    }

    ecs_free(ecs);
    ecs = NULL;
}

static void setup_get()
{
    ecs = ecs_new();
    if (use_tpool && num_threads > 1) {
        tpool = tpool_new(num_threads, 0);
        ecs_set_task_callbacks(ecs, bench_enqueue_cb, bench_wait_cb, NULL, num_threads);
    }

    PosComponent = ECS_COMPONENT(ecs, v2d_t);

    for (size_t i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity entity = ecs_create(ecs);
        ecs_add(ecs, entity, PosComponent);
    }
}

/*=============================================================================
 * Update function callbacks
 *============================================================================*/

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

/*=============================================================================
 * Benchmark functions
 *============================================================================*/

// Creates entity IDs as fast as possible
static void bench_create()
{
    for (size_t i = 0; i < MAX_ENTITIES; i++) ecs_create(ecs);
}

// Creates entity IDs as fast as possible and immediately destroys the
// corresponding entity
static void bench_create_destroy()
{
    for (size_t i = 0; i < MAX_ENTITIES; i++) ecs_destroy(ecs, ecs_create(ecs));
}

static void bench_destroy_with_two_components()
{
    for (size_t i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity entity = (ecs_entity)(i + 1);
        ecs_destroy(ecs, entity);
    }
}

static void bench_create_with_two_components()
{
    for (size_t i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity entity = ecs_create(ecs);
        ecs_add(ecs, entity, PosComponent);
        ecs_add(ecs, entity, RectComponent);
    }
}

// Adds components to entities and assigns values to them
static void bench_add_remove()
{
    for (size_t i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity entity = ecs_create(ecs);
        ecs_add(ecs, entity, PosComponent);
        ecs_remove(ecs, entity, PosComponent);
    }
}

// Adds components to entities and assigns values to them
static void bench_add_assign()
{
    for (size_t i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity entity = ecs_create(ecs);

        v2d_t *pos = (v2d_t *)ecs_add(ecs, entity, PosComponent);
        rect_t *rect = (rect_t *)ecs_add(ecs, entity, RectComponent);

        *pos = (v2d_t){ 1, 2 };
        *rect = (rect_t){ 1, 2, 3, 4 };
    }
}

// Adds components to entities, retrieves the components, and assigns
// values to them
static void bench_get()
{
    for (size_t i = 0; i < MAX_ENTITIES; i++) {
        ecs_entity entity = (ecs_entity)(i + 1);
        ecs_get(ecs, entity, PosComponent);
    }
}

static void bench_queue_destroy()
{
    QueueDestroySystem = ecs_sys_create(ecs, queue_destroy_system, NULL);
    ecs_sys_require(ecs, QueueDestroySystem, PosComponent);
    ecs_sys_require(ecs, QueueDestroySystem, RectComponent);

    for (size_t i = 0; i < MAX_ENTITIES; i++) { ecs_create(ecs); }

    ecs_run_system(ecs, QueueDestroySystem, 0);
}

static void bench_three_systems(bool use_progress)
{
    if (use_progress) {
        ecs_progress(ecs, 0);
    } else {
        ecs_run_system(ecs, MovementSystem, 0);
        ecs_run_system(ecs, ComflabSystem, 0);
        ecs_run_system(ecs, BoundsSystem, 0);
    }
}

static void bench_three_systems_min()
{
    bench_three_systems(false);
}

static void bench_three_systems_max()
{
    bench_three_systems(false);
}

static void bench_three_systems_max_scheduler()
{
    bench_three_systems(true);
}

static void run_benchmarks(const char *label)
{
    printf("\n===============================================================\n");
    printf("Brutal ECS Benchmarks (%s)\n", label);
    printf("===============================================================\n");
    printf("Number of entities: %u\n", MAX_ENTITIES);
    bench_progress(label);

    BENCH_RUN(bench_create, setup, teardown);
    BENCH_RUN(bench_create_destroy, setup, teardown);
    BENCH_RUN(bench_create_with_two_components, setup, teardown);
    BENCH_RUN(bench_destroy_with_two_components, setup_destroy_with_two_components, teardown);
    BENCH_RUN(bench_add_remove, setup, teardown);
    BENCH_RUN(bench_add_assign, setup, teardown);
    BENCH_RUN(bench_get, setup_get, teardown);
    BENCH_RUN(bench_queue_destroy, setup, teardown);
    BENCH_RUN(bench_three_systems_min, setup_three_systems_min, teardown);
    BENCH_RUN(bench_three_systems_max, setup_three_systems_max, teardown);

    BENCH_RUN(bench_three_systems_max_scheduler, setup_three_systems_max, teardown);

    printf("---------------------------------------------------------------\n");
}

/*=============================================================================
 * Threading Analysis Benchmarks
 *============================================================================*/

typedef struct
{
    int entity_count;
    int thread_count;
    double setup_ms;
    double wall_time_ms;
    double cpu_time_ms;
    double entities_per_sec;
} bench_result_t;

static int heavy_work_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;

    for (int i = 0; i < view->count; i++) {
        ecs_entity entity = view->entities[i];
        v2d_t *pos = ecs_get(ecs, entity, PosComponent);
        v2d_t *dir = ecs_get(ecs, entity, DirComponent);

        // Simulate heavier computation
        for (int j = 0; j < 100; j++) {
            pos->x += dir->x * 0.001f;
            pos->y += dir->y * 0.001f;
            dir->x = pos->x * 0.999f;
            dir->y = pos->y * 0.999f;
        }
    }

    return 0;
}

static int light_work_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;

    for (int i = 0; i < view->count; i++) {
        ecs_entity entity = view->entities[i];
        v2d_t *pos = ecs_get(ecs, entity, PosComponent);
        v2d_t *dir = ecs_get(ecs, entity, DirComponent);

        pos->x += dir->x;
        pos->y += dir->y;
    }

    return 0;
}

static bench_result_t run_threading_test(
    int entity_count,
    int thread_count,
    int (*system_fn)(ecs_t *, ecs_view *, void *),
    const char *work_type,
    double baseline_wall_ms
)
{
    bench_result_t result = { 0 };
    result.entity_count = entity_count;
    result.thread_count = thread_count;
    bench_progress(work_type);
    double setup_start_ms = now_wall_ms();

    // Setup
    ecs_t *test_ecs = ecs_new();
    tpool_t *test_pool = NULL;

    if (thread_count > 1) {
        test_pool = tpool_new(thread_count, 0);
        ecs_set_task_callbacks(test_ecs, bench_enqueue_cb, bench_wait_cb, NULL, thread_count);
        tpool = test_pool; // Set global for callbacks
    }

    ecs_comp_t pos_id = ECS_COMPONENT(test_ecs, v2d_t);
    ecs_comp_t dir_id = ECS_COMPONENT(test_ecs, v2d_t);
    PosComponent = pos_id;
    DirComponent = dir_id;

    // Create entities
    for (int i = 0; i < entity_count; i++) {
        ecs_entity e = ecs_create(test_ecs);
        v2d_t *pos = ecs_add(test_ecs, e, pos_id);
        v2d_t *dir = ecs_add(test_ecs, e, dir_id);
        pos->x = (float)i;
        pos->y = (float)i;
        dir->x = 1.0f;
        dir->y = 0.5f;
    }

    // Create system
    ecs_sys_t sys = ecs_sys_create(test_ecs, system_fn, NULL);
    ecs_sys_require(test_ecs, sys, pos_id);
    ecs_sys_require(test_ecs, sys, dir_id);
    ecs_sys_write(test_ecs, sys, pos_id);
    ecs_sys_write(test_ecs, sys, dir_id);

    // Warmup
    ecs_progress(test_ecs, 0);
    result.setup_ms = now_wall_ms() - setup_start_ms;

    // Benchmark (run multiple iterations for accuracy)
    const int iterations = 10;
    double bench_wall_start = now_wall_ms();
    double bench_cpu_start = now_cpu_ms();

    for (int iter = 0; iter < iterations; iter++) { ecs_progress(test_ecs, 0); }

    double bench_wall_end = now_wall_ms();
    double bench_cpu_end = now_cpu_ms();
    result.wall_time_ms = (bench_wall_end - bench_wall_start) / iterations;
    result.cpu_time_ms = (bench_cpu_end - bench_cpu_start) / iterations;
    result.entities_per_sec = (double)entity_count / (result.wall_time_ms / 1000.0);

    // Cleanup
    if (test_pool) {
        tpool_destroy(test_pool);
        tpool = NULL;
    }
    ecs_free(test_ecs);

    printf(
        "  %s | %7d entities | %2d threads | wall %8.3f ms | cpu %8.3f ms | "
        "%12.0f entities/sec",
        work_type,
        entity_count,
        thread_count,
        result.wall_time_ms,
        result.cpu_time_ms,
        result.entities_per_sec
    );

    if (thread_count > 1) {
        printf(" | %d ent/thread", entity_count / thread_count);
    }
    if (baseline_wall_ms > 0.0) {
        printf(" | speedup %.2fx", baseline_wall_ms / result.wall_time_ms);
    }
    if (thread_count == 1) {
        printf(" | setup %.3f ms (excluded)", result.setup_ms);
    }
    printf("\n");

    return result;
}

int main(void)
{
    // Single-threaded benchmarks
    num_threads = 1;
    use_tpool = 0;
    run_benchmarks("single-threaded");

    // Multi-threaded benchmarks
    num_threads = 14;
    use_tpool = 1;
    run_benchmarks("multi-threaded, 14 threads");
    return 0;
}
