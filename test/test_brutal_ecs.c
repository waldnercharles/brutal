#include "brutal_ecs.h"
#include "brutal_tpool.h"
#include "pico_unit.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>

typedef struct
{
    int x;
    int y;
} Position;

typedef struct
{
    int vx;
    int vy;
} Velocity;

typedef struct
{
    char name[32];
} Name;

typedef struct
{
    float health;
} Health;

TEST_CASE(test_ecs_new_free)
{
    ecs_t *ecs = ecs_new();
    REQUIRE(ecs != NULL);

    ecs_free(ecs);
    return true;
}

TEST_CASE(test_entity_create_destroy)
{
    ecs_t *ecs = ecs_new();

    ecs_entity e1 = ecs_create(ecs);
    ecs_entity e2 = ecs_create(ecs);

    REQUIRE(e1 != e2);

    ecs_destroy(ecs, e1);

    ecs_entity e_recycled = ecs_create(ecs);
    REQUIRE(e_recycled == e1);

    ecs_free(ecs);
    return true;
}

// ---- Component Tests ----

TEST_CASE(test_register_component)
{
    ecs_t *ecs = ecs_new();

    ecs_comp_t pos_comp = ECS_COMPONENT(ecs, Position);
    ecs_comp_t vel_comp = ECS_COMPONENT(ecs, Velocity);

    REQUIRE(pos_comp != vel_comp);

    ecs_free(ecs);
    return true;
}

TEST_CASE(test_add_get_component)
{
    ecs_t *ecs = ecs_new();

    ecs_comp_t pos_comp = ECS_COMPONENT(ecs, Position);
    ecs_entity e = ecs_create(ecs);

    Position *pos = (Position *)ecs_add(ecs, e, pos_comp);
    REQUIRE(pos != NULL);

    pos->x = 10;
    pos->y = 20;

    Position *retrieved = (Position *)ecs_get(ecs, e, pos_comp);
    REQUIRE(retrieved != NULL);
    REQUIRE(retrieved->x == 10);
    REQUIRE(retrieved->y == 20);

    ecs_free(ecs);
    return true;
}

TEST_CASE(test_has_component)
{
    ecs_t *ecs = ecs_new();

    ecs_comp_t pos_comp = ECS_COMPONENT(ecs, Position);
    ecs_comp_t vel_comp = ECS_COMPONENT(ecs, Velocity);
    ecs_entity e = ecs_create(ecs);

    REQUIRE(!ecs_has(ecs, e, pos_comp));

    ecs_add(ecs, e, pos_comp);
    REQUIRE(ecs_has(ecs, e, pos_comp));
    REQUIRE(!ecs_has(ecs, e, vel_comp));

    ecs_free(ecs);
    return true;
}

TEST_CASE(test_remove_component)
{
    ecs_t *ecs = ecs_new();

    ecs_comp_t pos_comp = ECS_COMPONENT(ecs, Position);
    ecs_entity e = ecs_create(ecs);

    ecs_add(ecs, e, pos_comp);
    REQUIRE(ecs_has(ecs, e, pos_comp));

    ecs_remove(ecs, e, pos_comp);
    REQUIRE(!ecs_has(ecs, e, pos_comp));

    ecs_free(ecs);
    return true;
}

TEST_CASE(test_multiple_components_per_entity)
{
    ecs_t *ecs = ecs_new();

    ecs_comp_t pos_comp = ECS_COMPONENT(ecs, Position);
    ecs_comp_t vel_comp = ECS_COMPONENT(ecs, Velocity);
    ecs_entity e = ecs_create(ecs);

    Position *pos = (Position *)ecs_add(ecs, e, pos_comp);
    pos->x = 100;
    pos->y = 200;

    Velocity *vel = (Velocity *)ecs_add(ecs, e, vel_comp);
    vel->vx = 5;
    vel->vy = 10;

    REQUIRE(ecs_has(ecs, e, pos_comp));
    REQUIRE(ecs_has(ecs, e, vel_comp));

    Position *pos_get = (Position *)ecs_get(ecs, e, pos_comp);
    Velocity *vel_get = (Velocity *)ecs_get(ecs, e, vel_comp);

    REQUIRE(pos_get->x == 100);
    REQUIRE(vel_get->vx == 5);

    ecs_free(ecs);
    return true;
}

static int test_system_call_count = 0;

static int test_system_fn(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)ecs;
    (void)udata;
    test_system_call_count += view->count;
    return 0;
}

TEST_CASE(test_add_system)
{
    ecs_t *ecs = ecs_new();

    ecs_comp_t pos_comp = ECS_COMPONENT(ecs, Position);

    ecs_sys_t sys = ecs_sys_create(ecs, test_system_fn, NULL);
    ecs_sys_require(ecs, sys, pos_comp);

    REQUIRE(sys >= 0);

    ecs_free(ecs);
    return true;
}

TEST_CASE(test_system_execution)
{
    ecs_t *ecs = ecs_new();

    ecs_comp_t pos_comp = ECS_COMPONENT(ecs, Position);

    ecs_entity e1 = ecs_create(ecs);
    ecs_entity e2 = ecs_create(ecs);
    (void)ecs_create(ecs);

    ecs_add(ecs, e1, pos_comp);
    ecs_add(ecs, e2, pos_comp);

    ecs_sys_t sys = ecs_sys_create(ecs, test_system_fn, NULL);
    ecs_sys_require(ecs, sys, pos_comp);

    test_system_call_count = 0;
    ecs_progress(ecs, 0);

    REQUIRE(test_system_call_count == 2);

    ecs_free(ecs);
    return true;
}

static int velocity_system_fn(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;
    ecs_comp_t pos_comp = 0;
    ecs_comp_t vel_comp = 1;

    // Iterate entities and use ecs_get() for in-place modification
    for (int i = 0; i < view->count; i++) {
        ecs_entity e = view->entities[i];
        Position *pos = (Position *)ecs_get(ecs, e, pos_comp);
        Velocity *vel = (Velocity *)ecs_get(ecs, e, vel_comp);

        if (pos && vel) {
            pos->x += vel->vx;
            pos->y += vel->vy;
        }
    }

    return 0;
}

TEST_CASE(test_system_with_query)
{
    ecs_t *ecs = ecs_new();

    ecs_comp_t pos_comp = ECS_COMPONENT(ecs, Position);
    ecs_comp_t vel_comp = ECS_COMPONENT(ecs, Velocity);

    ecs_entity e = ecs_create(ecs);
    Position *pos = (Position *)ecs_add(ecs, e, pos_comp);
    Velocity *vel = (Velocity *)ecs_add(ecs, e, vel_comp);

    pos->x = 10;
    pos->y = 20;
    vel->vx = 5;
    vel->vy = 3;

    ecs_sys_t sys = ecs_sys_create(ecs, velocity_system_fn, NULL);
    ecs_sys_require(ecs, sys, pos_comp);
    ecs_sys_require(ecs, sys, vel_comp);

    ecs_progress(ecs, 0);

    Position *updated_pos = (Position *)ecs_get(ecs, e, pos_comp);
    REQUIRE(updated_pos->x == 15);
    REQUIRE(updated_pos->y == 23);

    ecs_free(ecs);
    return true;
}

TEST_CASE(test_system_none_of_filter)
{
    ecs_t *ecs = ecs_new();

    ecs_comp_t pos_comp = ECS_COMPONENT(ecs, Position);
    ecs_comp_t vel_comp = ECS_COMPONENT(ecs, Velocity);

    ecs_entity e1 = ecs_create(ecs);
    ecs_entity e2 = ecs_create(ecs);

    ecs_add(ecs, e1, pos_comp);
    ecs_add(ecs, e2, pos_comp);
    ecs_add(ecs, e2, vel_comp);

    ecs_sys_t sys = ecs_sys_create(ecs, test_system_fn, NULL);
    ecs_sys_require(ecs, sys, pos_comp);
    ecs_sys_exclude(ecs, sys, vel_comp);

    test_system_call_count = 0;
    ecs_progress(ecs, 0);

    REQUIRE(test_system_call_count == 1);

    ecs_free(ecs);
    return true;
}

static int group_a_counter = 0;
static int group_b_counter = 0;
static int group_default_counter = 0;

static int group_a_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)ecs;
    (void)udata;
    group_a_counter += view->count;
    return 0;
}

static int group_b_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)ecs;
    (void)udata;
    group_b_counter += view->count;
    return 0;
}

static int group_default_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)ecs;
    (void)udata;
    group_default_counter += view->count;
    return 0;
}

TEST_CASE(test_selective_group_execution)
{
    ecs_t *ecs = ecs_new();

    ecs_comp_t pos_comp = ECS_COMPONENT(ecs, Position);

    // Create test entities
    for (int i = 0; i < 10; i++) {
        ecs_entity e = ecs_create(ecs);
        ecs_add(ecs, e, pos_comp);
    }

// Define group constants
#define GROUP_A 1
#define GROUP_B 2

    // Register systems in different groups
    ecs_sys_t sys_a = ecs_sys_create(ecs, group_a_system, NULL);
    ecs_sys_require(ecs, sys_a, pos_comp);
    ecs_sys_set_group(ecs, sys_a, GROUP_A);

    ecs_sys_t sys_b = ecs_sys_create(ecs, group_b_system, NULL);
    ecs_sys_require(ecs, sys_b, pos_comp);
    ecs_sys_set_group(ecs, sys_b, GROUP_B);

    ecs_sys_t sys_default = ecs_sys_create(ecs, group_default_system, NULL);
    ecs_sys_require(ecs, sys_default, pos_comp);
    // group=0 is default, no need to set

    // Reset counters
    group_a_counter = 0;
    group_b_counter = 0;
    group_default_counter = 0;

    // Test 1: Run only group A
    ecs_progress(ecs, GROUP_A);
    REQUIRE(group_a_counter == 10);
    REQUIRE(group_b_counter == 0);
    REQUIRE(group_default_counter == 0);

    // Reset counters
    group_a_counter = 0;
    group_b_counter = 0;
    group_default_counter = 0;

    // Test 2: Run only group B
    ecs_progress(ecs, GROUP_B);
    REQUIRE(group_a_counter == 0);
    REQUIRE(group_b_counter == 10);
    REQUIRE(group_default_counter == 0);

    // Reset counters
    group_a_counter = 0;
    group_b_counter = 0;
    group_default_counter = 0;

    // Test 3: Run both groups A and B
    ecs_progress(ecs, GROUP_A | GROUP_B);
    REQUIRE(group_a_counter == 10);
    REQUIRE(group_b_counter == 10);
    REQUIRE(group_default_counter == 0);

    // Reset counters
    group_a_counter = 0;
    group_b_counter = 0;
    group_default_counter = 0;

    // Test 4: Run default group only
    ecs_progress(ecs, 0);
    REQUIRE(group_a_counter == 0);
    REQUIRE(group_b_counter == 0);
    REQUIRE(group_default_counter == 10);

    ecs_free(ecs);
    return true;

#undef GROUP_A
#undef GROUP_B
}

typedef struct
{
    ecs_comp_t vel_comp;
    int added;
} add_velocity_state;

typedef struct
{
    ecs_comp_t vel_comp;
    int seen;
} consume_velocity_state;

static int add_velocity_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    add_velocity_state *state = (add_velocity_state *)udata;

    for (int i = 0; i < view->count; i++) {
        ecs_entity e = view->entities[i];
        if (ecs_has(ecs, e, state->vel_comp)) continue;

        Velocity *vel = (Velocity *)ecs_add(ecs, e, state->vel_comp);
        vel->vx = 3;
        vel->vy = 7;
        state->added++;
    }

    return 0;
}

static int consume_velocity_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    consume_velocity_state *state = (consume_velocity_state *)udata;

    for (int i = 0; i < view->count; i++) {
        ecs_entity e = view->entities[i];
        Velocity *vel = (Velocity *)ecs_get(ecs, e, state->vel_comp);
        if (vel && vel->vx == 3 && vel->vy == 7) state->seen++;
    }

    return 0;
}

TEST_CASE(test_stage_sync_applies_deferred_adds)
{
    ecs_t *ecs = ecs_new();

    ecs_comp_t pos_comp = ECS_COMPONENT(ecs, Position);
    ecs_comp_t vel_comp = ECS_COMPONENT(ecs, Velocity);

    for (int i = 0; i < 8; i++) {
        ecs_entity e = ecs_create(ecs);
        Position *pos = (Position *)ecs_add(ecs, e, pos_comp);
        pos->x = i;
        pos->y = i;
    }

    add_velocity_state add_state = { .vel_comp = vel_comp, .added = 0 };
    consume_velocity_state consume_state = { .vel_comp = vel_comp, .seen = 0 };

    ecs_sys_t add_sys = ecs_sys_create(ecs, add_velocity_system, &add_state);
    ecs_sys_require(ecs, add_sys, pos_comp);
    ecs_sys_exclude(ecs, add_sys, vel_comp);

    ecs_sys_t consume_sys = ecs_sys_create(ecs, consume_velocity_system, &consume_state);
    ecs_sys_require(ecs, consume_sys, pos_comp);
    ecs_sys_require(ecs, consume_sys, vel_comp);

    ecs_progress(ecs, 0);
    REQUIRE(add_state.added == 8);
    REQUIRE(consume_state.seen == 8);

    add_state.added = 0;
    consume_state.seen = 0;
    ecs_progress(ecs, 0);
    REQUIRE(add_state.added == 0);
    REQUIRE(consume_state.seen == 8);

    ecs_free(ecs);
    return true;
}

typedef struct
{
    int callback_count;
} udata_state;

static int udata_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)ecs;
    udata_state *state = (udata_state *)udata;
    state->callback_count += view->count;
    return 0;
}

TEST_CASE(test_system_udata_roundtrip)
{
    ecs_t *ecs = ecs_new();

    ecs_comp_t pos_comp = ECS_COMPONENT(ecs, Position);
    for (int i = 0; i < 3; i++) {
        ecs_entity e = ecs_create(ecs);
        ecs_add(ecs, e, pos_comp);
    }

    udata_state state = { .callback_count = 0 };
    ecs_sys_t sys = ecs_sys_create(ecs, udata_system, &state);
    ecs_sys_require(ecs, sys, pos_comp);

    ecs_progress(ecs, 0);
    REQUIRE(state.callback_count == 3);

    ecs_free(ecs);
    return true;
}

// ---- Multithreading Tests ----

static tpool_t *g_tpool = NULL;

static int tpool_enqueue_adapter(int (*fn)(void *), void *args, void *udata)
{
    (void)udata;
    tpool_enqueue(g_tpool, fn, args);
    return 0;
}

static void tpool_wait_adapter(void *udata)
{
    (void)udata;
    tpool_wait(g_tpool);
}

static _Atomic int mt_system_calls = 0;
static _Atomic int mt_entity_count = 0;

static int mt_move_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)udata;
    ecs_comp_t pos_comp = 0;
    ecs_comp_t vel_comp = 1;

    for (int i = 0; i < view->count; i++) {
        ecs_entity e = view->entities[i];
        Position *pos = (Position *)ecs_get(ecs, e, pos_comp);
        Velocity *vel = (Velocity *)ecs_get(ecs, e, vel_comp);

        if (pos && vel) {
            pos->x += vel->vx;
            pos->y += vel->vy;
        }
        atomic_fetch_add(&mt_entity_count, 1);
    }

    atomic_fetch_add(&mt_system_calls, 1);
    return 0;
}

TEST_CASE(test_multithreading_basic)
{
    const int NUM_THREADS = 4;
    const int NUM_ENTITIES = 1000;

    // Create thread pool
    g_tpool = tpool_new(NUM_THREADS, 0);
    REQUIRE(g_tpool != NULL);

    // Create ECS
    ecs_t *ecs = ecs_new();
    REQUIRE(ecs != NULL);

    // Set up task callbacks for multithreading
    ecs_set_task_callbacks(ecs, tpool_enqueue_adapter, tpool_wait_adapter, NULL, NUM_THREADS);

    // Register components
    ecs_comp_t pos_comp = ECS_COMPONENT(ecs, Position);
    ecs_comp_t vel_comp = ECS_COMPONENT(ecs, Velocity);

    // Create entities
    for (int i = 0; i < NUM_ENTITIES; i++) {
        ecs_entity e = ecs_create(ecs);
        Position *pos = (Position *)ecs_add(ecs, e, pos_comp);
        Velocity *vel = (Velocity *)ecs_add(ecs, e, vel_comp);
        pos->x = i;
        pos->y = i * 2;
        vel->vx = 1;
        vel->vy = 2;
    }

    // Create system
    ecs_sys_t sys = ecs_sys_create(ecs, mt_move_system, NULL);
    ecs_sys_require(ecs, sys, pos_comp);
    ecs_sys_require(ecs, sys, vel_comp);
    ecs_sys_set_parallel(ecs, sys, true);

    // Reset counters
    atomic_store(&mt_system_calls, 0);
    atomic_store(&mt_entity_count, 0);

    // Run ECS - should use multiple threads
    ecs_progress(ecs, 0);

    int calls = atomic_load(&mt_system_calls);
    int entities = atomic_load(&mt_entity_count);

    // The system should be called NUM_THREADS times (once per task)
    REQUIRE(calls == NUM_THREADS);
    // All entities should be processed
    REQUIRE(entities == NUM_ENTITIES);

    // Cleanup
    ecs_free(ecs);
    tpool_destroy(g_tpool);
    g_tpool = NULL;

    return true;
}

TEST_CASE(test_multithreading_verify_parallel_execution)
{
    const int NUM_THREADS = 4;
    const int NUM_ENTITIES = 10000;

    g_tpool = tpool_new(NUM_THREADS, 0);
    REQUIRE(g_tpool != NULL);

    ecs_t *ecs = ecs_new();
    ecs_set_task_callbacks(ecs, tpool_enqueue_adapter, tpool_wait_adapter, NULL, NUM_THREADS);

    ecs_comp_t pos_comp = ECS_COMPONENT(ecs, Position);
    ecs_comp_t vel_comp = ECS_COMPONENT(ecs, Velocity);

    for (int i = 0; i < NUM_ENTITIES; i++) {
        ecs_entity e = ecs_create(ecs);
        Position *pos = (Position *)ecs_add(ecs, e, pos_comp);
        Velocity *vel = (Velocity *)ecs_add(ecs, e, vel_comp);
        pos->x = (float)i;
        pos->y = (float)i;
        vel->vx = 1;
        vel->vy = 1;
    }

    ecs_sys_t sys = ecs_sys_create(ecs, mt_move_system, NULL);
    ecs_sys_require(ecs, sys, pos_comp);
    ecs_sys_require(ecs, sys, vel_comp);
    ecs_sys_set_parallel(ecs, sys, true);

    atomic_store(&mt_system_calls, 0);
    atomic_store(&mt_entity_count, 0);

    ecs_progress(ecs, 0);

    int first_entities = atomic_load(&mt_entity_count);
    REQUIRE(first_entities == NUM_ENTITIES);

    atomic_store(&mt_system_calls, 0);
    atomic_store(&mt_entity_count, 0);

    ecs_progress(ecs, 0);

    int second_entities = atomic_load(&mt_entity_count);
    REQUIRE(second_entities == NUM_ENTITIES);

    ecs_free(ecs);
    tpool_destroy(g_tpool);
    g_tpool = NULL;

    return true;
}

// ---- Hybrid System+Entity Parallelism Tests ----

static _Atomic int mt_sys1_calls = 0;
static _Atomic int mt_sys2_calls = 0;
static _Atomic int mt_sys1_entities = 0;
static _Atomic int mt_sys2_entities = 0;

static int mt_independent_sys1(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)ecs;
    (void)udata;
    atomic_fetch_add(&mt_sys1_calls, 1);
    atomic_fetch_add(&mt_sys1_entities, view->count);
    return 0;
}

static int mt_independent_sys2(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)ecs;
    (void)udata;
    atomic_fetch_add(&mt_sys2_calls, 1);
    atomic_fetch_add(&mt_sys2_entities, view->count);
    return 0;
}

TEST_CASE(test_mt_independent_systems_parallel)
{
    const int NUM_THREADS = 4;
    const int NUM_ENTITIES = 1000;

    g_tpool = tpool_new(NUM_THREADS, 0);
    REQUIRE(g_tpool != NULL);

    ecs_t *ecs = ecs_new();
    ecs_set_task_callbacks(ecs, tpool_enqueue_adapter, tpool_wait_adapter, NULL, NUM_THREADS);

    ecs_comp_t pos_comp = ECS_COMPONENT(ecs, Position);

    for (int i = 0; i < NUM_ENTITIES; i++) {
        ecs_entity e = ecs_create(ecs);
        ecs_add(ecs, e, pos_comp);
    }

    // Two read-only systems
    ecs_sys_t sys1 = ecs_sys_create(ecs, mt_independent_sys1, NULL);
    ecs_sys_require(ecs, sys1, pos_comp);
    ecs_sys_set_parallel(ecs, sys1, true);

    ecs_sys_t sys2 = ecs_sys_create(ecs, mt_independent_sys2, NULL);
    ecs_sys_require(ecs, sys2, pos_comp);
    ecs_sys_set_parallel(ecs, sys2, true);

    atomic_store(&mt_sys1_calls, 0);
    atomic_store(&mt_sys2_calls, 0);
    atomic_store(&mt_sys1_entities, 0);
    atomic_store(&mt_sys2_entities, 0);

    ecs_progress(ecs, 0);

    // Both systems should see all entities
    int sys1_entities = atomic_load(&mt_sys1_entities);
    int sys2_entities = atomic_load(&mt_sys2_entities);
    REQUIRE(sys1_entities == NUM_ENTITIES);
    REQUIRE(sys2_entities == NUM_ENTITIES);

    // Both systems should run (may be called multiple times due to entity slicing)
    int sys1_calls = atomic_load(&mt_sys1_calls);
    int sys2_calls = atomic_load(&mt_sys2_calls);
    REQUIRE(sys1_calls > 0);
    REQUIRE(sys2_calls > 0);

    ecs_free(ecs);
    tpool_destroy(g_tpool);
    g_tpool = NULL;

    return true;
}

static _Atomic int mt_writer_ran = 0;
static _Atomic int mt_reader_saw_adds = 0;

static int mt_writer_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    ecs_comp_t *vel_comp = (ecs_comp_t *)udata;
    for (int i = 0; i < view->count; i++) {
        ecs_entity e = view->entities[i];
        ecs_add(ecs, e, *vel_comp);
    }
    atomic_store(&mt_writer_ran, 1);
    return 0;
}

static int mt_reader_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)ecs;
    (void)udata;
    if (view->count > 0) {
        atomic_store(&mt_reader_saw_adds, 1);
    }
    return 0;
}

TEST_CASE(test_mt_conflicting_systems_staged)
{
    const int NUM_THREADS = 4;
    const int NUM_ENTITIES = 100;

    g_tpool = tpool_new(NUM_THREADS, 0);
    REQUIRE(g_tpool != NULL);

    ecs_t *ecs = ecs_new();
    ecs_set_task_callbacks(ecs, tpool_enqueue_adapter, tpool_wait_adapter, NULL, NUM_THREADS);

    ecs_comp_t pos_comp = ECS_COMPONENT(ecs, Position);
    ecs_comp_t vel_comp = ECS_COMPONENT(ecs, Velocity);

    for (int i = 0; i < NUM_ENTITIES; i++) {
        ecs_entity e = ecs_create(ecs);
        ecs_add(ecs, e, pos_comp);
    }

    // Writer system adds vel_comp (writes to structural data)
    ecs_sys_t writer = ecs_sys_create(ecs, mt_writer_system, &vel_comp);
    ecs_sys_require(ecs, writer, pos_comp);
    ecs_sys_set_parallel(ecs, writer, true);

    // Reader system requires vel_comp
    ecs_sys_t reader = ecs_sys_create(ecs, mt_reader_system, NULL);
    ecs_sys_require(ecs, reader, vel_comp);
    ecs_sys_set_parallel(ecs, reader, true);

    atomic_store(&mt_writer_ran, 0);
    atomic_store(&mt_reader_saw_adds, 0);

    ecs_progress(ecs, 0);

    // Writer should run
    REQUIRE(atomic_load(&mt_writer_ran) == 1);
    // Reader should see the deferred adds after stage sync
    REQUIRE(atomic_load(&mt_reader_saw_adds) == 1);

    ecs_free(ecs);
    tpool_destroy(g_tpool);
    g_tpool = NULL;

    return true;
}

static _Atomic int mt_many_systems_total = 0;

static int mt_many_reader_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)ecs;
    (void)udata;
    atomic_fetch_add(&mt_many_systems_total, view->count);
    return 0;
}

TEST_CASE(test_mt_many_systems_batching)
{
    const int NUM_THREADS = 4;
    const int NUM_SYSTEMS = 20;
    const int NUM_ENTITIES = 100;

    g_tpool = tpool_new(NUM_THREADS, 0);
    REQUIRE(g_tpool != NULL);

    ecs_t *ecs = ecs_new();
    ecs_set_task_callbacks(ecs, tpool_enqueue_adapter, tpool_wait_adapter, NULL, NUM_THREADS);

    ecs_comp_t pos_comp = ECS_COMPONENT(ecs, Position);

    for (int i = 0; i < NUM_ENTITIES; i++) {
        ecs_entity e = ecs_create(ecs);
        ecs_add(ecs, e, pos_comp);
    }

    // Create many read-only systems
    for (int i = 0; i < NUM_SYSTEMS; i++) {
        ecs_sys_t sys = ecs_sys_create(ecs, mt_many_reader_system, NULL);
        ecs_sys_require(ecs, sys, pos_comp);
        ecs_sys_set_parallel(ecs, sys, true);
    }

    atomic_store(&mt_many_systems_total, 0);

    ecs_progress(ecs, 0);

    // All systems should process all entities
    int total = atomic_load(&mt_many_systems_total);
    REQUIRE(total == NUM_SYSTEMS * NUM_ENTITIES);

    ecs_free(ecs);
    tpool_destroy(g_tpool);
    g_tpool = NULL;

    return true;
}

// ---- Test Suite ----

TEST_SUITE(ecs_suite)
{
    RUN_TEST_CASE(test_ecs_new_free);
    RUN_TEST_CASE(test_entity_create_destroy);

    RUN_TEST_CASE(test_register_component);
    RUN_TEST_CASE(test_add_get_component);
    RUN_TEST_CASE(test_has_component);
    RUN_TEST_CASE(test_remove_component);
    RUN_TEST_CASE(test_multiple_components_per_entity);

    RUN_TEST_CASE(test_add_system);
    RUN_TEST_CASE(test_system_execution);
    RUN_TEST_CASE(test_system_with_query);
    RUN_TEST_CASE(test_system_none_of_filter);
    RUN_TEST_CASE(test_selective_group_execution);
    RUN_TEST_CASE(test_stage_sync_applies_deferred_adds);
    RUN_TEST_CASE(test_system_udata_roundtrip);

    RUN_TEST_CASE(test_multithreading_basic);
    RUN_TEST_CASE(test_multithreading_verify_parallel_execution);

    RUN_TEST_CASE(test_mt_independent_systems_parallel);
    RUN_TEST_CASE(test_mt_conflicting_systems_staged);
    RUN_TEST_CASE(test_mt_many_systems_batching);
}
