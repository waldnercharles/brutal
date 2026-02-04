#include "brutal_ecs.h"
#include "pico_unit.h"

#define TPOOL_IMPLEMENTATION
#include "tpool.h"

#include <pthread.h>
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

TEST_CASE(test_bitset_zero_and_any)
{
    ecs_bitset bs;
    ecs_bs_zero(&bs);

    REQUIRE(ecs_bs_none(&bs));
    REQUIRE(!ecs_bs_any(&bs));

    ecs_bs_set(&bs, 0);
    REQUIRE(ecs_bs_any(&bs));
    REQUIRE(!ecs_bs_none(&bs));

    return true;
}

TEST_CASE(test_bitset_set_clear_test)
{
    ecs_bitset bs;
    ecs_bs_zero(&bs);

    ecs_bs_set(&bs, 5);
    REQUIRE(ecs_bs_test(&bs, 5));
    REQUIRE(!ecs_bs_test(&bs, 4));
    REQUIRE(!ecs_bs_test(&bs, 6));

    ecs_bs_clear(&bs, 5);
    REQUIRE(!ecs_bs_test(&bs, 5));

    return true;
}

TEST_CASE(test_bitset_operations)
{
    ecs_bitset a, b, result;
    ecs_bs_zero(&a);
    ecs_bs_zero(&b);
    ecs_bs_zero(&result);

    ecs_bs_set(&a, 1);
    ecs_bs_set(&a, 3);
    ecs_bs_set(&b, 2);
    ecs_bs_set(&b, 3);

    ecs_bs_or(&result, &a, &b);
    REQUIRE(ecs_bs_test(&result, 1));
    REQUIRE(ecs_bs_test(&result, 2));
    REQUIRE(ecs_bs_test(&result, 3));
    REQUIRE(!ecs_bs_test(&result, 0));

    ecs_bs_and(&result, &a, &b);
    REQUIRE(ecs_bs_test(&result, 3));
    REQUIRE(!ecs_bs_test(&result, 1));
    REQUIRE(!ecs_bs_test(&result, 2));

    ecs_bs_andnot(&result, &a, &b);
    REQUIRE(ecs_bs_test(&result, 1));
    REQUIRE(!ecs_bs_test(&result, 2));
    REQUIRE(!ecs_bs_test(&result, 3));

    return true;
}

TEST_CASE(test_bitset_intersects)
{
    ecs_bitset a, b;
    ecs_bs_zero(&a);
    ecs_bs_zero(&b);

    ecs_bs_set(&a, 5);
    ecs_bs_set(&b, 10);
    REQUIRE(!ecs_bs_intersects(&a, &b));

    ecs_bs_set(&b, 5);
    REQUIRE(ecs_bs_intersects(&a, &b));

    return true;
}

// ---- Sparse Set Tests ----

TEST_CASE(test_sparse_set_basic)
{
    ecs_sparse_set set;
    ecs_ss_init(&set);

    REQUIRE(set.count == 0);
    REQUIRE(!ecs_ss_has(&set, 0));

    ecs_ss_insert(&set, 10);
    REQUIRE(ecs_ss_has(&set, 10));
    REQUIRE(set.count == 1);

    ecs_ss_remove(&set, 10);
    REQUIRE(!ecs_ss_has(&set, 10));
    REQUIRE(set.count == 0);

    ecs_ss_free(&set);
    return true;
}

TEST_CASE(test_sparse_set_multiple)
{
    ecs_sparse_set set;
    ecs_ss_init(&set);

    ecs_ss_insert(&set, 5);
    ecs_ss_insert(&set, 10);
    ecs_ss_insert(&set, 15);

    REQUIRE(set.count == 3);
    REQUIRE(ecs_ss_has(&set, 5));
    REQUIRE(ecs_ss_has(&set, 10));
    REQUIRE(ecs_ss_has(&set, 15));
    REQUIRE(!ecs_ss_has(&set, 7));

    ecs_ss_remove(&set, 10);
    REQUIRE(set.count == 2);
    REQUIRE(ecs_ss_has(&set, 5));
    REQUIRE(!ecs_ss_has(&set, 10));
    REQUIRE(ecs_ss_has(&set, 15));

    ecs_ss_free(&set);
    return true;
}

// ---- ECS Entity Tests ----

TEST_CASE(test_ecs_new_free)
{
    ecs_t *ecs = ecs_new();

    REQUIRE(ecs->next_entity == 1);
    REQUIRE(ecs->comp_count == 0);
    REQUIRE(ecs->system_count == 0);

    ecs_free(ecs);
    return true;
}

TEST_CASE(test_entity_create_destroy)
{
    ecs_t *ecs = ecs_new();

    ecs_entity e1 = ecs_create(ecs);
    ecs_entity e2 = ecs_create(ecs);

    REQUIRE(e1 == 1);
    REQUIRE(e2 == 2);

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

    REQUIRE(pos_comp == 0);
    REQUIRE(vel_comp == 1);
    REQUIRE(ecs->comp_count == 2);

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

    REQUIRE(sys == 0);
    REQUIRE(ecs->system_count == 1);

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
    ecs_sys_read(ecs, sys, vel_comp);
    ecs_sys_write(ecs, sys, pos_comp);

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

static int phase_a_counter = 0;
static int phase_b_counter = 0;
static int phase_default_counter = 0;

static int phase_a_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)ecs;
    (void)udata;
    phase_a_counter += view->count;
    return 0;
}

static int phase_b_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)ecs;
    (void)udata;
    phase_b_counter += view->count;
    return 0;
}

static int phase_default_system(ecs_t *ecs, ecs_view *view, void *udata)
{
    (void)ecs;
    (void)udata;
    phase_default_counter += view->count;
    return 0;
}

TEST_CASE(test_selective_phase_execution)
{
    ecs_t *ecs = ecs_new();

    ecs_comp_t pos_comp = ECS_COMPONENT(ecs, Position);

    // Create test entities
    for (int i = 0; i < 10; i++) {
        ecs_entity e = ecs_create(ecs);
        ecs_add(ecs, e, pos_comp);
    }

// Define phase constants
#define PHASE_A 1
#define PHASE_B 2

    // Register systems in different phases
    ecs_sys_t sys_a = ecs_sys_create(ecs, phase_a_system, NULL);
    ecs_sys_require(ecs, sys_a, pos_comp);
    ecs_sys_set_phase(ecs, sys_a, PHASE_A);

    ecs_sys_t sys_b = ecs_sys_create(ecs, phase_b_system, NULL);
    ecs_sys_require(ecs, sys_b, pos_comp);
    ecs_sys_set_phase(ecs, sys_b, PHASE_B);

    ecs_sys_t sys_default = ecs_sys_create(ecs, phase_default_system, NULL);
    ecs_sys_require(ecs, sys_default, pos_comp);
    // phase=0 is default, no need to set

    // Reset counters
    phase_a_counter = 0;
    phase_b_counter = 0;
    phase_default_counter = 0;

    // Test 1: Run only phase A
    ecs_progress(ecs, PHASE_A);
    REQUIRE(phase_a_counter == 10);
    REQUIRE(phase_b_counter == 0);
    REQUIRE(phase_default_counter == 0);

    // Reset counters
    phase_a_counter = 0;
    phase_b_counter = 0;
    phase_default_counter = 0;

    // Test 2: Run only phase B
    ecs_progress(ecs, PHASE_B);
    REQUIRE(phase_a_counter == 0);
    REQUIRE(phase_b_counter == 10);
    REQUIRE(phase_default_counter == 0);

    // Reset counters
    phase_a_counter = 0;
    phase_b_counter = 0;
    phase_default_counter = 0;

    // Test 3: Run both phases A and B
    ecs_progress(ecs, PHASE_A | PHASE_B);
    REQUIRE(phase_a_counter == 10);
    REQUIRE(phase_b_counter == 10);
    REQUIRE(phase_default_counter == 0);

    // Reset counters
    phase_a_counter = 0;
    phase_b_counter = 0;
    phase_default_counter = 0;

    // Test 4: Run default phase only
    ecs_progress(ecs, 0);
    REQUIRE(phase_a_counter == 0);
    REQUIRE(phase_b_counter == 0);
    REQUIRE(phase_default_counter == 10);

    ecs_free(ecs);
    return true;

#undef PHASE_A
#undef PHASE_B
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
    (void)ecs;

    for (int i = 0; i < view->count; i++) {
        ecs_entity e = view->entities[i];
        Velocity *vel = (Velocity *)ecs_get(view->ecs, e, state->vel_comp);
        if (vel && vel->vx == 3 && vel->vy == 7) state->seen++;
    }

    return 0;
}

TEST_CASE(test_phase_sync_applies_deferred_adds)
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
    ecs_sys_write(ecs, add_sys, vel_comp);

    ecs_sys_t consume_sys = ecs_sys_create(ecs, consume_velocity_system, &consume_state);
    ecs_sys_require(ecs, consume_sys, pos_comp);
    ecs_sys_require(ecs, consume_sys, vel_comp);
    ecs_sys_read(ecs, consume_sys, vel_comp);

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
    return tpool_add_work(g_tpool, fn, args);
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

    pthread_t tid = pthread_self();
    printf("[Thread %p] Processing %d entities (task_index from TLS)\n", (void *)tid, view->count);

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
    g_tpool = tpool_create(NUM_THREADS);
    REQUIRE(g_tpool != NULL);

    // Create ECS
    ecs_t *ecs = ecs_new();
    REQUIRE(ecs != NULL);

    // Set up task callbacks for multithreading
    ecs_set_task_callbacks(ecs, tpool_enqueue_adapter, tpool_wait_adapter, NULL, NUM_THREADS);

    // Register components
    ecs_comp_t pos_comp = ECS_COMPONENT(ecs, Position);
    ecs_comp_t vel_comp = ECS_COMPONENT(ecs, Velocity);

    printf("\n--- Multithreading Test ---\n");
    printf("Thread pool: %d threads\n", NUM_THREADS);
    printf("Creating %d entities...\n", NUM_ENTITIES);

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
    ecs_sys_write(ecs, sys, pos_comp);
    ecs_sys_read(ecs, sys, vel_comp);

    // Reset counters
    atomic_store(&mt_system_calls, 0);
    atomic_store(&mt_entity_count, 0);

    printf("Running system with multithreading...\n");

    // Run ECS - should use multiple threads
    ecs_progress(ecs, 0);

    int calls = atomic_load(&mt_system_calls);
    int entities = atomic_load(&mt_entity_count);

    printf("System was called %d times (expected: %d for %d threads)\n", calls, NUM_THREADS, NUM_THREADS);
    printf("Total entities processed: %d (expected: %d)\n", entities, NUM_ENTITIES);

    // The system should be called NUM_THREADS times (once per task)
    REQUIRE(calls == NUM_THREADS);
    // All entities should be processed
    REQUIRE(entities == NUM_ENTITIES);

    printf("--- Multithreading Test PASSED ---\n\n");

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

    g_tpool = tpool_create(NUM_THREADS);
    REQUIRE(g_tpool != NULL);

    ecs_t *ecs = ecs_new();
    ecs_set_task_callbacks(ecs, tpool_enqueue_adapter, tpool_wait_adapter, NULL, NUM_THREADS);

    ecs_comp_t pos_comp = ECS_COMPONENT(ecs, Position);
    ecs_comp_t vel_comp = ECS_COMPONENT(ecs, Velocity);

    printf("\n--- Parallel Execution Verification ---\n");
    printf("Creating %d entities with %d threads...\n", NUM_ENTITIES, NUM_THREADS);

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
    ecs_sys_write(ecs, sys, pos_comp);
    ecs_sys_read(ecs, sys, vel_comp);

    atomic_store(&mt_system_calls, 0);
    atomic_store(&mt_entity_count, 0);

    printf("Progress iteration 1:\n");
    ecs_progress(ecs, 0);

    int first_entities = atomic_load(&mt_entity_count);
    REQUIRE(first_entities == NUM_ENTITIES);

    atomic_store(&mt_system_calls, 0);
    atomic_store(&mt_entity_count, 0);

    printf("Progress iteration 2:\n");
    ecs_progress(ecs, 0);

    int second_entities = atomic_load(&mt_entity_count);
    REQUIRE(second_entities == NUM_ENTITIES);

    printf("Both iterations processed all %d entities correctly.\n", NUM_ENTITIES);
    printf("--- Parallel Execution Verification PASSED ---\n\n");

    ecs_free(ecs);
    tpool_destroy(g_tpool);
    g_tpool = NULL;

    return true;
}

// ---- Test Suite ----

TEST_SUITE(brutal_ecs_suite)
{
    RUN_TEST_CASE(test_bitset_zero_and_any);
    RUN_TEST_CASE(test_bitset_set_clear_test);
    RUN_TEST_CASE(test_bitset_operations);
    RUN_TEST_CASE(test_bitset_intersects);

    RUN_TEST_CASE(test_sparse_set_basic);
    RUN_TEST_CASE(test_sparse_set_multiple);

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
    RUN_TEST_CASE(test_selective_phase_execution);
    RUN_TEST_CASE(test_phase_sync_applies_deferred_adds);
    RUN_TEST_CASE(test_system_udata_roundtrip);

    // Multithreading tests
    RUN_TEST_CASE(test_multithreading_basic);
    RUN_TEST_CASE(test_multithreading_verify_parallel_execution);
}
