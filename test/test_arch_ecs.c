/*
    test_arch_ecs.c — tests for arch_ecs.h
*/

typedef struct { float x, y;   } Position;
typedef struct { float vx, vy; } Velocity;
typedef struct { int hp;        } Health;

#define ECS_COMPONENTS(X) \
    X(Position, position)  \
    X(Velocity, velocity)  \
    X(Health,   health)

#include "arch_ecs.h"
#include "pico_unit.h"

/* ---- Spawn / Alive / Kill ---- */

TEST_CASE(test_arch_spawn_alive)
{
    ecs_world world = {0}; ecs_world_init(&world);
    ecs_entity e = ecs_create(&world, Position);
    ecs_set_position(&world, e, (Position){ 1.f, 2.f });
    REQUIRE(ecs_alive(&world, e));
    REQUIRE(ecs_unpack_(e).id == 1);
    REQUIRE(ecs_unpack_(e).gen == 1);
    ecs_world_destroy(&world);
    return true;
}

TEST_CASE(test_arch_kill)
{
    ecs_world world = {0}; ecs_world_init(&world);
    ecs_entity e = ecs_create(&world, Position);
    ecs_destroy(&world, e);
    REQUIRE(!ecs_alive(&world, e));
    ecs_world_destroy(&world);
    return true;
}

TEST_CASE(test_arch_double_kill)
{
    ecs_world world = {0}; ecs_world_init(&world);
    ecs_entity e = ecs_create(&world, Position);
    ecs_destroy(&world, e);
    ecs_destroy(&world, e);
    REQUIRE(world.alive_count == 0);
    ecs_world_destroy(&world);
    return true;
}

TEST_CASE(test_arch_slot_reuse)
{
    ecs_world world = {0}; ecs_world_init(&world);
    ecs_entity e1 = ecs_create(&world, Position);
    int id1 = ecs_unpack_(e1).id;
    ecs_destroy(&world, e1);

    ecs_entity e2 = ecs_create(&world, Position);
    REQUIRE(ecs_unpack_(e2).id == id1);
    ecs_world_destroy(&world);
    return true;
}

TEST_CASE(test_arch_stale_handle)
{
    ecs_world world = {0}; ecs_world_init(&world);
    ecs_entity e1 = ecs_create(&world, Position);
    ecs_destroy(&world, e1);
    ecs_entity e2 = ecs_create(&world, Position);

    REQUIRE(ecs_unpack_(e2).id == ecs_unpack_(e1).id);
    REQUIRE(ecs_unpack_(e1).gen != ecs_unpack_(e2).gen);
    REQUIRE(!ecs_alive(&world, e1));
    REQUIRE(ecs_alive(&world, e2));
    ecs_world_destroy(&world);
    return true;
}

TEST_CASE(test_arch_null_not_alive)
{
    ecs_world world = {0}; ecs_world_init(&world);
    REQUIRE(!ecs_alive(&world, ECS_NULL));
    ecs_world_destroy(&world);
    return true;
}

/* ---- Swap-remove ---- */

TEST_CASE(test_arch_swap_remove)
{
    ecs_world world = {0}; ecs_world_init(&world);
    ecs_entity a = ecs_create(&world, Position);
    ecs_set_position(&world, a, (Position){ 1.f, 1.f });
    ecs_entity b = ecs_create(&world, Position);
    ecs_set_position(&world, b, (Position){ 2.f, 2.f });
    ecs_entity c = ecs_create(&world, Position);
    ecs_set_position(&world, c, (Position){ 3.f, 3.f });

    ecs_destroy(&world, b);

    REQUIRE(ecs_alive(&world, a));
    REQUIRE(!ecs_alive(&world, b));
    REQUIRE(ecs_alive(&world, c));

    Position *pa = ecs_get_position(&world, a);
    Position *pc = ecs_get_position(&world, c);
    REQUIRE(pa->x == 1.f && pa->y == 1.f);
    REQUIRE(pc->x == 3.f && pc->y == 3.f);

    ecs_world_destroy(&world);
    return true;
}

TEST_CASE(test_arch_swap_remove_multi_component)
{
    ecs_world world = {0}; ecs_world_init(&world);
    ecs_entity a = ecs_create(&world, Position, Health);
    ecs_set_position(&world, a, (Position){ 1.f, 1.f });
    ecs_set_health(&world, a, (Health){ 10 });
    ecs_entity b = ecs_create(&world, Position, Health);
    ecs_set_position(&world, b, (Position){ 2.f, 2.f });
    ecs_set_health(&world, b, (Health){ 20 });
    ecs_entity c = ecs_create(&world, Position, Health);
    ecs_set_position(&world, c, (Position){ 3.f, 3.f });
    ecs_set_health(&world, c, (Health){ 30 });

    ecs_destroy(&world, a);

    REQUIRE(ecs_get_position(&world, b)->x == 2.f);
    REQUIRE(ecs_get_health(&world, b)->hp == 20);
    REQUIRE(ecs_get_position(&world, c)->x == 3.f);
    REQUIRE(ecs_get_health(&world, c)->hp == 30);

    ecs_world_destroy(&world);
    return true;
}

/* ---- ecs_get / ecs_set ---- */

TEST_CASE(test_arch_get)
{
    ecs_world world = {0}; ecs_world_init(&world);
    ecs_entity e = ecs_create(&world, Position, Health);
    ecs_set_position(&world, e, (Position){ 3.f, 7.f });
    ecs_set_health(&world, e, (Health){ 100 });

    Position *p = ecs_get_position(&world, e);
    Health   *h = ecs_get_health(&world, e);
    REQUIRE(p->x == 3.f && p->y == 7.f);
    REQUIRE(h->hp == 100);
    ecs_world_destroy(&world);
    return true;
}

TEST_CASE(test_arch_set)
{
    ecs_world world = {0}; ecs_world_init(&world);
    ecs_entity e = ecs_create(&world, Position);

    ecs_set_position(&world, e, (Position){ 5.f, 9.f });
    REQUIRE(ecs_get_position(&world, e)->x == 5.f);
    REQUIRE(ecs_get_position(&world, e)->y == 9.f);

    ecs_world_destroy(&world);
    return true;
}

/* ---- ecs_has ---- */

TEST_CASE(test_arch_has)
{
    ecs_world world = {0}; ecs_world_init(&world);
    ecs_entity e = ecs_create(&world, Position, Velocity);

    REQUIRE(ecs_has_position(&world, e));
    REQUIRE(ecs_has_velocity(&world, e));
    REQUIRE(!ecs_has_health(&world, e));

    ecs_world_destroy(&world);
    return true;
}

TEST_CASE(test_arch_has_dead)
{
    ecs_world world = {0}; ecs_world_init(&world);
    ecs_entity e = ecs_create(&world, Position);
    ecs_destroy(&world, e);
    REQUIRE(!ecs_has_position(&world, e));
    ecs_world_destroy(&world);
    return true;
}

/* ---- ECS_FOR ---- */

TEST_CASE(test_arch_for_filters)
{
    ecs_world world = {0}; ecs_world_init(&world);
    ecs_entity e1 = ecs_create(&world, Position, Velocity);
    ecs_set_position(&world, e1, (Position){ 0.f, 0.f });
    ecs_set_velocity(&world, e1, (Velocity){ 1.f, 0.f });
    ecs_entity e2 = ecs_create(&world, Position);
    ecs_set_position(&world, e2, (Position){ 5.f, 5.f });

    ECS_FOR(&world, Position, pos, Velocity, vel) {
        pos->x += vel->vx;
    }

    /* verify: first entity moved, second untouched */
    float xs[2];
    int n = 0;
    ECS_FOR(&world, Position, pos) {
        xs[n++] = pos->x;
    }
    REQUIRE(n == 2);
    bool found_1 = (xs[0] == 1.f || xs[1] == 1.f);
    bool found_5 = (xs[0] == 5.f || xs[1] == 5.f);
    REQUIRE(found_1 && found_5);

    ecs_world_destroy(&world);
    return true;
}

TEST_CASE(test_arch_for_multi)
{
    ecs_world world = {0}; ecs_world_init(&world);
    ecs_entity e = ecs_create(&world, Position, Velocity, Health);
    ecs_set_position(&world, e, (Position){ 0.f, 0.f });
    ecs_set_velocity(&world, e, (Velocity){ 2.f, 3.f });
    ecs_set_health(&world, e, (Health){ 100 });

    ECS_FOR(&world, Position, pos, Velocity, vel, Health, hp) {
        pos->x += vel->vx;
        pos->y += vel->vy;
        hp->hp -= 1;
    }

    REQUIRE(ecs_get_position(&world, e)->x == 2.f);
    REQUIRE(ecs_get_position(&world, e)->y == 3.f);
    REQUIRE(ecs_get_health(&world, e)->hp == 99);

    ecs_world_destroy(&world);
    return true;
}

TEST_CASE(test_arch_spawn_loop_mask)
{
    ecs_world world = {0}; ecs_world_init(&world);
    for (int i = 0; i < 5; i++) {
        ecs_entity e = ecs_create(&world, Position);
        ecs_set_position(&world, e, (Position){ (float)i, 0.f });
    }

    REQUIRE(world.alive_count == 5);
    REQUIRE(world.archetype_count == 1);
    REQUIRE(world.archetypes[0].mask == ECS_MASK(position));
    REQUIRE(world.archetypes[0].count == 5);

    ecs_world_destroy(&world);
    return true;
}

TEST_CASE(test_arch_for_break)
{
    ecs_world world = {0}; ecs_world_init(&world);
    for (int i = 0; i < 5; i++) {
        ecs_entity e = ecs_create(&world, Position);
        ecs_set_position(&world, e, (Position){ (float)i, 0.f });
    }

    int count = 0;
    ECS_FOR(&world, Position, pos) {
        count++;
        if (count == 3) break;
    }
    REQUIRE(count == 3);

    ecs_world_destroy(&world);
    return true;
}

TEST_CASE(test_arch_for_continue)
{
    ecs_world world = {0}; ecs_world_init(&world);
    for (int i = 0; i < 5; i++) {
        ecs_entity e = ecs_create(&world, Position);
        ecs_set_position(&world, e, (Position){ (float)i, 0.f });
    }

    int count = 0;
    ECS_FOR(&world, Position, pos) {
        if (pos->x == 2.f) continue;
        count++;
    }
    REQUIRE(count == 4);

    ecs_world_destroy(&world);
    return true;
}

/* ---- Archetypes ---- */

TEST_CASE(test_arch_multiple_archetypes)
{
    ecs_world world = {0}; ecs_world_init(&world);
    ecs_create(&world, Position);
    ecs_create(&world, Position, Velocity);
    ecs_create(&world, Position, Velocity, Health);
    REQUIRE(world.archetype_count == 3);

    ecs_world_destroy(&world);
    return true;
}

TEST_CASE(test_arch_reuse_archetype)
{
    ecs_world world = {0}; ecs_world_init(&world);
    ecs_create(&world, Position);
    ecs_create(&world, Position);
    ecs_create(&world, Position);
    REQUIRE(world.archetype_count == 1);
    REQUIRE(world.archetypes[0].count == 3);

    ecs_world_destroy(&world);
    return true;
}

/* ---- alive_count ---- */

TEST_CASE(test_arch_alive_count)
{
    ecs_world world = {0}; ecs_world_init(&world);
    ecs_entity a = ecs_create(&world, Position);
    ecs_entity b = ecs_create(&world, Position);
    ecs_create(&world, Position);
    REQUIRE(world.alive_count == 3);

    ecs_destroy(&world, a);
    REQUIRE(world.alive_count == 2);
    ecs_destroy(&world, b);
    REQUIRE(world.alive_count == 1);

    ecs_world_destroy(&world);
    return true;
}

/* ---- Mask ---- */


/* ---- Spawn with values ---- */

TEST_CASE(test_arch_spawn_with_values)
{
    ecs_world world = {0}; ecs_world_init(&world);
    ecs_entity e = ecs_create(&world, Position, Health);
    ecs_set_position(&world, e, (Position){ 5.f, 6.f });
    ecs_set_health(&world, e, (Health){ 42 });

    REQUIRE(ecs_has_position(&world, e));
    REQUIRE(!ecs_has_velocity(&world, e));
    REQUIRE(ecs_has_health(&world, e));
    REQUIRE(ecs_get_position(&world, e)->x == 5.f);
    REQUIRE(ecs_get_health(&world, e)->hp == 42);

    ecs_world_destroy(&world);
    return true;
}

/* ---- ecs_create without init ---- */

TEST_CASE(test_arch_spawn_no_init)
{
    ecs_world world = {0}; ecs_world_init(&world);
    ecs_entity e = ecs_create(&world, Position, Velocity);

    REQUIRE(ecs_alive(&world, e));
    REQUIRE(ecs_has_position(&world, e));
    REQUIRE(ecs_has_velocity(&world, e));

    ecs_set_position(&world, e, (Position){ 5.f, 6.f });
    ecs_set_velocity(&world, e, (Velocity){ 1.f, 2.f });
    REQUIRE(ecs_get_position(&world, e)->x == 5.f);
    REQUIRE(ecs_get_velocity(&world, e)->vx == 1.f);

    ecs_world_destroy(&world);
    return true;
}

/* ---- ECS_FOR with ecs_entity ---- */

TEST_CASE(test_arch_for_entity_handle)
{
    ecs_world world = {0}; ecs_world_init(&world);
    ecs_entity e1 = ecs_create(&world, Position);
    ecs_set_position(&world, e1, (Position){ 1.f, 0.f });
    ecs_entity e2 = ecs_create(&world, Position);
    ecs_set_position(&world, e2, (Position){ 2.f, 0.f });

    int count = 0;
    ECS_FOR(&world, ecs_entity, e, Position, pos) {
        REQUIRE(ecs_unpack_(*e).id > 0);
        REQUIRE(ecs_alive(&world, *e));
        count++;
    }
    REQUIRE(count == 2);

    ecs_world_destroy(&world);
    return true;
}

/* ---- Kill during iteration ---- */

TEST_CASE(test_arch_kill_during_iteration)
{
    ecs_world world = {0}; ecs_world_init(&world);
    ecs_entity e1 = ecs_create(&world, Health);
    ecs_set_health(&world, e1, (Health){ 0 });
    ecs_entity e2 = ecs_create(&world, Health);
    ecs_set_health(&world, e2, (Health){ 100 });
    ecs_entity e3 = ecs_create(&world, Health);
    ecs_set_health(&world, e3, (Health){ 0 });

    /* kill entities with hp <= 0 during iteration */
    ECS_FOR(&world, ecs_entity, e, Health, hp) {
        if (hp->hp <= 0)
            ecs_destroy(&world, *e);
    }

    REQUIRE(world.alive_count == 1);
    REQUIRE(!ecs_alive(&world, e1));
    REQUIRE(ecs_alive(&world, e2));
    REQUIRE(!ecs_alive(&world, e3));
    REQUIRE(ecs_get_health(&world, e2)->hp == 100);

    ecs_world_destroy(&world);
    return true;
}

TEST_CASE(test_arch_kill_during_iteration_skips_dead)
{
    ecs_world world = {0}; ecs_world_init(&world);
    /* spawn 5 entities, kill first 3 during iteration */
    for (int i = 0; i < 5; i++) {
        ecs_entity e = ecs_create(&world, Position);
        ecs_set_position(&world, e, (Position){ (float)i, 0.f });
    }

    int visited = 0;
    ECS_FOR(&world, ecs_entity, e, Position, pos) {
        visited++;
        if (pos->x < 3.f)
            ecs_destroy(&world, *e);
    }
    /* entities killed before their iteration turn are skipped */
    REQUIRE(visited >= 2);
    REQUIRE(world.alive_count == 2);

    ecs_world_destroy(&world);
    return true;
}

/* ---- Spawn during iteration (pointer stability) ---- */

TEST_CASE(test_arch_spawn_during_iteration)
{
    ecs_world world = {0}; ecs_world_init(&world);
    for (int i = 0; i < 3; i++) {
        ecs_entity e = ecs_create(&world, Position);
        ecs_set_position(&world, e, (Position){ (float)i, 0.f });
    }

    /* capture pointers before spawning more entities */
    Position *ptrs[3];
    int n = 0;
    ECS_FOR(&world, Position, pos) {
        ptrs[n++] = pos;
    }
    REQUIRE(n == 3);

    /* spawn more entities in the same archetype */
    for (int i = 0; i < 100; i++)
        ecs_create(&world, Position);

    /* original pointers must still be valid (mmap, no realloc) */
    REQUIRE(ptrs[0]->x == 0.f);
    REQUIRE(ptrs[1]->x == 1.f);
    REQUIRE(ptrs[2]->x == 2.f);

    ecs_world_destroy(&world);
    return true;
}


/* ---- Suite entry point ---- */

void arch_ecs_suite()
{
    RUN_TEST_CASE(test_arch_spawn_alive);
    RUN_TEST_CASE(test_arch_kill);
    RUN_TEST_CASE(test_arch_double_kill);
    RUN_TEST_CASE(test_arch_slot_reuse);
    RUN_TEST_CASE(test_arch_stale_handle);
    RUN_TEST_CASE(test_arch_null_not_alive);
    RUN_TEST_CASE(test_arch_swap_remove);
    RUN_TEST_CASE(test_arch_swap_remove_multi_component);
    RUN_TEST_CASE(test_arch_get);
    RUN_TEST_CASE(test_arch_set);
    RUN_TEST_CASE(test_arch_has);
    RUN_TEST_CASE(test_arch_has_dead);
    RUN_TEST_CASE(test_arch_for_filters);
    RUN_TEST_CASE(test_arch_for_multi);
    RUN_TEST_CASE(test_arch_spawn_loop_mask);
    RUN_TEST_CASE(test_arch_for_break);
    RUN_TEST_CASE(test_arch_for_continue);
    RUN_TEST_CASE(test_arch_multiple_archetypes);
    RUN_TEST_CASE(test_arch_reuse_archetype);
    RUN_TEST_CASE(test_arch_alive_count);
    RUN_TEST_CASE(test_arch_spawn_with_values);
    RUN_TEST_CASE(test_arch_spawn_no_init);
    /* ECS_FOR + ecs_entity */
    RUN_TEST_CASE(test_arch_for_entity_handle);
    /* deferred kill */
    RUN_TEST_CASE(test_arch_kill_during_iteration);
    RUN_TEST_CASE(test_arch_kill_during_iteration_skips_dead);
    /* pointer stability */
    RUN_TEST_CASE(test_arch_spawn_during_iteration);
}
