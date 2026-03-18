/*
    test_fat_ecs.c — tests for fat_ecs.h
*/

typedef struct { float x, y;   } Position;
typedef struct { float vx, vy; } Velocity;
typedef struct { int hp;        } Health;

#define ECS_COMPONENTS(X)  X(Position, pos)  X(Velocity, vel)  X(Health, health)

#include "fat_ecs.h"
#include "pico_unit.h"

/* ---- Spawn / Kill ---- */

TEST_CASE(test_fat_spawn_kill)
{
    ecs_world world = { 0 };
    ecs_entity e    = ecs_create(&world);
    REQUIRE(ecs_valid(e));

    ecs_kill(e);
    REQUIRE(!ecs_valid(e));
    return true;
}

TEST_CASE(test_fat_spawn_reuse_slot)
{
    ecs_world  world = { 0 };
    ecs_entity e1   = ecs_create(&world);
    int        i1   = e1.index;
    ecs_kill(e1);

    ecs_entity e2 = ecs_create(&world);
    REQUIRE(e2.index == i1);
    return true;
}

/* ---- ecs_set ---- */

TEST_CASE(test_fat_set)
{
    ecs_world  world = { 0 };
    ecs_entity e     = ecs_create(&world);

    ecs_set_pos(e,    (Position){ 3.0f, 7.0f });
    ecs_set_health(e, (Health){ 100 });

    REQUIRE(ecs_has(e, Position));
    REQUIRE(e.pos->x == 3.0f);
    REQUIRE(e.pos->y == 7.0f);
    REQUIRE(ecs_has(e, Health));
    REQUIRE(e.health->hp == 100);
    return true;
}

TEST_CASE(test_fat_zeroed_on_respawn)
{
    ecs_world  world = { 0 };
    ecs_entity e1    = ecs_create(&world);

    ecs_set_pos(e1, (Position){ 9.0f, 9.0f });
    REQUIRE(ecs_has(e1, Position));

    ecs_kill(e1);

    ecs_entity e2 = ecs_create(&world);
    REQUIRE(!ecs_has(e2, Position));  /* mask cleared on respawn */
    return true;
}

/* ---- ecs_create + multiple ecs_set ---- */

TEST_CASE(test_fat_create)
{
    ecs_world  world = { 0 };
    ecs_entity e     = ecs_create(&world);

    ecs_set_pos(e,    (Position){ 1.0f, 2.0f });
    ecs_set_vel(e,    (Velocity){ 3.0f, 4.0f });
    ecs_set_health(e, (Health){ 50 });

    REQUIRE(ecs_valid(e));
    REQUIRE(ecs_has(e, Position));
    REQUIRE(ecs_has(e, Velocity));
    REQUIRE(ecs_has(e, Health));
    REQUIRE(e.pos->x     == 1.0f);
    REQUIRE(e.vel->vx    == 3.0f);
    REQUIRE(e.health->hp == 50);
    return true;
}

/* ---- ECS_SYSTEM: mask filtering ---- */

TEST_CASE(test_fat_system_filters_entities)
{
    ecs_world world = { 0 };

    ecs_entity mover = ecs_create(&world);
    ecs_set_pos(mover, (Position){ 0.0f, 0.0f });
    ecs_set_vel(mover, (Velocity){ 1.0f, 0.0f });

    ecs_entity static_e = ecs_create(&world);
    ecs_set_pos(static_e, (Position){ 5.0f, 5.0f });

    ECS_SYSTEM(&world, Position, Velocity) {
        e.pos->x += e.vel->vx;
    }

    REQUIRE(mover.pos->x    == 1.0f);  /* moved */
    REQUIRE(static_e.pos->x == 5.0f);  /* untouched */
    return true;
}

TEST_CASE(test_fat_system_multi)
{
    ecs_world  world = { 0 };
    ecs_entity ent   = ecs_create(&world);
    ecs_set_pos(ent,    (Position){ 0.0f, 0.0f });
    ecs_set_vel(ent,    (Velocity){ 2.0f, 3.0f });
    ecs_set_health(ent, (Health){ 100 });

    ECS_SYSTEM(&world, Position, Velocity, Health) {
        e.pos->x     += e.vel->vx;
        e.pos->y     += e.vel->vy;
        e.health->hp -= 1;
    }

    REQUIRE(ent.pos->x     == 2.0f);
    REQUIRE(ent.pos->y     == 3.0f);
    REQUIRE(ent.health->hp == 99);
    return true;
}

/* ---- compile-time mask ---- */

TEST_CASE(test_fat_system_mask_3)
{
    uint64_t got  = ECS_MASK_ALL(Position, Velocity, Health);
    uint64_t want = ECS_MASK(Position) | ECS_MASK(Velocity) | ECS_MASK(Health);
    REQUIRE(got == want);
    return true;
}

/* ---- ecs_has / ecs_has_all ---- */

TEST_CASE(test_fat_has_all)
{
    ecs_world  world = { 0 };
    ecs_entity e     = ecs_create(&world);
    ecs_set_pos(e, (Position){ 0, 0 });
    ecs_set_vel(e, (Velocity){ 0, 0 });

    uint64_t pv  = ECS_MASK(Position) | ECS_MASK(Velocity);
    uint64_t pvh = pv | ECS_MASK(Health);
    REQUIRE( ecs_has_all(e, pv));
    REQUIRE(!ecs_has_all(e, pvh));
    return true;
}

/* ---- Suite entry point ---- */

void fat_ecs_suite()
{
    RUN_TEST_CASE(test_fat_spawn_kill);
    RUN_TEST_CASE(test_fat_spawn_reuse_slot);
    RUN_TEST_CASE(test_fat_set);
    RUN_TEST_CASE(test_fat_zeroed_on_respawn);
    RUN_TEST_CASE(test_fat_create);
    RUN_TEST_CASE(test_fat_system_filters_entities);
    RUN_TEST_CASE(test_fat_system_multi);
    RUN_TEST_CASE(test_fat_system_mask_3);
    RUN_TEST_CASE(test_fat_has_all);
}
