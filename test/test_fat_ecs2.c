/*
    test_fat_ecs2.c — unit tests for fat_ecs.h (value-handle, SoA ECS)
*/

typedef struct { float x, y;   } Position;
typedef struct { float vx, vy; } Velocity;
typedef struct { int hp;        } Health;

#define ECS_COMPONENTS(X) \
    X(Position, position) \
    X(Velocity, velocity) \
    X(Health,   health)

#include "fat_ecs.h"
#include "pico_unit.h"

/* ---- spawn / kill ---- */

TEST_CASE(test_fat2_create_valid)
{
    ecs_world world = { 0 };
    ecs_entity e = ecs_create(&world);
    REQUIRE(ecs_valid(e));
    REQUIRE(world.alive_count == 1);
    return true;
}

TEST_CASE(test_fat2_null_invalid)
{
    REQUIRE(!ecs_valid(ECS_NULL));
    return true;
}

TEST_CASE(test_fat2_kill_invalidates)
{
    ecs_world world = { 0 };
    ecs_entity e = ecs_create(&world);
    ecs_kill(e);
    REQUIRE(!ecs_valid(e));
    REQUIRE(world.alive_count == 0);
    return true;
}

TEST_CASE(test_fat2_double_kill_safe)
{
    ecs_world world = { 0 };
    ecs_entity e = ecs_create(&world);
    ecs_kill(e);
    ecs_kill(e);   /* no-op: stale handle */
    REQUIRE(world.alive_count == 0);
    return true;
}

TEST_CASE(test_fat2_stale_handle_after_reuse)
{
    ecs_world world = { 0 };
    ecs_entity e1 = ecs_create(&world);
    int slot = e1.index;
    ecs_kill(e1);

    ecs_entity e2 = ecs_create(&world);
    REQUIRE(e2.index == slot);  /* slot reused */
    REQUIRE(!ecs_valid(e1));    /* old handle stale (gen mismatch) */
    REQUIRE(ecs_valid(e2));
    return true;
}

TEST_CASE(test_fat2_mask_cleared_on_respawn)
{
    ecs_world world = { 0 };
    ecs_entity e1 = ecs_create(&world);
    ecs_set_position(e1, (Position){ 9.0f, 9.0f });
    REQUIRE(ecs_has(e1, Position));
    ecs_kill(e1);

    ecs_entity e2 = ecs_create(&world);
    REQUIRE(!ecs_has(e2, Position));  /* mask zeroed on respawn */
    return true;
}

/* ---- component setters ---- */

TEST_CASE(test_fat2_generic_set)
{
    ecs_world world = { 0 };
    ecs_entity e = ecs_create(&world);

    ecs_set(e, (Position){ 3.0f, 7.0f });
    ecs_set(e, (Velocity){ 1.0f, 2.0f });
    REQUIRE(ecs_has(e, Position));
    REQUIRE(ecs_has(e, Velocity));
    REQUIRE(e.position->x == 3.0f);
    REQUIRE(e.velocity->vx == 1.0f);
    return true;
}

TEST_CASE(test_fat2_typed_setter)
{
    ecs_world world = { 0 };
    ecs_entity e = ecs_create(&world);

    ecs_set_position(e, (Position){ 3.0f, 7.0f });
    REQUIRE(ecs_has(e, Position));
    REQUIRE(e.position->x == 3.0f);
    REQUIRE(e.position->y == 7.0f);
    return true;
}

TEST_CASE(test_fat2_multiple_setters)
{
    ecs_world world = { 0 };
    ecs_entity e = ecs_create(&world);

    ecs_set_position(e, (Position){ 1.0f, 2.0f });
    ecs_set_velocity(e, (Velocity){ 3.0f, 4.0f });
    ecs_set_health(e,   (Health){ 50 });

    REQUIRE(ecs_has(e, Position));
    REQUIRE(ecs_has(e, Velocity));
    REQUIRE(ecs_has(e, Health));
    REQUIRE(e.position->x  == 1.0f);
    REQUIRE(e.velocity->vx == 3.0f);
    REQUIRE(e.health->hp   == 50);
    return true;
}

/* --- mask queries --- */

TEST_CASE(test_fat2_has_all)
{
    ecs_world world = { 0 };
    ecs_entity e = ecs_create(&world);
    ecs_set_position(e, (Position){ 0, 0 });
    ecs_set_velocity(e, (Velocity){ 0, 0 });

    uint64_t pv  = ECS_MASK_ALL(Position, Velocity);
    uint64_t pvh = ECS_MASK_ALL(Position, Velocity, Health);
    REQUIRE( ecs_has_all(e, pv));
    REQUIRE(!ecs_has_all(e, pvh));
    return true;
}

/* ---- component remove ---- */

TEST_CASE(test_fat2_remove_clears_mask)
{
    ecs_world world = { 0 };
    ecs_entity e = ecs_create(&world);
    ecs_set_position(e, (Position){ 1.0f, 2.0f });
    ecs_set_velocity(e, (Velocity){ 3.0f, 4.0f });
    REQUIRE(ecs_has(e, Position));
    REQUIRE(ecs_has(e, Velocity));

    ecs_remove_position(e);
    REQUIRE(!ecs_has(e, Position));
    REQUIRE( ecs_has(e, Velocity));  /* other components unaffected */
    return true;
}

/* ---- ECS_NONE_OF ---- */

TEST_CASE(test_fat2_none_of_excludes)
{
    ecs_world world = { 0 };

    ecs_entity mover = ecs_create(&world);
    ecs_set_position(mover, (Position){ 1.0f, 0.0f });
    ecs_set_velocity(mover, (Velocity){ 1.0f, 0.0f });

    ecs_entity static_ = ecs_create(&world);
    ecs_set_position(static_, (Position){ 2.0f, 0.0f });

    int count = 0;
    ECS_WITH(&world, Position)
    ECS_NONE_OF(Velocity)
    {
        count++;
        REQUIRE(e.index == static_.index);
    }
    REQUIRE(count == 1);
    return true;
}

/* ---- ecs_first ---- */

TEST_CASE(test_fat2_first_found)
{
    ecs_world world = { 0 };
    ecs_entity a = ecs_create(&world);
    ecs_set_health(a, (Health){ 42 });
    ecs_entity b = ecs_create(&world);
    ecs_set_health(b, (Health){ 99 });

    ecs_entity f = ecs_first(&world, Health);
    REQUIRE(ecs_valid(f));
    REQUIRE(f.index == a.index);
    return true;
}

TEST_CASE(test_fat2_first_not_found)
{
    ecs_world world = { 0 };
    ecs_entity e = ecs_create(&world);
    ecs_set_position(e, (Position){ 0, 0 });

    REQUIRE(!ecs_valid(ecs_first(&world, Health)));
    return true;
}

/* ---- ECS_EACH ---- */

TEST_CASE(test_fat2_each_visits_all_live)
{
    ecs_world world = { 0 };
    ecs_entity a = ecs_create(&world);
    ecs_set_position(a, (Position){ 0, 0 });
    ecs_entity b = ecs_create(&world);
    ecs_set_health(b, (Health){ 1 });
    ecs_entity c = ecs_create(&world);
    ecs_set_velocity(c, (Velocity){ 0, 0 });
    ecs_kill(b);

    int count = 0;
    ECS_EACH(&world) { count++; }
    REQUIRE(count == 2);   /* b was killed */
    return true;
}

/* ---- break / continue ---- */

TEST_CASE(test_fat2_break_exits_loop)
{
    ecs_world world = { 0 };
    for (int i = 0; i < 5; i++) {
        ecs_entity e = ecs_create(&world);
        ecs_set_position(e, (Position){ (float)i, 0 });
    }

    int count = 0;
    ECS_WITH(&world, Position) {
        count++;
        if (count == 2) break;
    }
    REQUIRE(count == 2);
    return true;
}

TEST_CASE(test_fat2_continue_skips_entity)
{
    ecs_world world = { 0 };
    for (int i = 0; i < 4; i++) {
        ecs_entity e = ecs_create(&world);
        ecs_set_health(e, (Health){ i });
    }

    int sum = 0;
    ECS_WITH(&world, Health) {
        if (e.health->hp == 2) continue;
        sum += e.health->hp;
    }
    REQUIRE(sum == 0 + 1 + 3);  /* 2 skipped */
    return true;
}

/* ---- ECS_WITH / ECS_SYSTEM ---- */

TEST_CASE(test_fat2_system_filters_by_mask)
{
    ecs_world world = { 0 };

    ecs_entity mover = ecs_create(&world);
    ecs_set_position(mover, (Position){ 0.0f, 0.0f });
    ecs_set_velocity(mover, (Velocity){ 1.0f, 0.5f });

    ecs_entity stationary = ecs_create(&world);
    ecs_set_position(stationary, (Position){ 5.0f, 5.0f });

    ECS_WITH(&world, Position, Velocity) {
        e.position->x += e.velocity->vx;
        e.position->y += e.velocity->vy;
    }

    REQUIRE(mover.position->x      == 1.0f);
    REQUIRE(mover.position->y      == 0.5f);
    REQUIRE(stationary.position->x == 5.0f);   /* untouched */
    return true;
}

TEST_CASE(test_fat2_system_skips_empty_world)
{
    ecs_world world = { 0 };
    int visited = 0;

    ECS_WITH(&world, Position) { visited++; }

    REQUIRE(visited == 0);
    return true;
}

TEST_CASE(test_fat2_system_entity_count)
{
    ecs_world world = { 0 };
    int count = 0;

    for (int i = 0; i < 5; i++) {
        ecs_entity e = ecs_create(&world);
        ecs_set_position(e, (Position){ (float)i, 0.0f });
    }

    ecs_entity e = ecs_create(&world);
    ecs_set_health(e, (Health){ 99 });   /* no Position -- must be skipped */

    ECS_WITH(&world, Position) { count++; }

    REQUIRE(count == 5);
    return true;
}

TEST_CASE(test_fat2_alive_count_tracks_kills)
{
    ecs_world world = { 0 };

    ecs_entity a = ecs_create(&world);
    ecs_set_health(a, (Health){ 1 });
    ecs_entity b = ecs_create(&world);
    ecs_set_health(b, (Health){ 2 });
    REQUIRE(world.alive_count == 2);

    ecs_kill(a);
    REQUIRE(world.alive_count == 1);
    ecs_kill(b);
    REQUIRE(world.alive_count == 0);
    return true;
}

/* ---- suite ---- */

void fat_ecs2_suite()
{
    RUN_TEST_CASE(test_fat2_create_valid);
    RUN_TEST_CASE(test_fat2_null_invalid);
    RUN_TEST_CASE(test_fat2_kill_invalidates);
    RUN_TEST_CASE(test_fat2_double_kill_safe);
    RUN_TEST_CASE(test_fat2_stale_handle_after_reuse);
    RUN_TEST_CASE(test_fat2_mask_cleared_on_respawn);
    RUN_TEST_CASE(test_fat2_generic_set);
    RUN_TEST_CASE(test_fat2_typed_setter);
    RUN_TEST_CASE(test_fat2_multiple_setters);
    RUN_TEST_CASE(test_fat2_has_all);
    RUN_TEST_CASE(test_fat2_remove_clears_mask);
    RUN_TEST_CASE(test_fat2_none_of_excludes);
    RUN_TEST_CASE(test_fat2_first_found);
    RUN_TEST_CASE(test_fat2_first_not_found);
    RUN_TEST_CASE(test_fat2_each_visits_all_live);
    RUN_TEST_CASE(test_fat2_break_exits_loop);
    RUN_TEST_CASE(test_fat2_continue_skips_entity);
    RUN_TEST_CASE(test_fat2_system_filters_by_mask);
    RUN_TEST_CASE(test_fat2_system_skips_empty_world);
    RUN_TEST_CASE(test_fat2_system_entity_count);
    RUN_TEST_CASE(test_fat2_alive_count_tracks_kills);
}
