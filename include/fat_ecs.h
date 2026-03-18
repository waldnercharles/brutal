/*
    fat_ecs.h - C23 ECS, single-header, SoA layout

    USAGE:
        #define ECS_COMPONENTS(X)  X(Type, field) ...
        #include "fat_ecs.h"

    Zero-initialised ecs_world is ready to use immediately.
    Slot 0 is a sentinel -- never allocated, always invalid.

    SPAWNING
        ecs_entity e = ecs_create(&world);
        ecs_set_position(e, (Position){1.f, 0.f});
        ecs_set_velocity(e, (Velocity){0.f, 1.f});

    ITERATING
        ECS_WITH(&world, Position, Velocity) {
            e.position->x += e.velocity->vx;
        }

        ECS_EACH(&world) { ... }            // all live entities

        ECS_WITH(&world, Position)          // chain none-of after with/each:
        ECS_NONE_OF(Velocity) { ... }

        ecs_entity e = ecs_first(&world, Config);  // first match or ECS_NULL

    HANDLES
    ecs_entity holds the slot index, the generation at spawn time, a world
    pointer, and a T* per component pointing into the world's SoA columns.
    ecs_valid() catches stale handles after kill or slot reuse.
    Double-kill is safe.

    DEMO
        #define ECS_DEMO
        #include "fat_ecs.h"
        // clang -std=c23 fat_ecs.h -DECS_DEMO -o demo && ./demo
*/

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifndef ECS_MAX_ENTITIES
#define ECS_MAX_ENTITIES 4096
#endif

#ifdef ECS_COMPONENTS
#ifndef FAT_ECS_H
#define FAT_ECS_H

/* --- component IDs and masks ---------------------------------- */

enum
{
#define ECS_ENUM_(T, f) ECS_ID_##T,
    ECS_COMPONENTS(ECS_ENUM_)
#undef ECS_ENUM_
        ECS_COMPONENT_COUNT
};

_Static_assert(ECS_COMPONENT_COUNT <= 64, "max 64 components");

#define ECS_MASK(T) ((uint64_t)1ull << ECS_ID_##T)

/* clang-format off */
/* ECS_MASK_ALL(T, ...) -- OR of type masks for any number of components.
   3^4 = 81 EVAL passes */
#define ECS_M_EMPTY_()
#define ECS_M_DEFER_(m)         m ECS_M_EMPTY_()
#define ECS_EVAL_(...)          ECS_E1_(ECS_E1_(ECS_E1_(__VA_ARGS__)))
#define ECS_E1_(...)            ECS_E2_(ECS_E2_(ECS_E2_(__VA_ARGS__)))
#define ECS_E2_(...)            ECS_E3_(ECS_E3_(ECS_E3_(__VA_ARGS__)))
#define ECS_E3_(...)            ECS_E4_(ECS_E4_(ECS_E4_(__VA_ARGS__)))
#define ECS_E4_(...)            __VA_ARGS__
#define ECS_M_FOLD_(T, ...)     ECS_MASK(T) __VA_OPT__(| ECS_M_DEFER_(ECS_M_FOLD_C_)()(__VA_ARGS__))
#define ECS_M_FOLD_C_()         ECS_M_FOLD_
#define ECS_MASK_ALL(...)       ECS_EVAL_(ECS_M_FOLD_(__VA_ARGS__))
/* clang-format on */

/* --- world ---------------------------------------------------- */
/*
    Parallel flat arrays; slot 0 is reserved as the sentinel.

    mask[i]       -- which components this entity owns; 0 = dead/empty
    gen[i]        -- incremented on create and kill; stale-handle detection.
                     Slots start at 0; first spawn yields gen=1. Dead/empty
                     slots always compare unequal to any live handle (gen>=1).
    next_free[i]  -- intrusive free list; 0 = end of chain
    free_head     -- head of free list; 0 = empty
    hwm           -- high-water mark; first spawn yields slot 1
    alive_count   -- live entity count

    One SoA column T field[MAX] per ECS_COMPONENTS entry.
*/
typedef struct
{
    uint64_t mask[ECS_MAX_ENTITIES];
    uint16_t gen[ECS_MAX_ENTITIES];

    int next_free[ECS_MAX_ENTITIES];
    int free_head;
    int hwm;
    int alive_count;

#define ECS_COL_(T, f) T f[ECS_MAX_ENTITIES];
    ECS_COMPONENTS(ECS_COL_)
#undef ECS_COL_
} ecs_world;

/* --- entity handle -------------------------------------------- */
/*
    index  -- slot (0 = null / ECS_NULL)
    gen    -- generation at spawn time
    world  -- back-pointer
    <f>    -- T* into world SoA column for each component
*/
typedef struct
{
    int index;
    uint16_t gen;
    ecs_world *world;
#define ECS_PTR_(T, f) T *f;
    ECS_COMPONENTS(ECS_PTR_)
#undef ECS_PTR_
} ecs_entity;

#define ECS_NULL ((ecs_entity){ 0 })

static inline ecs_entity ecs_entity_at(ecs_world *w, int i)
{
    return (ecs_entity){ .index = i,
                         .gen = w->gen[i],
                         .world = w,
#define ECS_INIT_PTR_(T, f) .f = &w->f[i],
                         ECS_COMPONENTS(ECS_INIT_PTR_)
#undef ECS_INIT_PTR_
    };
}

static inline bool ecs_valid(ecs_entity e)
{
    return e.index != 0 && e.world != NULL && e.index < ECS_MAX_ENTITIES &&
           e.world->gen[e.index] == e.gen;
}

/* --- spawn / kill --------------------------------------------- */

static inline ecs_entity ecs_create(ecs_world *w)
{
    int i;
    if (w->free_head != 0) {
        i = w->free_head;
        w->free_head = w->next_free[i];
        w->next_free[i] = 0;
    } else if (w->hwm + 1 < ECS_MAX_ENTITIES) {
        i = ++w->hwm;
    } else {
        return ECS_NULL;
    }
    w->gen[i]++;
    w->mask[i] = 0;
    w->alive_count++;
    return ecs_entity_at(w, i);
}

/* Increments gen (invalidates all handles to this slot), then pushes the
   slot onto the free list. No-op on stale handle. */
static inline void ecs_kill(ecs_entity e)
{
    if (!ecs_valid(e)) return;
    int i = e.index;
    e.world->gen[i]++;
    e.world->mask[i] = 0;
    e.world->alive_count--;
    e.world->next_free[i] = e.world->free_head;
    e.world->free_head = i;
}

/* --- component setters / removers ----------------------------- */
/* ecs_set_<field>(entity, value) -- writes value and sets mask bit */
/* ecs_remove_<field>(entity)     -- clears mask bit              */

#define ECS_SET_IMPL_(T, f)                                                    \
    static inline void ecs_set_##f(ecs_entity e, T c)                          \
    {                                                                          \
        e.world->f[e.index] = c;                                               \
        e.world->mask[e.index] |= ECS_MASK(T);                                 \
    }
ECS_COMPONENTS(ECS_SET_IMPL_)
#undef ECS_SET_IMPL_

#define ECS_REMOVE_IMPL_(T, f)                                                 \
    static inline void ecs_remove_##f(ecs_entity e)                            \
    {                                                                          \
        e.world->mask[e.index] &= ~ECS_MASK(T);                                \
    }
ECS_COMPONENTS(ECS_REMOVE_IMPL_)
#undef ECS_REMOVE_IMPL_

/* clang-format off */
/* ecs_set(entity, value) -- type-dispatched setter via _Generic.
   Uses __VA_ARGS__ so compound literals like (Vec2){1,2} are captured whole
   despite the comma inside their braces (braces don't nest in the preprocessor).
   ECS_GENERIC_CASE_ must stay defined; ecs_set expands it at each call site. */
#define ECS_SET_GENERIC_(T, f) T: ecs_set_##f,
#define ecs_set(e, ...)                                                        \
    _Generic((__VA_ARGS__), ECS_COMPONENTS(ECS_GENERIC_CASE_) default: (void)0)((e), __VA_ARGS__)
/* clang-format on */

/* --- queries -------------------------------------------------- */

#define ecs_has(e, T) ((e).world->mask[(e).index] & ECS_MASK(T))
#define ecs_has_all(e, m) (((e).world->mask[(e).index] & (m)) == (m))

/* ecs_first / ecs_next: first and subsequent entities matching a mask.
   ecs_any_next: advance to the next live entity regardless of components. */
static inline ecs_entity ecs_first_(ecs_world *w, uint64_t mask)
{
    for (int i = 1; i <= w->hwm; i++)
        if ((w->mask[i] & mask) == mask) return ecs_entity_at(w, i);
    return ECS_NULL;
}
static inline ecs_entity ecs_next_(ecs_world *w, int from, uint64_t mask)
{
    for (int i = from + 1; i <= w->hwm; i++)
        if ((w->mask[i] & mask) == mask) return ecs_entity_at(w, i);
    return ECS_NULL;
}
static inline ecs_entity ecs_any_next(ecs_world *w, int from)
{
    for (int i = from + 1; i <= w->hwm; i++)
        if (w->mask[i]) return ecs_entity_at(w, i);
    return ECS_NULL;
}

#define ecs_first(w, ...) ecs_first_(w, ECS_MASK_ALL(__VA_ARGS__))
#define ecs_next(w, id, ...) ecs_next_(w, id, ECS_MASK_ALL(__VA_ARGS__))

/* --- ECS_WITH / ECS_EACH / ECS_NONE_OF ------------------------ */
/*
    ECS_WITH:    iterate entities that own ALL listed components.
    ECS_EACH:    iterate all live entities (no component filter).
    ECS_NONE_OF: chain after ECS_WITH/ECS_EACH to exclude entities
                 that own ANY of the listed components.

        ECS_WITH(&world, Position, Velocity) {
            e.position->x += e.velocity->vx;
        }

        ECS_EACH(&world) { log_entity(e); }

        ECS_WITH(&world, Position)
        ECS_NONE_OF(Frozen) {
            move(e);
        }

    All three inject ecs_entity e. Each expands to a single for-loop,
    so `break` and `continue` work as expected.
*/
#define ECS_WITH(world, ...)                                                        \
    for (ecs_entity e = ecs_first_(world, ECS_MASK_ALL(__VA_ARGS__)); ecs_valid(e); \
         e = ecs_next_(world, e.index, ECS_MASK_ALL(__VA_ARGS__)))

#define ECS_EACH(world)                                                        \
    for (ecs_entity e = ecs_any_next(world, 0); ecs_valid(e);                  \
         e = ecs_any_next(world, e.index))

#define ECS_NONE_OF(...)                                                       \
    if ((e.world->mask[e.index] & ECS_MASK_ALL(__VA_ARGS__)) == 0)

#define ECS_SYSTEM ECS_WITH

#endif /* FAT_ECS_H */
#elif !defined(ECS_DEMO)
#error "Define ECS_COMPONENTS(X) before including fat_ecs.h"
#endif /* ECS_COMPONENTS */

/* ----------------------------------------------------------------
   Demo — define types + ECS_COMPONENTS, then self-include pulls
   in the implementation above (ECS_COMPONENTS now set, FAT_ECS_H
   not yet set → impl compiles). On the return pass ECS_DEMO_TYPES
   is set so this block is skipped, preventing infinite recursion.
---------------------------------------------------------------- */
#ifdef ECS_DEMO
typedef struct
{
    float x, y;
} Position;
typedef struct
{
    float vx, vy;
} Velocity;
typedef struct
{
    int hp;
} Health;
#define ECS_COMPONENTS(X)                                                      \
    X(Position, position)                                                      \
    X(Velocity, velocity)                                                      \
    X(Health, health)
#include __FILE__
#include <stdio.h>

int main(void)
{
    static ecs_world world;

    ecs_entity a = ecs_create(&world);
    ecs_set_position(a, (Position){ 0.0f, 0.0f });
    ecs_set_velocity(a, (Velocity){ 1.0f, 0.5f });
    ecs_set_health(a, (Health){ 100 });

    ecs_entity b = ecs_create(&world);
    ecs_set_position(b, (Position){ 5.0f, 3.0f });
    ecs_set_velocity(b, (Velocity){ -1.0f, 0.0f });
    ecs_set_health(b, (Health){ 50 });

    /* no velocity -- skipped by movement system */
    ecs_entity c = ecs_create(&world);
    ecs_set_position(c, (Position){ 9.0f, 9.0f });
    ecs_set_health(c, (Health){ 200 });

    printf("ECS_NULL valid: %d\n", ecs_valid(ECS_NULL));
    printf("alive: %d  hwm: %d\n\n", world.alive_count, world.hwm);

    ECS_WITH(&world, Position, Velocity)
    {
        e.position->x += e.velocity->vx;
        e.position->y += e.velocity->vy;
    }

    ECS_WITH(&world, Health)
    {
        e.health->hp -= 10;
    }

    printf(
        "a  slot=%d  pos=(%.1f, %.1f)  hp=%d\n",
        a.index,
        a.position->x,
        a.position->y,
        a.health->hp
    );
    printf(
        "b  slot=%d  pos=(%.1f, %.1f)  hp=%d\n",
        b.index,
        b.position->x,
        b.position->y,
        b.health->hp
    );
    printf(
        "c  slot=%d  pos=(%.1f, %.1f)  hp=%d\n",
        c.index,
        c.position->x,
        c.position->y,
        c.health->hp
    );

    printf("\nvalid: a=%d b=%d c=%d\n", ecs_valid(a), ecs_valid(b), ecs_valid(c));

    ecs_kill(b);
    printf("after kill(b):        valid b=%d  alive=%d\n", ecs_valid(b), world.alive_count);
    ecs_kill(b);
    printf("after double-kill(b): alive=%d\n", world.alive_count);

    ecs_entity b2 = ecs_create(&world);
    ecs_set_position(b2, (Position){ 99.f, 99.f });
    printf("b2 slot=%d (reused b slot=%d): %d\n", b2.index, b.index, b2.index == b.index);
    printf("b valid after reuse: %d\n\n", ecs_valid(b));

    /* none-of: Position entities that have no Velocity */
    printf("Position only (no Velocity):\n");
    ECS_WITH(&world, Position)
    ECS_NONE_OF(Velocity)
    {
        printf(
            "  slot %d  pos=(%.1f, %.1f)\n",
            e.index,
            e.position->x,
            e.position->y
        );
    }

    /* ecs_first: find first entity with Health */
    ecs_entity first_hp = ecs_first(&world, Health);
    printf("first Health slot=%d  hp=%d\n", first_hp.index, first_hp.health->hp);

    /* ECS_EACH: count all live entities */
    int total = 0;
    ECS_EACH(&world)
    {
        total++;
    }
    printf("total live entities: %d\n", total);

    return 0;
}
#endif /* ECS_DEMO */
