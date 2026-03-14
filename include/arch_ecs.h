/*
    arch_ecs.h — C23 Archetypal ECS, single-header, SoAoS layout

    USAGE:
        #define ECS_COMPONENTS(X)  X(Type, field) ...
        #include "arch_ecs.h"

    Zero-initialised ecs_world is ready to use immediately.
    Components are immutable per entity lifetime (fixed archetype).
    Component arrays are mmap'd upfront for pointer stability.
    Kill-during-iteration is safe (deferred swap-remove).

    SPAWNING
        ecs_entity e = ecs_spawn(&world, Position, Velocity);
        ecs_set(&world, e, (Position){ 1.f, 2.f });
        ecs_set(&world, e, (Velocity){ 3.f, 4.f });

    ITERATING
        ECS_FOR(&world, Position, pos, Velocity, vel) {
            pos->x += vel->vx;
        }

        ECS_FOR(&world, ecs_entity, e, Health, hp) {
            if (hp->hp <= 0)
                ecs_kill(&world, *e);
        }

        ECS_FOR(&world, Position, pos)
        ECS_NONE_OF(Velocity) { ... }

    HANDLES
    ecs_entity is a thin (id, gen) pair — 8 bytes.
    ecs_alive() catches stale handles after kill or slot reuse.
    Double-kill is safe.
*/

#include <assert.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifndef ECS_MAX_ENTITIES
#define ECS_MAX_ENTITIES 4096
#endif
#ifndef ECS_MAX_ARCHETYPES
#define ECS_MAX_ARCHETYPES 64
#endif

#ifdef ECS_COMPONENTS
#ifndef ARCH_ECS_H
#define ARCH_ECS_H

/* --- vm_reserve / vm_free ------------------------------------- */

#ifdef _WIN32
#include <windows.h>
static inline void *ecs_vm_reserve_(size_t sz)
{
    return VirtualAlloc(NULL, sz, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE);
}
static inline void ecs_vm_free_(void *p, size_t sz)
{
    (void)sz;
    VirtualFree(p, 0, MEM_RELEASE);
}
#else
#include <sys/mman.h>
static inline void *ecs_vm_reserve_(size_t sz)
{
    void *p = mmap(NULL, sz, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    return p == MAP_FAILED ? NULL : p;
}
static inline void ecs_vm_free_(void *p, size_t sz)
{
    munmap(p, sz);
}
#endif

/* --- internal variable prefix --------------------------------- */

#define ECS_P_(name) _ecs_##name

/* --- component IDs and masks (field-name based) --------------- */

enum
{
#define ECS_ENUM_(T, f) ECS_ID_##f,
    ECS_COMPONENTS(ECS_ENUM_)
#undef ECS_ENUM_
        ECS_COMPONENT_COUNT
};

_Static_assert(ECS_COMPONENT_COUNT <= 64, "max 64 components");

#define ECS_MASK(f) ((uint64_t)1ull << ECS_ID_##f)

/* clang-format off */
/* ECS_MASK_ALL(f, ...) — OR of field masks for any number of components. */
#define ECS_M_EMPTY_()
#define ECS_M_DEFER_(m)     m ECS_M_EMPTY_()
#define ECS_EVAL_(...)      ECS_E1_(ECS_E1_(ECS_E1_(__VA_ARGS__)))
#define ECS_E1_(...)        ECS_E2_(ECS_E2_(ECS_E2_(__VA_ARGS__)))
#define ECS_E2_(...)        ECS_E3_(ECS_E3_(ECS_E3_(__VA_ARGS__)))
#define ECS_E3_(...)        ECS_E4_(ECS_E4_(ECS_E4_(__VA_ARGS__)))
#define ECS_E4_(...)        __VA_ARGS__
#define ECS_M_FOLD_(f, ...) ECS_MASK(f) __VA_OPT__(| ECS_M_DEFER_(ECS_M_FOLD_C_)()(__VA_ARGS__))
#define ECS_M_FOLD_C_()     ECS_M_FOLD_
#define ECS_MASK_ALL(...)   (ECS_EVAL_(ECS_M_FOLD_(__VA_ARGS__)))

/* ECS_TMASK_TYPE_ — map a type name to its component mask via _Generic. */
#define ECS_TMASK_CASE_(T, f) T: ECS_MASK(f),
#define ECS_TMASK_TYPE_(TypeName) \
    _Generic((TypeName){0}, ECS_COMPONENTS(ECS_TMASK_CASE_) default: (uint64_t)0)

/* ECS_TMASK_ALL_TYPES — OR of type masks. */
#define ECS_TMT_FOLD_(T, ...) ECS_TMASK_TYPE_(T) __VA_OPT__(| ECS_M_DEFER_(ECS_TMT_FOLD_C_)()(__VA_ARGS__))
#define ECS_TMT_FOLD_C_() ECS_TMT_FOLD_
#define ECS_TMASK_ALL_TYPES(...) (ECS_EVAL_(ECS_TMT_FOLD_(__VA_ARGS__)))
/* clang-format on */

/* --- entity handle (thin) ------------------------------------- */

typedef struct
{
    int id;
    uint16_t gen;
} ecs_entity;

#define ECS_NULL ((ecs_entity){ 0 })

/* --- entity map ----------------------------------------------- */

typedef struct
{
    int archetype; /* index into world archetypes, -1 = dead */
    int row;
    uint16_t gen;
} ecs_entity_info;

/* --- archetype ------------------------------------------------ */

typedef struct
{
    uint64_t mask;
    int count;
    int *entity_ids;
#define ECS_ARCH_COL_(T, f) T *f;
    ECS_COMPONENTS(ECS_ARCH_COL_)
#undef ECS_ARCH_COL_
} ecs_archetype;

/* --- world ---------------------------------------------------- */

typedef struct
{
    ecs_entity_info entities[ECS_MAX_ENTITIES];
    int free_list[ECS_MAX_ENTITIES];
    int free_count, hwm, alive_count;
    ecs_archetype archetypes[ECS_MAX_ARCHETYPES];
    int archetype_count;
    bool iterating;
    int deferred_kills[ECS_MAX_ENTITIES];
    int deferred_kill_count;
    bool deferred_dead[ECS_MAX_ENTITIES];
} ecs_world;

/* --- alive check ---------------------------------------------- */

static inline bool ecs_alive(ecs_world *w, ecs_entity e)
{
    return e.id > 0 && e.id < ECS_MAX_ENTITIES &&
           w->entities[e.id].gen == e.gen && !w->deferred_dead[e.id];
}

/* --- archetype helpers ---------------------------------------- */

static inline int ecs_find_or_create_archetype_(ecs_world *w, uint64_t mask)
{
    for (int a = 0; a < w->archetype_count; a++)
        if (w->archetypes[a].mask == mask) return a;
    assert(w->archetype_count < ECS_MAX_ARCHETYPES);
    int a = w->archetype_count++;
    ecs_archetype *arch = &w->archetypes[a];
    arch->mask = mask;
    arch->entity_ids = ecs_vm_reserve_((size_t)ECS_MAX_ENTITIES * sizeof(int));
#define ECS_ALLOC_(T, f)                                                       \
    if (mask & ECS_MASK(f))                                                    \
        arch->f = ecs_vm_reserve_((size_t)ECS_MAX_ENTITIES * sizeof(T));
    ECS_COMPONENTS(ECS_ALLOC_)
#undef ECS_ALLOC_
    return a;
}

/* --- spawn ---------------------------------------------------- */

static inline ecs_entity ecs_spawn_(ecs_world *w, uint64_t mask)
{
    int id;
    if (w->free_count > 0) {
        id = w->free_list[--w->free_count];
    } else {
        assert(w->hwm + 1 < ECS_MAX_ENTITIES);
        id = ++w->hwm;
    }
    w->entities[id].gen++;

    int ai = ecs_find_or_create_archetype_(w, mask);
    ecs_archetype *arch = &w->archetypes[ai];

    assert(arch->count < ECS_MAX_ENTITIES);

    int row = arch->count++;
    arch->entity_ids[row] = id;

    w->entities[id].archetype = ai;
    w->entities[id].row = row;
    w->alive_count++;

    return (ecs_entity){ .id = id, .gen = w->entities[id].gen };
}

/* clang-format off */
#define ecs_spawn(w, ...) ecs_spawn_((w), ECS_TMASK_ALL_TYPES(__VA_ARGS__))
/* clang-format on */

/* --- deferred kill flush -------------------------------------- */

static inline void ecs_flush_kills_(ecs_world *w)
{
    for (int k = 0; k < w->deferred_kill_count; k++) {
        int id = w->deferred_kills[k];
        w->deferred_dead[id] = false;
        ecs_entity_info *info = &w->entities[id];
        ecs_archetype *arch = &w->archetypes[info->archetype];
        int row = info->row;
        int last = arch->count - 1;

        if (row != last) {
            int swapped_id = arch->entity_ids[last];
            arch->entity_ids[row] = swapped_id;
#define ECS_SWAP_(T, f)                                                        \
    if (arch->mask & ECS_MASK(f)) arch->f[row] = arch->f[last];
            ECS_COMPONENTS(ECS_SWAP_)
#undef ECS_SWAP_
            w->entities[swapped_id].row = row;
        }

        arch->count--;
        info->archetype = -1;
        w->free_list[w->free_count++] = id;
    }
    w->deferred_kill_count = 0;
}

/* --- kill (swap-remove or deferred) --------------------------- */

static inline void ecs_kill(ecs_world *w, ecs_entity e)
{
    if (!ecs_alive(w, e)) return;
    ecs_entity_info *info = &w->entities[e.id];

    if (w->iterating) {
        info->gen++;
        w->deferred_dead[e.id] = true;
        w->deferred_kills[w->deferred_kill_count++] = e.id;
        w->alive_count--;
        return;
    }

    ecs_archetype *arch = &w->archetypes[info->archetype];
    int row = info->row;
    int last = arch->count - 1;

    if (row != last) {
        int swapped_id = arch->entity_ids[last];
        arch->entity_ids[row] = swapped_id;
#define ECS_SWAP_(T, f)                                                        \
    if (arch->mask & ECS_MASK(f)) arch->f[row] = arch->f[last];
        ECS_COMPONENTS(ECS_SWAP_)
#undef ECS_SWAP_
        w->entities[swapped_id].row = row;
    }

    arch->count--;
    info->gen++;
    info->archetype = -1;
    w->free_list[w->free_count++] = e.id;
    w->alive_count--;
}

/* --- per-component accessors ---------------------------------- */

#define ECS_GET_IMPL_(T, f)                                                    \
    static inline T *ecs_get_##f(ecs_world *w, ecs_entity e)                   \
    {                                                                          \
        assert(ecs_alive(w, e));                                               \
        ecs_entity_info *info = &w->entities[e.id];                            \
        ecs_archetype *arch = &w->archetypes[info->archetype];                 \
        assert(arch->mask & ECS_MASK(f));                                      \
        return &arch->f[info->row];                                            \
    }
ECS_COMPONENTS(ECS_GET_IMPL_)
#undef ECS_GET_IMPL_

#define ECS_SET_IMPL_(T, f)                                                    \
    static inline void ecs_set_##f(ecs_world *w, ecs_entity e, T val)          \
    {                                                                          \
        assert(ecs_alive(w, e));                                               \
        ecs_entity_info *info = &w->entities[e.id];                            \
        ecs_archetype *arch = &w->archetypes[info->archetype];                 \
        assert(arch->mask & ECS_MASK(f));                                      \
        arch->f[info->row] = val;                                              \
    }
ECS_COMPONENTS(ECS_SET_IMPL_)
#undef ECS_SET_IMPL_

/* clang-format off */
#define ECS_GENERIC_SET_(T, f) T: ecs_set_##f,
#define ecs_set(w, e, ...)                                                     \
    _Generic((__VA_ARGS__),                                                    \
        ECS_COMPONENTS(ECS_GENERIC_SET_)                                       \
        default: (void)0                                                       \
    )((w), (e), __VA_ARGS__)
/* clang-format on */

/* --- queries -------------------------------------------------- */

#define ecs_has(w, e, T)                                                       \
    (ecs_alive((w), (e)) &&                                                    \
     ((w)->archetypes[(w)->entities[(e).id].archetype].mask &                  \
      ECS_TMASK_TYPE_(T)))

#define ecs_has_all(w, e, ...)                                                 \
    (ecs_alive((w), (e)) &&                                                    \
     (((w)->archetypes[(w)->entities[(e).id].archetype].mask &                 \
       ECS_TMASK_ALL_TYPES(__VA_ARGS__)) == ECS_TMASK_ALL_TYPES(__VA_ARGS__)))

static inline ecs_entity ecs_first_(ecs_world *w, uint64_t mask)
{
    for (int a = 0; a < w->archetype_count; a++) {
        ecs_archetype *arch = &w->archetypes[a];
        if ((arch->mask & mask) == mask && arch->count > 0) {
            int id = arch->entity_ids[0];
            return (ecs_entity){ .id = id, .gen = w->entities[id].gen };
        }
    }
    return ECS_NULL;
}

#define ecs_first(w, ...) ecs_first_((w), ECS_TMASK_ALL_TYPES(__VA_ARGS__))

// clang-format off

/* --- ECS_FOR (flat-pair: Type, name, Type, name, ...) --------- */
/*
    ECS_FOR(&world, Position, pos, Velocity, vel) {
        pos->x += vel->vx;
    }

    ECS_FOR(&world, ecs_entity, e, Health, hp) {
        if (hp->hp <= 0) ecs_kill(&world, *e);
    }

    ECS_FOR(&world, Position, pos)
    ECS_NONE_OF(Velocity) { ... }

    break and continue work correctly.
    ecs_kill during iteration is safe (deferred).
*/

/* Type-to-field-pointer: resolve a type name to its SoA pointer.
   ecs_entity maps to a compound-literal handle. */
#define ECS_TYPE_FIELD_PTR_(T, f) T: &ECS_P_(arch)->f[ECS_P_(i)],
#define ECS_RESOLVE_(TypeName) _Generic((TypeName){ 0 },                                    \
    ECS_COMPONENTS(ECS_TYPE_FIELD_PTR_)                                                      \
    ecs_entity: (ecs_entity[1]){ { .id  = ECS_P_(arch)->entity_ids[ECS_P_(i)],               \
                                   .gen = ECS_P_(w)->entities[ECS_P_(arch)->entity_ids[ECS_P_(i)]].gen } }, \
    default: (void *)0)

/* Flat-pair mask fold: consume (Type, name) pairs, ignore name. */
#define ECS_PM_FOLD_(T, name, ...) ECS_TMASK_TYPE_(T) __VA_OPT__(| ECS_M_DEFER_(ECS_PM_FOLD_C_)()(__VA_ARGS__))
#define ECS_PM_FOLD_C_() ECS_PM_FOLD_
#define ECS_PMASK_ALL(...) (ECS_EVAL_(ECS_PM_FOLD_(__VA_ARGS__)))

/* Flat-pair variable injection. */
#define ECS_PIV_ENTRY_(T, name, ...) \
    for (typeof(*ECS_RESOLVE_(T)) *restrict name = ECS_RESOLVE_(T); name; name = NULL) \
    __VA_OPT__(ECS_M_DEFER_(ECS_PIV_NEXT_C_)()(__VA_ARGS__))
#define ECS_PIV_NEXT_(T, name, ...) \
    for (typeof(*ECS_RESOLVE_(T)) *restrict name = ECS_RESOLVE_(T); name; name = NULL) \
    __VA_OPT__(ECS_M_DEFER_(ECS_PIV_NEXT_C_)()(__VA_ARGS__))

#define ECS_PIV_NEXT_C_() ECS_PIV_NEXT_
#define ECS_PINJECT_VARS_(...) ECS_EVAL_(ECS_PIV_ENTRY_(__VA_ARGS__))

#define ECS_FOR(world, ...)                                                                                                                             \
    for (ecs_world *ECS_P_(w) = (world), *ECS_P_(once) = (ECS_P_(w)->iterating = true, ECS_P_(w));                                                      \
         ECS_P_(once);                                                                                                                                  \
         ECS_P_(once) = (ECS_P_(w)->iterating = false, ecs_flush_kills_(ECS_P_(w)), NULL))                                                              \
    for (int ECS_P_(a) = 0, ECS_P_(brk) = 0; ECS_P_(a) < ECS_P_(w)->archetype_count && !ECS_P_(brk); ECS_P_(a)++)                                       \
    if ((ECS_P_(w)->archetypes[ECS_P_(a)].mask & ECS_PMASK_ALL(__VA_ARGS__)) == ECS_PMASK_ALL(__VA_ARGS__))                                              \
    for (ecs_archetype *ECS_P_(arch) = &ECS_P_(w)->archetypes[ECS_P_(a)], *ECS_P_(a_once) = ECS_P_(arch); ECS_P_(a_once); ECS_P_(a_once) = NULL)         \
    for (int ECS_P_(n) = ECS_P_(arch)->count, ECS_P_(i) = 0; ECS_P_(i) < ECS_P_(n) && !ECS_P_(brk); ECS_P_(i)++)                                        \
    if (!ECS_P_(w)->deferred_dead[ECS_P_(arch)->entity_ids[ECS_P_(i)]])                                                                                 \
    ECS_PINJECT_VARS_(__VA_ARGS__)                                                                                                                       \
    for (int ECS_P_(done) = (ECS_P_(brk) = 1, 0); !ECS_P_(done); ECS_P_(done) = 1, ECS_P_(brk) = 0)

/* ECS_NONE_OF — exclude by type names. */
#define ECS_NONE_OF(...) if (!(ECS_P_(arch)->mask & ECS_TMASK_ALL_TYPES(__VA_ARGS__)))
//
// clang-format on

/* --- cleanup -------------------------------------------------- */

static inline void ecs_world_destroy(ecs_world *w)
{
    for (int a = 0; a < w->archetype_count; a++) {
        ecs_archetype *arch = &w->archetypes[a];
        ecs_vm_free_(arch->entity_ids,
                     (size_t)ECS_MAX_ENTITIES * sizeof(int));
#define ECS_FREE_(T, f)                                                        \
    if (arch->f)                                                               \
        ecs_vm_free_(arch->f, (size_t)ECS_MAX_ENTITIES * sizeof(T));
        ECS_COMPONENTS(ECS_FREE_)
#undef ECS_FREE_
    }
}

#endif /* ARCH_ECS_H */
#else
#error "Define ECS_COMPONENTS(X) before including arch_ecs.h"
#endif /* ECS_COMPONENTS */
