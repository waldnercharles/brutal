/*
    arch_ecs.h — C23 Archetypal ECS, single-header, SoAoS layout

    USAGE:
        #define ECS_COMPONENTS(X)  X(Type, field) ...
        #include "arch_ecs.h"

    Call ecs_world_init() before use.
    Components are immutable per entity lifetime (fixed archetype).
    Component arrays are mmap'd upfront for pointer stability.
    Destroy is always deferred. Call ecs_flush() to apply.

    CREATING
        ecs_entity e = ecs_create(&world, Transform, Velocity);
        ecs_set_transform(&world, e, (Transform){ 1.f, 2.f });
        ecs_set_velocity(&world, e, (Velocity){ 3.f, 4.f });

    ITERATING
        ECS_FOR(&world, Transform, pos, Velocity, vel) {
            pos->x += vel->vx;
        }

        ECS_FOR(&world, ecs_entity, e, Health, hp) {
            if (hp->hp <= 0)
                ecs_destroy(&world, *e);
        }

    ITERATING DESTROYED (pre-flush, component data still valid)
        ECS_FOR_DESTROYED(&world, SomeComp, sc) {
            cleanup(sc);
        }
        ecs_flush(&world);

    HANDLES
    ecs_entity is a thin (id, gen) pair — 8 bytes.
    ecs_alive() catches stale handles after destroy or slot reuse.
    Double-destroy is safe.
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

/* --- entity handle  ------------------------------------------- */

#ifndef ARCH_ECS_ENTITY_H
#define ARCH_ECS_ENTITY_H

typedef uint64_t ecs_entity;
#define ECS_NULL ((ecs_entity)0)

/* Internal: union for field access within arch_ecs implementation. */
typedef union
{
    struct
    {
        int id;
        uint16_t gen;
        uint16_t _pad;
    };
    uint64_t bits;
} ecs_entity_;

static inline ecs_entity_ ecs_unpack_(ecs_entity e)
{ return (ecs_entity_){ .bits = e }; }
static inline ecs_entity ecs_pack_(int id, uint16_t gen)
{ return (ecs_entity_){ .id = id, .gen = gen }.bits; }

typedef struct ecs_world ecs_world;

/* --- entity ↔ uintptr_t for physics user_data --- */
#define ecs_entity_to_ud(e) ((uintptr_t)ecs_unpack_(e).id)
#define ecs_entity_from_ud(w, ud)                                              \
    ecs_pack_((int)(ud), (w)->entities[(int)(ud)].gen)

#endif /* ARCH_ECS_ENTITY_H */

#ifdef ECS_COMPONENTS
#ifndef ARCH_ECS_H
#define ARCH_ECS_H

/* --- vm_reserve / vm_free ------------------------------------- */

#ifdef _WIN32
#include <windows.h>
static inline void *ecs_vm_reserve_(size_t sz)
{ return VirtualAlloc(NULL, sz, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE); }
static inline void ecs_vm_free_(void *p, size_t sz)
{
    (void)sz;
    VirtualFree(p, 0, MEM_RELEASE);
}
#else
#include <sys/mman.h>
static inline void *ecs_vm_reserve_(size_t sz)
{
    void *p = mmap(NULL, sz, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    return p == MAP_FAILED ? NULL : p;
}
static inline void ecs_vm_free_(void *p, size_t sz)
{ munmap(p, sz); }
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
/* Deferred-evaluation machinery (shared by ecs_create / ECS_FOR macros). */
#define ECS_M_EMPTY_()
#define ECS_M_DEFER_(m)     m ECS_M_EMPTY_()
#define ECS_EVAL_(...)      ECS_E1_(ECS_E1_(ECS_E1_(__VA_ARGS__)))
#define ECS_E1_(...)        ECS_E2_(ECS_E2_(ECS_E2_(__VA_ARGS__)))
#define ECS_E2_(...)        ECS_E3_(ECS_E3_(ECS_E3_(__VA_ARGS__)))
#define ECS_E3_(...)        ECS_E4_(ECS_E4_(ECS_E4_(__VA_ARGS__)))
#define ECS_E4_(...)        __VA_ARGS__
/* clang-format on */

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

struct ecs_world
{
    ecs_entity_info *entities;
    int *free_list_next; /* per-entity linked list, 0 = end */
    int free_list_head;
    int hwm;
    int alive_count;
    ecs_archetype archetypes[ECS_MAX_ARCHETYPES];
    int archetype_count;
    bool iterating;
    int *deferred_destroys;
    int deferred_destroy_count;
    bool *deferred_dead;
    bool *excluded;
};

/* --- world init ----------------------------------------------- */

static inline void ecs_world_init(ecs_world *w)
{
    *w = (ecs_world){ 0 };
    w->entities = ecs_vm_reserve_((size_t)ECS_MAX_ENTITIES * sizeof(ecs_entity_info));
    w->free_list_next = ecs_vm_reserve_((size_t)ECS_MAX_ENTITIES * sizeof(int));
    w->deferred_destroys = ecs_vm_reserve_((size_t)ECS_MAX_ENTITIES * sizeof(int));
    w->deferred_dead = ecs_vm_reserve_((size_t)ECS_MAX_ENTITIES * sizeof(bool));
    w->excluded = ecs_vm_reserve_((size_t)ECS_MAX_ENTITIES * sizeof(bool));
}

/* --- alive check ---------------------------------------------- */

static inline bool ecs_alive(ecs_world *w, ecs_entity e)
{
    ecs_entity_ u = ecs_unpack_(e);
    return u.id > 0 && u.id < ECS_MAX_ENTITIES &&
           w->entities[u.id].gen == u.gen && !w->deferred_dead[u.id];
}

/* --- free list ------------------------------------------------ */

static inline int ecs_free_list_pop_(ecs_world *w)
{
    int id = w->free_list_head;
    if (id) w->free_list_head = w->free_list_next[id];
    return id;
}

static inline void ecs_free_list_push_(ecs_world *w, int id)
{
    w->free_list_next[id] = w->free_list_head;
    w->free_list_head = id;
}

/* --- archetype helpers ---------------------------------------- */

static inline int ecs_find_or_create_archetype_(ecs_world *w, uint64_t mask)
{
    for (int a = 0; a < w->archetype_count; a++)
        if (w->archetypes[a].mask == mask) return a;

    int a = w->archetype_count++;
    assert(a < ECS_MAX_ARCHETYPES);

    ecs_archetype *arch = &w->archetypes[a];
    arch->entity_ids = ecs_vm_reserve_((size_t)ECS_MAX_ENTITIES * sizeof(int));
#define ECS_ALLOC_(T, f)                                                       \
    if (mask & ECS_MASK(f))                                                    \
        arch->f = ecs_vm_reserve_((size_t)ECS_MAX_ENTITIES * sizeof(T));
    ECS_COMPONENTS(ECS_ALLOC_)
#undef ECS_ALLOC_
    arch->mask = mask; /* publish last */
    return a;
}

/* --- create --------------------------------------------------- */

static inline ecs_entity ecs_create_(ecs_world *w, uint64_t mask)
{
    int id = ecs_free_list_pop_(w);
    if (id == 0) {
        id = ++w->hwm;
        assert(id < ECS_MAX_ENTITIES);
    }
    w->entities[id].gen++;

    int ai = ecs_find_or_create_archetype_(w, mask);
    ecs_archetype *arch = &w->archetypes[ai];

    int row = arch->count++;
    assert(row < ECS_MAX_ENTITIES);
    arch->entity_ids[row] = id;

    w->entities[id].archetype = ai;
    w->entities[id].row = row;
    w->alive_count++;

    w->excluded[id] = false;
    return ecs_pack_(id, w->entities[id].gen);
}

/* clang-format off */
/* ecs_create(w, TypeName, ...) — create with type names (token-paste to mask). */
#define ECS_CR_FOLD_(T, ...) ecs__mask_##T __VA_OPT__(| ECS_M_DEFER_(ECS_CR_FOLD_C_)()(__VA_ARGS__))
#define ECS_CR_FOLD_C_() ECS_CR_FOLD_
#define ecs_create(w, ...) ecs_create_((w), ECS_EVAL_(ECS_CR_FOLD_(__VA_ARGS__)))

/* ecs_create_excluded — like ecs_create but invisible to ECS_FOR queries. */
#define ecs_create_excluded(w, ...) ecs_create_excluded_((w), ECS_EVAL_(ECS_CR_FOLD_(__VA_ARGS__)))

static inline ecs_entity ecs_create_excluded_(ecs_world *w, uint64_t mask)
{
    ecs_entity e = ecs_create_(w, mask);
    w->excluded[ecs_unpack_(e).id] = true;
    return e;
}

/* clang-format on */

/* --- deferred destroy flush ----------------------------------- */

static inline void ecs_flush(ecs_world *w)
{
    for (int k = 0; k < w->deferred_destroy_count; k++) {
        int id = w->deferred_destroys[k];
        w->deferred_dead[id] = false;
        ecs_entity_info *info = &w->entities[id];
        ecs_archetype *arch = &w->archetypes[info->archetype];
        int row = info->row;
        int last = --arch->count;

        if (row != last) {
            int swapped_id = arch->entity_ids[last];
            arch->entity_ids[row] = swapped_id;
#define ECS_SWAP_(T, f)                                                        \
    if (arch->mask & ECS_MASK(f)) arch->f[row] = arch->f[last];
            ECS_COMPONENTS(ECS_SWAP_)
#undef ECS_SWAP_
            w->entities[swapped_id].row = row;
        }

        info->archetype = -1;
        ecs_free_list_push_(w, id);
    }
    w->deferred_destroy_count = 0;
}

/* --- destroy (always deferred) -------------------------------- */

static inline void ecs_destroy(ecs_world *w, ecs_entity e)
{
    if (!ecs_alive(w, e)) return;
    int eid = ecs_unpack_(e).id;
    w->entities[eid].gen++;
    w->deferred_dead[eid] = true;
    w->deferred_destroys[w->deferred_destroy_count++] = eid;
    w->alive_count--;
}

/* --- per-component accessors ---------------------------------- */

#define ECS_GET_IMPL_(T, f)                                                    \
    static inline T *ecs_get_##f(ecs_world *w, ecs_entity e)                   \
    {                                                                          \
        assert(ecs_alive(w, e));                                               \
        ecs_entity_info *info = &w->entities[ecs_unpack_(e).id];               \
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
        ecs_entity_info *info = &w->entities[ecs_unpack_(e).id];               \
        ecs_archetype *arch = &w->archetypes[info->archetype];                 \
        assert(arch->mask & ECS_MASK(f));                                      \
        arch->f[info->row] = val;                                              \
    }
ECS_COMPONENTS(ECS_SET_IMPL_)
#undef ECS_SET_IMPL_

#define ECS_HAS_IMPL_(T, f)                                                    \
    static inline bool ecs_has_##f(ecs_world *w, ecs_entity e)                 \
    {                                                                          \
        if (!ecs_alive(w, e)) return false;                                    \
        return (w->archetypes[w->entities[ecs_unpack_(e).id].archetype].mask & \
                ECS_MASK(f)) != 0;                                             \
    }
ECS_COMPONENTS(ECS_HAS_IMPL_)
#undef ECS_HAS_IMPL_

/* --- auto-generated mask constants (type name → field mask) --- */

/* clang-format off */
#define ECS_MASK_GEN_(T, f) static const uint64_t ecs__mask_##T = ECS_MASK(f);
ECS_COMPONENTS(ECS_MASK_GEN_)
#undef ECS_MASK_GEN_
static const uint64_t ecs__mask_ecs_entity = 0;

/* --- auto-generated field accessors (type name → SoA pointer) - */

#define ECS_ACC_GEN_(T, f)                                                     \
    static inline T *ecs__acc_##T(ecs_archetype *arch, int i)                  \
    { return &arch->f[i]; }
ECS_COMPONENTS(ECS_ACC_GEN_)
#undef ECS_ACC_GEN_
/* clang-format on */

/* ecs_entity accessor — compound-literal handle (special case). */
#define ecs__acc_ecs_entity(arch, i)                                           \
    ((ecs_entity[1]){ ecs_pack_(                                               \
        (arch)->entity_ids[(i)],                                               \
        ECS_P_(w)->entities[(arch)->entity_ids[(i)]].gen                       \
    ) })

// clang-format off

/* --- ECS_FOR (flat-pair: Type, name, Type, name, ...) --------- */
/*
    ECS_FOR(&world, Transform, pos, Velocity, vel) {
        pos->x += vel->vx;
    }

    ECS_FOR(&world, ecs_entity, e, Health, hp) {
        if (hp->hp <= 0) ecs_destroy(&world, *e);
    }

    break and continue work correctly.
    ecs_destroy during iteration is safe (always deferred).
    Call ecs_flush() after all systems to apply pending destroys.
*/

/* Flat-pair mask fold: consume (Type, name) pairs, map Type via ecs__mask_##T. */
#define ECS_PMASK_ONE_(T) ecs__mask_##T
#define ECS_PM_FOLD_(T, name, ...) ECS_PMASK_ONE_(T) __VA_OPT__(| ECS_M_DEFER_(ECS_PM_FOLD_C_)()(__VA_ARGS__))
#define ECS_PM_FOLD_C_() ECS_PM_FOLD_
#define ECS_PMASK_ALL(...) (ECS_EVAL_(ECS_PM_FOLD_(__VA_ARGS__)))

/* Flat-pair variable injection: resolve Type via ecs__acc_##T. */
#define ECS_RESOLVE_(T) ecs__acc_##T(ECS_P_(arch), ECS_P_(i))

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
         ECS_P_(once) = (ECS_P_(w)->iterating = false, NULL))                                                              \
    for (int ECS_P_(a) = 0, ECS_P_(brk) = 0; ECS_P_(a) < ECS_P_(w)->archetype_count && !ECS_P_(brk); ECS_P_(a)++)                                       \
    if ((ECS_P_(w)->archetypes[ECS_P_(a)].mask & ECS_PMASK_ALL(__VA_ARGS__)) == ECS_PMASK_ALL(__VA_ARGS__))                                              \
    for (ecs_archetype *ECS_P_(arch) = &ECS_P_(w)->archetypes[ECS_P_(a)], *ECS_P_(a_once) = ECS_P_(arch); ECS_P_(a_once); ECS_P_(a_once) = NULL)         \
    for (int ECS_P_(n) = ECS_P_(arch)->count, ECS_P_(i) = 0; ECS_P_(i) < ECS_P_(n) && !ECS_P_(brk); ECS_P_(i)++)                                        \
    if (!ECS_P_(w)->deferred_dead[ECS_P_(arch)->entity_ids[ECS_P_(i)]] &&                                                                              \
        !ECS_P_(w)->excluded[ECS_P_(arch)->entity_ids[ECS_P_(i)]])                                                                                 \
    ECS_PINJECT_VARS_(__VA_ARGS__)                                                                                                                       \
    for (int ECS_P_(done) = (ECS_P_(brk) = 1, 0); !ECS_P_(done); ECS_P_(done) = 1, ECS_P_(brk) = 0)

/* --- ECS_FOR_DESTROYED (pre-flush iteration of dead entities) - */
/*
    ECS_FOR_DESTROYED(&world, PhysBody, pb) {
        phys_cleanup(pb);
    }
    ecs_flush(&world);

    Component data is valid until ecs_flush (swap-remove hasn't run).
    break and continue work correctly.
*/

#define ECS_FOR_DESTROYED(world, ...)                                                                                                                    \
    for (ecs_world *ECS_P_(w) = (world), *ECS_P_(once) = ECS_P_(w); ECS_P_(once); ECS_P_(once) = NULL)                                                  \
    for (int ECS_P_(k) = 0, ECS_P_(brk) = 0;                                                                                                           \
         ECS_P_(k) < ECS_P_(w)->deferred_destroy_count && !ECS_P_(brk); ECS_P_(k)++)                                                                    \
    for (int ECS_P_(eid) = ECS_P_(w)->deferred_destroys[ECS_P_(k)],                                                                                     \
             ECS_P_(ai) = ECS_P_(w)->entities[ECS_P_(eid)].archetype;                                                                                   \
         ECS_P_(ai) >= 0; ECS_P_(ai) = -1)                                                                                                              \
    for (ecs_archetype *ECS_P_(arch) = &ECS_P_(w)->archetypes[ECS_P_(ai)], *ECS_P_(a_once) = ECS_P_(arch); ECS_P_(a_once); ECS_P_(a_once) = NULL)        \
    if ((ECS_P_(arch)->mask & ECS_PMASK_ALL(__VA_ARGS__)) == ECS_PMASK_ALL(__VA_ARGS__))                                                                 \
    for (int ECS_P_(i) = ECS_P_(w)->entities[ECS_P_(eid)].row, *ECS_P_(i_once) = NULL; !ECS_P_(i_once); ECS_P_(i_once) = (int *)1)                      \
    ECS_PINJECT_VARS_(__VA_ARGS__)                                                                                                                       \
    for (int ECS_P_(done) = (ECS_P_(brk) = 1, 0); !ECS_P_(done); ECS_P_(done) = 1, ECS_P_(brk) = 0)

// clang-format on

/* --- cleanup -------------------------------------------------- */

static inline void ecs_world_destroy(ecs_world *w)
{
    for (int a = 0; a < w->archetype_count; a++) {
        ecs_archetype *arch = &w->archetypes[a];
        ecs_vm_free_(arch->entity_ids, (size_t)ECS_MAX_ENTITIES * sizeof(int));
#define ECS_FREE_(T, f)                                                        \
    if (arch->f) ecs_vm_free_(arch->f, (size_t)ECS_MAX_ENTITIES * sizeof(T));
        ECS_COMPONENTS(ECS_FREE_)
#undef ECS_FREE_
    }
    ecs_vm_free_(w->entities, (size_t)ECS_MAX_ENTITIES * sizeof(ecs_entity_info));
    ecs_vm_free_(w->free_list_next, (size_t)ECS_MAX_ENTITIES * sizeof(int));
    ecs_vm_free_(w->deferred_destroys, (size_t)ECS_MAX_ENTITIES * sizeof(int));
    ecs_vm_free_(w->deferred_dead, (size_t)ECS_MAX_ENTITIES * sizeof(bool));
    ecs_vm_free_(w->excluded, (size_t)ECS_MAX_ENTITIES * sizeof(bool));
}

#endif /* ARCH_ECS_H */
#elif !defined(ARCH_ECS_ENTITY_H)
#error "Define ECS_COMPONENTS(X) before including arch_ecs.h"
#endif /* ECS_COMPONENTS */
