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
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifndef ECS_MAX_ENTITIES
#define ECS_MAX_ENTITIES 4096
#endif
#ifndef ECS_MAX_ARCHETYPES
#define ECS_MAX_ARCHETYPES 64
#endif

/* --- entity handle (always available) ------------------------- */

#ifndef ARCH_ECS_ENTITY_H
#define ARCH_ECS_ENTITY_H

typedef uint64_t ecs_entity;
#define ECS_NULL ((ecs_entity)0)

/* Internal: union for field access within arch_ecs implementation. */
typedef union {
    struct { int id; uint16_t gen; uint16_t _pad; };
    uint64_t bits;
} ecs_entity_;

static inline ecs_entity_ ecs_unpack_(ecs_entity e) { return (ecs_entity_){ .bits = e }; }
static inline ecs_entity  ecs_pack_(int id, uint16_t gen) { return (ecs_entity_){ .id = id, .gen = gen }.bits; }

typedef struct ecs_world ecs_world;

#endif /* ARCH_ECS_ENTITY_H */

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
    _Atomic(int) count;
    int *entity_ids;
#define ECS_ARCH_COL_(T, f) T *f;
    ECS_COMPONENTS(ECS_ARCH_COL_)
#undef ECS_ARCH_COL_
} ecs_archetype;

/* --- world ---------------------------------------------------- */

struct ecs_world
{
    ecs_entity_info entities[ECS_MAX_ENTITIES];
    int free_list_next[ECS_MAX_ENTITIES]; /* per-entity linked list, 0 = end */
    _Atomic(int) free_list_head;          /* CAS stack head, 0 = empty */
    _Atomic(int) hwm;
    _Atomic(int) alive_count;
    ecs_archetype archetypes[ECS_MAX_ARCHETYPES];
    _Atomic(int) archetype_count;
    bool iterating;
    int deferred_kills[ECS_MAX_ENTITIES];
    int deferred_kill_count;
    bool deferred_dead[ECS_MAX_ENTITIES];
};

/* --- alive check ---------------------------------------------- */

static inline bool ecs_alive(ecs_world *w, ecs_entity e)
{
    ecs_entity_ u = ecs_unpack_(e);
    return u.id > 0 && u.id < ECS_MAX_ENTITIES &&
           w->entities[u.id].gen == u.gen && !w->deferred_dead[u.id];
}

/* --- lock-free free list -------------------------------------- */

static inline int ecs_free_list_pop_(ecs_world *w)
{
    int old_head, next;
    do {
        old_head = atomic_load_explicit(&w->free_list_head, memory_order_acquire);
        if (old_head == 0) return 0;
        next = w->free_list_next[old_head];
    } while (!atomic_compare_exchange_weak_explicit(
        &w->free_list_head, &old_head, next,
        memory_order_acq_rel, memory_order_acquire));
    return old_head;
}

static inline void ecs_free_list_push_(ecs_world *w, int id)
{
    int old_head;
    do {
        old_head = atomic_load_explicit(&w->free_list_head, memory_order_relaxed);
        w->free_list_next[id] = old_head;
    } while (!atomic_compare_exchange_weak_explicit(
        &w->free_list_head, &old_head, id,
        memory_order_release, memory_order_relaxed));
}

/* --- archetype helpers ---------------------------------------- */

static inline int ecs_find_or_create_archetype_(ecs_world *w, uint64_t mask)
{
    int n = atomic_load_explicit(&w->archetype_count, memory_order_acquire);
    for (int a = 0; a < n; a++)
        if (w->archetypes[a].mask == mask) return a;

    int a = atomic_fetch_add_explicit(&w->archetype_count, 1, memory_order_relaxed);
    assert(a < ECS_MAX_ARCHETYPES);

    /* Re-check: another thread may have created this mask concurrently */
    for (int i = 0; i < a; i++)
        if (w->archetypes[i].mask == mask) return i;

    ecs_archetype *arch = &w->archetypes[a];
    arch->entity_ids = ecs_vm_reserve_((size_t)ECS_MAX_ENTITIES * sizeof(int));
#define ECS_ALLOC_(T, f)                                                       \
    if (mask & ECS_MASK(f))                                                    \
        arch->f = ecs_vm_reserve_((size_t)ECS_MAX_ENTITIES * sizeof(T));
    ECS_COMPONENTS(ECS_ALLOC_)
#undef ECS_ALLOC_
    arch->mask = mask; /* publish last — scanners won't match until set */
    return a;
}

/* --- spawn ---------------------------------------------------- */

static inline ecs_entity ecs_spawn_(ecs_world *w, uint64_t mask)
{
    int id = ecs_free_list_pop_(w);
    if (id == 0) {
        id = atomic_fetch_add_explicit(&w->hwm, 1, memory_order_relaxed) + 1;
        assert(id < ECS_MAX_ENTITIES);
    }
    w->entities[id].gen++;

    int ai = ecs_find_or_create_archetype_(w, mask);
    ecs_archetype *arch = &w->archetypes[ai];

    int row = atomic_fetch_add_explicit(&arch->count, 1, memory_order_relaxed);
    assert(row < ECS_MAX_ENTITIES);
    arch->entity_ids[row] = id;

    w->entities[id].archetype = ai;
    w->entities[id].row = row;
    atomic_fetch_add_explicit(&w->alive_count, 1, memory_order_relaxed);

    return ecs_pack_(id, w->entities[id].gen);
}

/* clang-format off */
#define ecs_spawn(w, ...) ecs_spawn_((w), ECS_TMASK_ALL_TYPES(__VA_ARGS__))

/* Field-name-based spawn — avoids _Generic, works with type aliases.
   Usage: ecs_spawn_f(w, transform, velocity, player_tag) */
#define ecs_spawn_f(w, ...) ecs_spawn_((w), ECS_MASK_ALL(__VA_ARGS__))
/* clang-format on */

/* --- clone ---------------------------------------------------- */

static inline ecs_entity ecs_clone(ecs_world *w, ecs_entity src)
{
    assert(ecs_alive(w, src));
    ecs_entity_info *si = &w->entities[ecs_unpack_(src).id];
    ecs_archetype *sa = &w->archetypes[si->archetype];
    ecs_entity dst = ecs_spawn_(w, sa->mask);
    ecs_entity_info *di = &w->entities[ecs_unpack_(dst).id];
    ecs_archetype *da = &w->archetypes[di->archetype];
#define ECS_CLONE_(T, f) \
    if (sa->mask & ECS_MASK(f)) da->f[di->row] = sa->f[si->row];
    ECS_COMPONENTS(ECS_CLONE_)
#undef ECS_CLONE_
    return dst;
}

/* --- deferred kill flush -------------------------------------- */

static inline void ecs_flush_kills_(ecs_world *w)
{
    for (int k = 0; k < w->deferred_kill_count; k++) {
        int id = w->deferred_kills[k];
        w->deferred_dead[id] = false;
        ecs_entity_info *info = &w->entities[id];
        ecs_archetype *arch = &w->archetypes[info->archetype];
        int row = info->row;
        int last = atomic_fetch_sub_explicit(&arch->count, 1, memory_order_relaxed) - 1;

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
    w->deferred_kill_count = 0;
}

/* --- kill (swap-remove or deferred) --------------------------- */

static inline void ecs_kill(ecs_world *w, ecs_entity e)
{
    if (!ecs_alive(w, e)) return;
    int eid = ecs_unpack_(e).id;
    ecs_entity_info *info = &w->entities[eid];

    if (w->iterating) {
        info->gen++;
        w->deferred_dead[eid] = true;
        w->deferred_kills[w->deferred_kill_count++] = eid;
        atomic_fetch_sub_explicit(&w->alive_count, 1, memory_order_relaxed);
        return;
    }

    ecs_archetype *arch = &w->archetypes[info->archetype];
    int row = info->row;
    int last = atomic_fetch_sub_explicit(&arch->count, 1, memory_order_relaxed) - 1;

    if (row != last) {
        int swapped_id = arch->entity_ids[last];
        arch->entity_ids[row] = swapped_id;
#define ECS_SWAP_(T, f)                                                        \
    if (arch->mask & ECS_MASK(f)) arch->f[row] = arch->f[last];
        ECS_COMPONENTS(ECS_SWAP_)
#undef ECS_SWAP_
        w->entities[swapped_id].row = row;
    }

    info->gen++;
    info->archetype = -1;
    ecs_free_list_push_(w, eid);
    atomic_fetch_sub_explicit(&w->alive_count, 1, memory_order_relaxed);
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

/* clang-format off */
/* Generic setter: ecs_set(w, e, (Type){ ... }) */
#define ECS_GENERIC_SET_(T, f) T: ecs_set_##f,
#define ecs_set(w, e, ...)                                                     \
    _Generic((__VA_ARGS__),                                                    \
        ECS_COMPONENTS(ECS_GENERIC_SET_)                                       \
        default: (void)0                                                       \
    )((w), (e), __VA_ARGS__)
/* clang-format on */

/* clang-format off */
/* Token-pasting accessor: ECS_GET(w, e, TypeName) -> TypeName*
   Maps Type to field via per-type macros generated from ECS_COMPONENTS. */
#define ECS_GET_MAP_(T, f) static inline T *ecs__get_##T(ecs_world *w, ecs_entity e) { return ecs_get_##f(w, e); }
ECS_COMPONENTS(ECS_GET_MAP_)
#undef ECS_GET_MAP_

#define ECS_GET(w, e, T) ecs__get_##T((w), (e))

/* Token-pasting add (alias for get — component already in archetype) */
#define ECS_ADD(w, e, T) ECS_GET(w, e, T)

/* Token-pasting has check — uses field mask directly to avoid _Generic issues */
#define ECS_HAS_MAP_(T, f)                                                     \
    static inline bool ecs__has_##T(ecs_world *w, ecs_entity e) {              \
        if (!ecs_alive(w, e)) return false;                                    \
        return (w->archetypes[w->entities[ecs_unpack_(e).id].archetype].mask & ECS_MASK(f)) != 0; \
    }
ECS_COMPONENTS(ECS_HAS_MAP_)
#undef ECS_HAS_MAP_

#define ECS_HAS(w, e, T) ecs__has_##T((w), (e))
/* clang-format on */

/* --- queries -------------------------------------------------- */

#define ecs_has(w, e, T)                                                       \
    (ecs_alive((w), (e)) &&                                                    \
     ((w)->archetypes[(w)->entities[ecs_unpack_(e).id].archetype].mask &       \
      ECS_TMASK_TYPE_(T)))

#define ecs_has_all(w, e, ...)                                                 \
    (ecs_alive((w), (e)) &&                                                    \
     (((w)->archetypes[(w)->entities[ecs_unpack_(e).id].archetype].mask &      \
       ECS_TMASK_ALL_TYPES(__VA_ARGS__)) == ECS_TMASK_ALL_TYPES(__VA_ARGS__)))

static inline ecs_entity ecs_first_(ecs_world *w, uint64_t mask)
{
    int n = atomic_load_explicit(&w->archetype_count, memory_order_relaxed);
    for (int a = 0; a < n; a++) {
        ecs_archetype *arch = &w->archetypes[a];
        if ((arch->mask & mask) == mask && atomic_load_explicit(&arch->count, memory_order_relaxed) > 0) {
            int id = arch->entity_ids[0];
            return ecs_pack_(id, w->entities[id].gen);
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
   Uses ECS_TR_##TypeName macros (defined in ecs_components.h alongside ECS_TF_).
   Each ECS_TR_<Type> expands to the SoA field access expression.
   ecs_entity has ECS_TR_ecs_entity returning a compound-literal entity handle. */
#define ECS_TR_FIELD_(f) &ECS_P_(arch)->f[ECS_P_(i)]
#define ECS_TR_ecs_entity \
    (ecs_entity[1]){ ecs_pack_(ECS_P_(arch)->entity_ids[ECS_P_(i)],               \
                               ECS_P_(w)->entities[ECS_P_(arch)->entity_ids[ECS_P_(i)]].gen) }
#define ECS_RESOLVE_(TypeName) ECS_TR_##TypeName

/* Flat-pair mask fold: consume (Type, name) pairs, map Type to field via ECS_TF_##T.
   ECS_MASK_IND_ forces expansion of ECS_TF_##T before token-pasting with ECS_ID_##.
   ecs_entity contributes 0 via _Generic (unique type, no collision risk). */
#define ECS_MASK_IND_(f) ECS_MASK(f)
#define ECS_PMASK_ONE_(T) _Generic((T){0}, ecs_entity: (uint64_t)0, default: ECS_MASK_IND_(ECS_TF_##T))
#define ECS_PM_FOLD_(T, name, ...) ECS_PMASK_ONE_(T) __VA_OPT__(| ECS_M_DEFER_(ECS_PM_FOLD_C_)()(__VA_ARGS__))
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

/* ECS_NONE_OF_F — exclude by field names (avoids _Generic for aliased types). */
#define ECS_NONE_OF_F(...) if (!(ECS_P_(arch)->mask & ECS_MASK_ALL(__VA_ARGS__)))

// clang-format on

/* --- cleanup -------------------------------------------------- */

static inline void ecs_world_destroy(ecs_world *w)
{
    int n = atomic_load_explicit(&w->archetype_count, memory_order_relaxed);
    for (int a = 0; a < n; a++) {
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
#elif !defined(ARCH_ECS_ENTITY_H)
#error "Define ECS_COMPONENTS(X) before including arch_ecs.h"
#endif /* ECS_COMPONENTS */
