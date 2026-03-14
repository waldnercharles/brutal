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

    PARALLEL ITERATION
        void move(ecs_task *t) {
            for (int i = 0; i < t->n; i++)
                t->position[i].x += t->velocity[i].vx;
        }
        ecs_set_threads(&world, enqueue, wait, pool, 4);
        ecs_parallel_for(&world, move, Position, Velocity);

    HANDLES
    ecs_entity is a thin (id, gen) pair — 8 bytes.
    ecs_alive() catches stale handles after kill or slot reuse.
    Double-kill is safe. Kill-during-parallel-iteration uses per-task kill lists.
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
#ifndef ECS_MT_MAX_TASKS
#define ECS_MT_MAX_TASKS 64
#endif
#ifndef ECS_MT_KILL_LIST_SIZE
#define ECS_MT_KILL_LIST_SIZE 256
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

static _Thread_local int ecs_tls_task_index_;

/* --- thread pool callbacks ------------------------------------ */

typedef int (*ecs_enqueue_fn)(int (*fn)(void *), void *arg, void *udata);
typedef void (*ecs_wait_fn)(void *udata);

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
    _Atomic(int) count;
    int *entity_ids;
#define ECS_ARCH_COL_(T, f) T *f;
    ECS_COMPONENTS(ECS_ARCH_COL_)
#undef ECS_ARCH_COL_
} ecs_archetype;

/* --- world (forward decl) ------------------------------------ */

typedef struct ecs_world ecs_world;

/* --- task (parallel iteration slice) ------------------------- */

typedef struct ecs_task ecs_task;
struct ecs_task
{
    void (*fn)(ecs_task *);
    ecs_world *w;
    int n;
    int *entity_ids;
    int task_index;
#define ECS_TASK_PTR_(T, f) T *f;
    ECS_COMPONENTS(ECS_TASK_PTR_)
#undef ECS_TASK_PTR_
};

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
    /* threading */
    ecs_enqueue_fn enqueue;
    ecs_wait_fn wait;
    void *task_udata;
    int max_tasks;
    int min_entities_per_task;
    ecs_task par_tasks[ECS_MT_MAX_TASKS];
    bool par_iterating;
    int par_kills[ECS_MT_MAX_TASKS][ECS_MT_KILL_LIST_SIZE];
    int par_kill_counts[ECS_MT_MAX_TASKS];
};

/* --- alive check ---------------------------------------------- */

static inline bool ecs_alive(ecs_world *w, ecs_entity e)
{
    return e.id > 0 && e.id < ECS_MAX_ENTITIES &&
           w->entities[e.id].gen == e.gen && !w->deferred_dead[e.id];
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

    return (ecs_entity){ .id = id, .gen = w->entities[id].gen };
}

/* clang-format off */
#define ecs_spawn(w, ...) ecs_spawn_((w), ECS_TMASK_ALL_TYPES(__VA_ARGS__))
/* clang-format on */

/* --- clone ---------------------------------------------------- */

static inline ecs_entity ecs_clone(ecs_world *w, ecs_entity src)
{
    assert(ecs_alive(w, src));
    ecs_entity_info *si = &w->entities[src.id];
    ecs_archetype *sa = &w->archetypes[si->archetype];
    ecs_entity dst = ecs_spawn_(w, sa->mask);
    ecs_entity_info *di = &w->entities[dst.id];
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

/* --- parallel kill flush ------------------------------------- */

static inline void ecs_par_flush_kills_(ecs_world *w)
{
    int max = w->max_tasks > 0 ? w->max_tasks : 1;
    for (int t = 0; t < max; t++) {
        for (int k = 0; k < w->par_kill_counts[t]; k++) {
            int id = w->par_kills[t][k];
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
            atomic_fetch_sub_explicit(&w->alive_count, 1, memory_order_relaxed);
        }
        w->par_kill_counts[t] = 0;
    }
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
        atomic_fetch_sub_explicit(&w->alive_count, 1, memory_order_relaxed);
        return;
    }

    if (w->par_iterating) {
        info->gen++;
        int task = ecs_tls_task_index_;
        assert(w->par_kill_counts[task] < ECS_MT_KILL_LIST_SIZE);
        w->par_kills[task][w->par_kill_counts[task]++] = e.id;
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
    ecs_free_list_push_(w, e.id);
    atomic_fetch_sub_explicit(&w->alive_count, 1, memory_order_relaxed);
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
    int n = atomic_load_explicit(&w->archetype_count, memory_order_relaxed);
    for (int a = 0; a < n; a++) {
        ecs_archetype *arch = &w->archetypes[a];
        if ((arch->mask & mask) == mask && atomic_load_explicit(&arch->count, memory_order_relaxed) > 0) {
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

/* --- ecs_parallel_for ----------------------------------------- */
/*
    void move(ecs_task *t) {
        for (int i = 0; i < t->n; i++)
            t->position[i].x += t->velocity[i].vx;
    }
    ecs_parallel_for(&world, move, Position, Velocity);

    Falls back to single-threaded if no enqueue callback is set.
    ecs_kill_id during parallel iteration is safe (per-task kill lists).
*/

/* Fill task component pointers from archetype + start offset.
   ECS_PFILL_IMPL_ must stay defined (expanded at each call site). */
#define ECS_PFILL_IMPL_(T, f)                                                  \
    ECS_P_(w)->par_tasks[ECS_P_(pt)].f =                                      \
        (ECS_P_(pa)->mask & ECS_MASK(f))                                       \
            ? &ECS_P_(pa)->f[ECS_P_(ps)] : NULL;
#define ECS_PFILL_SET_(arch_, start_, tidx_)                                   \
    { ecs_archetype *ECS_P_(pa) = (arch_);                                     \
      int ECS_P_(ps) = (start_), ECS_P_(pt) = (tidx_);                        \
      ECS_COMPONENTS(ECS_PFILL_IMPL_) }

#define ecs_parallel_for(world, sys_fn, ...) do {                                                                          \
    ecs_world *ECS_P_(w) = (world);                                                                                        \
    uint64_t ECS_P_(mask) = ECS_TMASK_ALL_TYPES(__VA_ARGS__);                                                              \
    ECS_P_(w)->par_iterating = true;                                                                                       \
    for (int ECS_P_(a) = 0; ECS_P_(a) < ECS_P_(w)->archetype_count; ECS_P_(a)++) {                                          \
        ecs_archetype *ECS_P_(arch) = &ECS_P_(w)->archetypes[ECS_P_(a)];                                                    \
        int ECS_P_(cnt) = atomic_load_explicit(&ECS_P_(arch)->count, memory_order_relaxed);                                                                  \
        if ((ECS_P_(arch)->mask & ECS_P_(mask)) != ECS_P_(mask) || ECS_P_(cnt) == 0) continue;                                \
        int ECS_P_(tc) = ecs_compute_tasks_(ECS_P_(w), ECS_P_(cnt));                                                         \
        for (int ECS_P_(t) = 0; ECS_P_(t) < ECS_P_(tc); ECS_P_(t)++) {                                                      \
            int ECS_P_(start) = (ECS_P_(cnt) * ECS_P_(t)) / ECS_P_(tc);                                                     \
            int ECS_P_(end) = (ECS_P_(cnt) * (ECS_P_(t) + 1)) / ECS_P_(tc);                                                 \
            ECS_P_(w)->par_tasks[ECS_P_(t)].fn = (sys_fn);                                                                 \
            ECS_P_(w)->par_tasks[ECS_P_(t)].w = ECS_P_(w);                                                                 \
            ECS_P_(w)->par_tasks[ECS_P_(t)].n = ECS_P_(end) - ECS_P_(start);                                               \
            ECS_P_(w)->par_tasks[ECS_P_(t)].entity_ids = &ECS_P_(arch)->entity_ids[ECS_P_(start)];                          \
            ECS_P_(w)->par_tasks[ECS_P_(t)].task_index = ECS_P_(t);                                                        \
            ECS_PFILL_SET_(ECS_P_(arch), ECS_P_(start), ECS_P_(t))                                                          \
            if (ECS_P_(w)->enqueue)                                                                                         \
                ECS_P_(w)->enqueue(ecs_par_trampoline_, &ECS_P_(w)->par_tasks[ECS_P_(t)], ECS_P_(w)->task_udata);            \
            else {                                                                                                          \
                ecs_tls_task_index_ = ECS_P_(t);                                                                            \
                (sys_fn)(&ECS_P_(w)->par_tasks[ECS_P_(t)]);                                                                 \
            }                                                                                                               \
        }                                                                                                                   \
        if (ECS_P_(w)->enqueue) ECS_P_(w)->wait(ECS_P_(w)->task_udata);                                                     \
    }                                                                                                                       \
    ecs_par_flush_kills_(ECS_P_(w));                                                                                        \
    ECS_P_(w)->par_iterating = false;                                                                                       \
} while (0)

// clang-format on

/* --- parallel helpers ----------------------------------------- */

static int ecs_par_trampoline_(void *arg)
{
    ecs_task *t = arg;
    ecs_tls_task_index_ = t->task_index;
    t->fn(t);
    return 0;
}

static inline int ecs_compute_tasks_(ecs_world *w, int count)
{
    int min_per = w->min_entities_per_task > 0 ? w->min_entities_per_task : 64;
    int max = w->max_tasks > 0 ? w->max_tasks : 1;
    int tc = (count + min_per - 1) / min_per;
    if (tc < 1) tc = 1;
    if (tc > max) tc = max;
    return tc;
}

/* --- thread setup --------------------------------------------- */

static inline void ecs_set_threads(ecs_world *w, ecs_enqueue_fn enqueue,
                                   ecs_wait_fn wait, void *udata,
                                   int max_tasks)
{
    w->enqueue = enqueue;
    w->wait = wait;
    w->task_udata = udata;
    w->max_tasks = max_tasks > ECS_MT_MAX_TASKS ? ECS_MT_MAX_TASKS : max_tasks;
}

static inline void ecs_set_min_entities_per_task(ecs_world *w, int min_count)
{
    w->min_entities_per_task = min_count;
}

/* --- kill by id (for parallel iteration) ---------------------- */

static inline void ecs_kill_id(ecs_world *w, int id)
{
    ecs_kill(w, (ecs_entity){ .id = id, .gen = w->entities[id].gen });
}

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
#else
#error "Define ECS_COMPONENTS(X) before including arch_ecs.h"
#endif /* ECS_COMPONENTS */
