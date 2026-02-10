/**
 * brutal_ecs.h - Spartan single-header ECS
 *
 * Thread-safe entity component system with automatic command buffering for
 * structural changes during system execution.
 *
 * Heavily inspired by: https://github.com/empyreanx/pico_headers/blob/main/pico_ecs.h
 *
 * USAGE EXAMPLE:
 *   typedef struct { float x, y; } Position;
 *   ECS_DEFINE(Position);
 *   // Use ECS_DECLARE(Position) if separating declaration from definition.
 *
 *   static int move_system(ecs_t *ecs, ecs_view *view, void *udata)
 *   {
 *       (void)udata;
 *       for (int i = 0; i < view->count; i++) {
 *           ecs_entity e = view->entities[i];
 *           Position *pos = ECS_GET(ecs, e, Position);
 *           pos->x += 1.0f;
 *       }
 *       return 0;
 *   }
 *
 *   int main()
 *   {
 *       ecs_t *ecs = ecs_new();
 *       ECS_REGISTER(ecs, Position);
 *       ecs_entity e = ecs_create(ecs);
 *       Position *pos = ECS_ADD(ecs, e, Position);
 *       pos->x = 0.0f;
 *       pos->y = 0.0f;
 *
 *       ecs_sys_t move = ecs_sys_create(ecs, move_system, NULL);
 *       ECS_REQUIRE(ecs, move, Position);
 *       ecs_progress(ecs, 0);
 *
 *       ecs_free(ecs);
 *       return 0;
 *   }
 *
 * To use this library in your project, add the following
 *
 * #define BRUTAL_ECS_IMPLEMENTATION
 * #include "brutal_ecs.h"
 *
 * to a source file (once), then simply include the header normally.
 *
 * REQUIREMENTS:
 *   - C11 atomics (stdatomic.h)
 */

#ifndef BRUTAL_ECS_H
#define BRUTAL_ECS_H

// -----------------------------------------------------------------------------
//  Configuration

#ifndef ECS_MAX_COMPONENTS
#define ECS_MAX_COMPONENTS 64
#endif

#ifndef ECS_MAX_SYSTEMS
#define ECS_MAX_SYSTEMS 256
#endif

#ifndef ECS_MT_MAX_TASKS
#define ECS_MT_MAX_TASKS 1024
#endif

#ifndef ECS_CACHE_LINE
#define ECS_CACHE_LINE 64
#endif

// -----------------------------------------------------------------------------
//  Public API

typedef int ecs_entity;
typedef unsigned char ecs_comp_t;
typedef int ecs_sys_t;

struct ecs_s;
typedef struct ecs_s ecs_t;

// View of matching entities passed to system callbacks
typedef struct
{
    ecs_entity *entities;
    int count;
} ecs_view;

typedef int (*ecs_system_fn)(ecs_t *ecs, ecs_view *view, void *udata);
typedef int (*ecs_enqueue_task_fn)(int (*fn)(void *args), void *fn_args, void *udata);
typedef void (*ecs_wait_tasks_fn)(void *udata);

#include <stdbool.h>

// clang-format off
// Component ID macros — derive a variable name from the type
#define ECS_COMP_ID(Type)       _ecs_comp_##Type
#define ECS_DECLARE(Type)       extern ecs_comp_t ECS_COMP_ID(Type)
#define ECS_DEFINE(Type)        ecs_comp_t ECS_COMP_ID(Type)
#define ECS_REGISTER(ecs, Type) (ECS_COMP_ID(Type) = ecs_register_component((ecs), (int)sizeof(Type)))

// Type-safe component access
#define ECS_GET(ecs, entity, Type) ((Type *)ecs_get((ecs), (entity), ECS_COMP_ID(Type)))
#define ECS_ADD(ecs, entity, Type) ((Type *)ecs_add((ecs), (entity), ECS_COMP_ID(Type)))
#define ECS_HAS(ecs, entity, Type) ecs_has((ecs), (entity), ECS_COMP_ID(Type))
#define ECS_REMOVE(ecs, entity, Type) ecs_remove((ecs), (entity), ECS_COMP_ID(Type))

// Type-safe system query
#define ECS_REQUIRE(ecs, sys, Type) ecs_sys_require((ecs), (sys), ECS_COMP_ID(Type))
#define ECS_EXCLUDE(ecs, sys, Type) ecs_sys_exclude((ecs), (sys), ECS_COMP_ID(Type))
// clang-format on

// Core
ecs_t *ecs_new();
void ecs_free(ecs_t *ecs);
void ecs_set_task_callbacks(
    ecs_t *ecs,
    ecs_enqueue_task_fn enqueue_cb,
    ecs_wait_tasks_fn wait_cb,
    void *task_udata,
    int task_count
);

// Entities
ecs_entity ecs_create(ecs_t *ecs);
void ecs_destroy(ecs_t *ecs, ecs_entity e);

// Components
ecs_comp_t ecs_register_component(ecs_t *ecs, int size);
void *ecs_add(ecs_t *ecs, ecs_entity entity, ecs_comp_t component);
void ecs_remove(ecs_t *ecs, ecs_entity entity, ecs_comp_t component);
void *ecs_get(ecs_t *ecs, ecs_entity entity, ecs_comp_t component);
bool ecs_has(ecs_t *ecs, ecs_entity entity, ecs_comp_t component);

// Systems
ecs_sys_t ecs_sys_create(ecs_t *ecs, ecs_system_fn fn, void *udata);
void ecs_sys_require(ecs_t *ecs, ecs_sys_t sys, ecs_comp_t comp);
void ecs_sys_exclude(ecs_t *ecs, ecs_sys_t sys, ecs_comp_t comp);
void ecs_sys_enable(ecs_t *ecs, ecs_sys_t sys);
void ecs_sys_disable(ecs_t *ecs, ecs_sys_t sys);
void ecs_sys_set_parallel(ecs_t *ecs, ecs_sys_t sys, bool parallel);
void ecs_sys_set_group(ecs_t *ecs, ecs_sys_t sys, int group);
int ecs_sys_get_group(ecs_t *ecs, ecs_sys_t sys);
void ecs_sys_set_udata(ecs_t *ecs, ecs_sys_t sys, void *udata);
void *ecs_sys_get_udata(ecs_t *ecs, ecs_sys_t sys);

// Execution
int ecs_run_system(ecs_t *ecs, ecs_sys_t sys);
int ecs_progress(ecs_t *ecs, int group_mask);

#endif // BRUTAL_ECS_H

// -----------------------------------------------------------------------------
//  Implementation

#ifdef BRUTAL_ECS_IMPLEMENTATION

#include <assert.h>
#include <stdalign.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifndef ECS_CMD_BUFFER_CAPACITY
#define ECS_CMD_BUFFER_CAPACITY 1024
#endif

// -----------------------------------------------------------------------------
//  Bitset

#define ECS_BS_WORD_BITS 64
#define ECS_BS_WORDS                                                           \
    ((ECS_MAX_COMPONENTS + (ECS_BS_WORD_BITS - 1)) / ECS_BS_WORD_BITS)

#if defined(_MSC_VER)
#include <intrin.h>
static inline int ecs_ctz64(uint64_t x)
{
    unsigned long index;
    _BitScanForward64(&index, x);
    return (int)index;
}
static inline int ecs_popcnt64(uint64_t x)
{
    return (int)__popcnt64(x);
}
#elif defined(__GNUC__) || defined(__clang__)
static inline int ecs_ctz64(uint64_t x)
{
    return __builtin_ctzll(x);
}
static inline int ecs_popcnt64(uint64_t x)
{
    return __builtin_popcountll(x);
}
#else
// clang-format off
static inline int ecs_ctz64(uint64_t x)
{
    int n = 0;
    if ((x & 0x00000000FFFFFFFFull) == 0) { n += 32; x >>= 32; }
    if ((x & 0x000000000000FFFFull) == 0) { n += 16; x >>= 16; }
    if ((x & 0x00000000000000FFull) == 0) { n += 8;  x >>= 8;  }
    if ((x & 0x000000000000000Full) == 0) { n += 4;  x >>= 4;  }
    if ((x & 0x0000000000000003ull) == 0) { n += 2;  x >>= 2;  }
    if ((x & 0x0000000000000001ull) == 0) { n += 1; }
    return n;
}
static inline int ecs_popcnt64(uint64_t x)
{
    x = x - ((x >> 1) & 0x5555555555555555ull);
    x = (x & 0x3333333333333333ull) + ((x >> 2) & 0x3333333333333333ull);
    return (int)(((x + (x >> 4)) & 0x0F0F0F0F0F0F0F0Full) * 0x0101010101010101ull >> 56);
}
// clang-format on
#endif

#if ECS_BS_WORDS == 1

typedef uint64_t ecs_bitset;

static inline void ecs_bs_zero(ecs_bitset *bs)
{
    *bs = 0;
}
static inline bool ecs_bs_any(ecs_bitset *bs)
{
    return *bs != 0;
}
static inline bool ecs_bs_none(ecs_bitset *bs)
{
    return *bs == 0;
}
static inline void ecs_bs_copy(ecs_bitset *dst, ecs_bitset *src)
{
    *dst = *src;
}

static inline void ecs_bs_set(ecs_bitset *bs, int bit)
{
    assert(bit < ECS_MAX_COMPONENTS);
    *bs |= (1ull << bit);
}

static inline void ecs_bs_clear(ecs_bitset *bs, int bit)
{
    assert(bit < ECS_MAX_COMPONENTS);
    *bs &= ~(1ull << bit);
}

static inline bool ecs_bs_test(ecs_bitset *bs, int bit)
{
    assert(bit < ECS_MAX_COMPONENTS);
    return (*bs >> bit) & 1ull;
}

static inline void ecs_bs_or(ecs_bitset *dst, ecs_bitset *a, ecs_bitset *b)
{
    *dst = *a | *b;
}
static inline void ecs_bs_and(ecs_bitset *dst, ecs_bitset *a, ecs_bitset *b)
{
    *dst = *a & *b;
}
static inline void ecs_bs_andnot(ecs_bitset *dst, ecs_bitset *a, ecs_bitset *b)
{
    *dst = *a & ~*b;
}
static inline bool ecs_bs_intersects(ecs_bitset *a, ecs_bitset *b)
{
    return (*a & *b) != 0;
}
static inline void ecs_bs_or_into(ecs_bitset *dst, ecs_bitset *a)
{
    *dst |= *a;
}
static inline int ecs_bs_popcount(ecs_bitset *bs)
{
    return ecs_popcnt64(*bs);
}
static inline bool ecs_bs_contains(ecs_bitset *a, ecs_bitset *b)
{
    return (*a & *b) == *b;
}

#define ECS_BS_FOREACH(bs, bit_var)                                            \
    for (uint64_t _w = *(bs); _w; _w &= (_w - 1ull))                           \
        for (int bit_var = ecs_ctz64(_w), _once = 1; _once; _once = 0)

#else

typedef struct
{
    uint64_t words[ECS_BS_WORDS];
} ecs_bitset;

static inline void ecs_bs_zero(ecs_bitset *bs)
{
    memset(bs->words, 0, sizeof(bs->words));
}

static inline bool ecs_bs_any(ecs_bitset *bs)
{
    for (int i = 0; i < ECS_BS_WORDS; i++)
        if (bs->words[i]) return true;

    return false;
}

static inline bool ecs_bs_none(ecs_bitset *bs)
{
    return !ecs_bs_any(bs);
}

static inline void ecs_bs_copy(ecs_bitset *dst, ecs_bitset *src)
{
    memcpy(dst->words, src->words, sizeof(dst->words));
}

static inline void ecs_bs_set(ecs_bitset *bs, int bit)
{
    assert(bit < ECS_MAX_COMPONENTS);
    bs->words[bit >> 6] |= (1ull << (bit & 63u));
}

static inline void ecs_bs_clear(ecs_bitset *bs, int bit)
{
    assert(bit < ECS_MAX_COMPONENTS);
    bs->words[bit >> 6] &= ~(1ull << (bit & 63u));
}

static inline bool ecs_bs_test(ecs_bitset *bs, int bit)
{
    assert(bit < ECS_MAX_COMPONENTS);
    return ((bs->words[bit >> 6] >> (bit & 63u)) & 1ull);
}

static inline void ecs_bs_or(ecs_bitset *dst, ecs_bitset *a, ecs_bitset *b)
{
    for (int i = 0; i < ECS_BS_WORDS; i++)
        dst->words[i] = a->words[i] | b->words[i];
}

static inline void ecs_bs_and(ecs_bitset *dst, ecs_bitset *a, ecs_bitset *b)
{
    for (int i = 0; i < ECS_BS_WORDS; i++)
        dst->words[i] = a->words[i] & b->words[i];
}

static inline void ecs_bs_andnot(ecs_bitset *dst, ecs_bitset *a, ecs_bitset *b)
{
    for (int i = 0; i < ECS_BS_WORDS; i++)
        dst->words[i] = a->words[i] & ~b->words[i];
}

static inline bool ecs_bs_intersects(ecs_bitset *a, ecs_bitset *b)
{
    for (int i = 0; i < ECS_BS_WORDS; i++)
        if (a->words[i] & b->words[i]) return true;
    return false;
}

static inline void ecs_bs_or_into(ecs_bitset *dst, ecs_bitset *a)
{
    for (int i = 0; i < ECS_BS_WORDS; i++) dst->words[i] |= a->words[i];
}

static inline int ecs_bs_popcount(ecs_bitset *bs)
{
    int n = 0;
    for (int i = 0; i < ECS_BS_WORDS; i++) n += ecs_popcnt64(bs->words[i]);
    return n;
}
static inline bool ecs_bs_contains(ecs_bitset *a, ecs_bitset *b)
{
    for (int i = 0; i < ECS_BS_WORDS; i++)
        if ((a->words[i] & b->words[i]) != b->words[i]) return false;
    return true;
}

#define ECS_BS_FOREACH(bs, bit_var)                                            \
    for (int _wi = 0; _wi < ECS_BS_WORDS; _wi++)                               \
        for (uint64_t _w = (bs)->words[_wi]; _w; _w &= (_w - 1ull))            \
            for (int bit_var = ecs_ctz64(_w) + (_wi * 64), _once = 1; _once; _once = 0)

#endif

// -----------------------------------------------------------------------------
//  Sparse Set

typedef struct
{
    int *sparse;
    ecs_entity *dense;
    int sparse_cap;
    int dense_cap;
    int count;
} ecs_sparse_set;

static inline void ecs_ss_init(ecs_sparse_set *set)
{
    memset(set, 0, sizeof(*set));
}

static inline void ecs_ss_free(ecs_sparse_set *set)
{
    free(set->sparse);
    free(set->dense);
    memset(set, 0, sizeof(*set));
}

static inline void ecs_ss_reserve_sparse(ecs_sparse_set *set, int need)
{
    if (need <= set->sparse_cap) return;
    int old = set->sparse_cap;
    int cap = old ? old : 1;
    while (cap < need) cap <<= 1;
    set->sparse = realloc(set->sparse, cap * sizeof(int));
    assert(set->sparse);
    memset(set->sparse + old, 0, (cap - old) * sizeof(int));
    set->sparse_cap = cap;
}

static inline void ecs_ss_reserve_dense(ecs_sparse_set *set, int need)
{
    if (need <= set->dense_cap) return;
    int cap = set->dense_cap ? set->dense_cap : 1;
    while (cap < need) cap <<= 1;
    set->dense = realloc(set->dense, cap * sizeof(ecs_entity));
    assert(set->dense);
    set->dense_cap = cap;
}

static inline bool ecs_ss_has(ecs_sparse_set *set, ecs_entity entity)
{
    return (entity < set->sparse_cap) && (set->sparse[entity] != 0);
}

static inline int ecs_ss_index_of(ecs_sparse_set *s, ecs_entity entity)
{
    assert(ecs_ss_has(s, entity));
    return s->sparse[entity] - 1;
}

static inline bool ecs_ss_insert(ecs_sparse_set *set, ecs_entity entity)
{
    ecs_ss_reserve_sparse(set, entity + 1);
    if (set->sparse[entity]) return false;
    ecs_ss_reserve_dense(set, set->count + 1);
    int idx = set->count++;
    set->dense[idx] = entity;
    set->sparse[entity] = idx + 1;
    return true;
}

static inline bool ecs_ss_remove(ecs_sparse_set *set, ecs_entity entity)
{
    if (entity >= set->sparse_cap) return false;
    int idx = set->sparse[entity];
    if (!idx) return false;

    idx--;
    int last = set->count - 1;
    ecs_entity last_id = set->dense[last];

    set->dense[idx] = last_id;
    set->count--;

    set->sparse[entity] = 0;
    if (idx != last) set->sparse[last_id] = idx + 1;
    return true;
}

// -----------------------------------------------------------------------------
//  Component Pool

typedef struct ecs_pool
{
    ecs_sparse_set set;
    void *data;
    int element_size;
} ecs_pool;

static inline void ecs_pool_init(ecs_pool *pool, int element_size)
{
    ecs_ss_init(&pool->set);
    pool->data = NULL;
    pool->element_size = element_size;
}

static inline void ecs_pool_free(ecs_pool *pool)
{
    free(pool->data);
    ecs_ss_free(&pool->set);
    memset(pool, 0, sizeof(*pool));
}

static inline void ecs_pool_reserve(ecs_pool *pool, int need)
{
    if (need <= pool->set.dense_cap) return;
    ecs_ss_reserve_dense(&pool->set, need);
    pool->data = realloc(pool->data, (size_t)pool->set.dense_cap * (size_t)pool->element_size);
    assert(pool->data);
}

static inline void *ecs_pool_ptr_at(ecs_pool *pool, int idx)
{
    return (uint8_t *)pool->data + (size_t)idx * (size_t)pool->element_size;
}

static inline void *ecs_pool_add(ecs_pool *pool, ecs_entity e)
{
    if (!ecs_ss_has(&pool->set, e)) {
        ecs_pool_reserve(pool, pool->set.count + 1);
        int idx = pool->set.count;
        (void)ecs_ss_insert(&pool->set, e);
        void *dst = ecs_pool_ptr_at(pool, idx);
        memset(dst, 0, (size_t)pool->element_size);
        return dst;
    }
    return ecs_pool_ptr_at(pool, ecs_ss_index_of(&pool->set, e));
}

static inline bool ecs_pool_remove(ecs_pool *pool, ecs_entity e)
{
    if (!ecs_ss_has(&pool->set, e)) return false;
    int idx = ecs_ss_index_of(&pool->set, e);
    int last = pool->set.count - 1;
    if (idx != last) {
        memcpy(
            ecs_pool_ptr_at(pool, idx),
            ecs_pool_ptr_at(pool, last),
            (size_t)pool->element_size
        );
    }
    return ecs_ss_remove(&pool->set, e);
}

static inline void *ecs_pool_get(ecs_pool *pool, ecs_entity e)
{
    return ecs_pool_ptr_at(pool, ecs_ss_index_of(&pool->set, e));
}

// -----------------------------------------------------------------------------
//  Command Buffer

typedef enum
{
    ECS_CMD_DESTROY,
    ECS_CMD_ADD,
    ECS_CMD_REMOVE
} ecs_cmd_type;

typedef struct
{
    ecs_cmd_type type;
    ecs_entity entity;
    ecs_comp_t component;
    void *component_data;
} ecs_cmd;

typedef struct
{
    alignas(ECS_CACHE_LINE) ecs_cmd *commands;
    int count;
    int capacity;

    uint8_t *data_buffer;
    int data_offset;
    int data_capacity;
} ecs_cmd_buffer;

// -----------------------------------------------------------------------------
//  ECS Core

typedef struct
{
    ecs_bitset all_of;
    ecs_bitset none_of;
    ecs_sparse_set matched;
    int group;
    ecs_system_fn fn;
    void *udata;
    bool enabled;
    bool parallel;
} ecs_system;

typedef struct
{
    ecs_t *ecs;
    int sys_index;
    int task_index;
} ecs_task_args;

struct ecs_s
{
    // Entities
    _Atomic(ecs_entity) next_entity;
    _Atomic(int) free_list_head;
    int *free_list_next;
    int free_list_capacity;
    ecs_bitset *entity_bits;
    int entity_bits_cap;

    // Components
    ecs_pool components[ECS_MAX_COMPONENTS];
    int comp_count;

    // Systems
    ecs_system systems[ECS_MAX_SYSTEMS];
    int system_count;

    // Multithreading
    ecs_enqueue_task_fn enqueue_cb;
    ecs_wait_tasks_fn wait_cb;
    void *task_udata;
    int task_count;

    bool in_progress;

    ecs_task_args task_args_storage[ECS_MT_MAX_TASKS];
    ecs_cmd_buffer cmd_buffers[ECS_MT_MAX_TASKS];
};

// -----------------------------------------------------------------------------
//  Command Buffer Implementation

static inline void ecs_cmd_buffer_init(ecs_cmd_buffer *cb, int initial_capacity)
{
    memset(cb, 0, sizeof(*cb));
    cb->capacity = initial_capacity;
    cb->commands = malloc((size_t)initial_capacity * sizeof(ecs_cmd));
    assert(cb->commands);

    cb->data_capacity = 1024 * 1024;
    cb->data_buffer = malloc((size_t)cb->data_capacity);
    assert(cb->data_buffer);
}

static inline void ecs_cmd_buffer_free(ecs_cmd_buffer *cb)
{
    free(cb->commands);
    free(cb->data_buffer);
    memset(cb, 0, sizeof(*cb));
}

static inline void ecs_cmd_buffer_grow(ecs_cmd_buffer *cb)
{
    int new_cap = cb->capacity * 2;
    cb->commands = realloc(cb->commands, (size_t)new_cap * sizeof(ecs_cmd));
    assert(cb->commands);
    cb->capacity = new_cap;
}

static inline void *ecs_cmd_alloc_data(ecs_cmd_buffer *cb, int size)
{
    if (cb->data_offset + size > cb->data_capacity) {
        int new_cap = cb->data_capacity * 2;
        while (new_cap < cb->data_offset + size) new_cap *= 2;
        cb->data_buffer = realloc(cb->data_buffer, (size_t)new_cap);
        assert(cb->data_buffer);
        cb->data_capacity = new_cap;
    }

    void *ptr = cb->data_buffer + cb->data_offset;
    cb->data_offset += size;
    return ptr;
}

static _Thread_local int ecs_tls_task_index = 0;

static inline void ecs_set_tls_task_index(int task_index)
{
    ecs_tls_task_index = task_index;
}

static inline ecs_cmd_buffer *ecs_current_cmd_buffer(ecs_t *ecs)
{
    int idx = ecs_tls_task_index;
    assert(idx >= 0 && idx < ecs->task_count);
    return &ecs->cmd_buffers[idx];
}

static inline void ecs_cmd_enqueue(ecs_t *ecs, ecs_cmd *cmd)
{
    ecs_cmd_buffer *cb = ecs_current_cmd_buffer(ecs);
    if (cb->count >= cb->capacity) ecs_cmd_buffer_grow(cb);
    cb->commands[cb->count++] = *cmd;
}

// -----------------------------------------------------------------------------
//  Free List

static inline ecs_entity ecs_free_list_pop(ecs_t *ecs)
{
    int old_head, new_head;
    do {
        old_head = atomic_load(&ecs->free_list_head);
        if (old_head == -1) return 0;
        new_head = ecs->free_list_next[old_head];
    } while (!atomic_compare_exchange_weak(&ecs->free_list_head, &old_head, new_head));
    return old_head;
}

static inline void ecs_free_list_push(ecs_t *ecs, ecs_entity entity)
{
    int old_head;
    do {
        old_head = atomic_load(&ecs->free_list_head);
        ecs->free_list_next[entity] = old_head;
    } while (!atomic_compare_exchange_weak(&ecs->free_list_head, &old_head, entity));
}

// -----------------------------------------------------------------------------
//  Entity Bitsets

static inline void ecs_ensure_entity_bits(ecs_t *ecs, ecs_entity e)
{
    int need = e + 1;
    if (need <= ecs->entity_bits_cap) return;
    int cap = ecs->entity_bits_cap ? ecs->entity_bits_cap : 1;
    while (cap < need) cap <<= 1;
    ecs->entity_bits = realloc(ecs->entity_bits, (size_t)cap * sizeof(ecs_bitset));
    assert(ecs->entity_bits);
    memset(
        ecs->entity_bits + ecs->entity_bits_cap,
        0,
        (size_t)(cap - ecs->entity_bits_cap) * sizeof(ecs_bitset)
    );
    ecs->entity_bits_cap = cap;
}

// -----------------------------------------------------------------------------
//  System Matching

static inline bool ecs_entity_matches_system(ecs_t *ecs, ecs_entity e, ecs_system *s)
{
    return e < ecs->entity_bits_cap &&
           ecs_bs_contains(&ecs->entity_bits[e], &s->all_of) &&
           (!ecs_bs_any(&s->none_of) ||
            !ecs_bs_intersects(&ecs->entity_bits[e], &s->none_of));
}

static inline void ecs_rebuild_system_matched(ecs_t *ecs, ecs_system *s)
{
    s->matched.count = 0;
    if (s->matched.sparse)
        memset(s->matched.sparse, 0, (size_t)s->matched.sparse_cap * sizeof(int));

    if (ecs_bs_none(&s->all_of)) return;

    int n = atomic_load(&ecs->next_entity);
    for (int e = 1; e < n && e < ecs->entity_bits_cap; e++) {
        if (ecs_entity_matches_system(ecs, e, s)) ecs_ss_insert(&s->matched, e);
    }
}

static inline void ecs_sync_entity_systems(ecs_t *ecs, ecs_entity entity)
{
    for (int i = 0; i < ecs->system_count; i++) {
        ecs_system *s = &ecs->systems[i];
        if (ecs_bs_none(&s->all_of)) continue;

        bool in_set = ecs_ss_has(&s->matched, entity);
        bool matches = ecs_entity_matches_system(ecs, entity, s);

        if (matches && !in_set) ecs_ss_insert(&s->matched, entity);
        else if (!matches && in_set) ecs_ss_remove(&s->matched, entity);
    }
}

// -----------------------------------------------------------------------------
//  Deferred Operations

static inline void *ecs_add_deferred(ecs_t *ecs, ecs_entity entity, ecs_comp_t component)
{
    assert(component < ecs->comp_count);

    int element_size = ecs->components[component].element_size;
    ecs_cmd_buffer *cb = ecs_current_cmd_buffer(ecs);
    void *data = ecs_cmd_alloc_data(cb, element_size);
    memset(data, 0, (size_t)element_size);

    ecs_cmd cmd = { .type = ECS_CMD_ADD,
                    .entity = entity,
                    .component = component,
                    .component_data = data };
    ecs_cmd_enqueue(ecs, &cmd);

    return data;
}

static inline void ecs_remove_deferred(ecs_t *ecs, ecs_entity entity, ecs_comp_t component)
{
    assert(component < ecs->comp_count);

    ecs_cmd cmd = { .type = ECS_CMD_REMOVE,
                    .entity = entity,
                    .component = component,
                    .component_data = NULL };
    ecs_cmd_enqueue(ecs, &cmd);
}

static inline void ecs_destroy_deferred(ecs_t *ecs, ecs_entity entity)
{
    ecs_cmd cmd = { .type = ECS_CMD_DESTROY,
                    .entity = entity,
                    .component = 0,
                    .component_data = NULL };
    ecs_cmd_enqueue(ecs, &cmd);
}

static inline void ecs_sync(ecs_t *ecs)
{
    assert(!ecs->in_progress);

    bool any_cmds = false;
    for (int t = 0; t < ecs->task_count; t++) {
        if (ecs->cmd_buffers[t].count) {
            any_cmds = true;
            break;
        }
    }
    if (!any_cmds) return;

    for (int t = 0; t < ecs->task_count; t++) {
        ecs_cmd_buffer *cb = &ecs->cmd_buffers[t];
        for (int i = 0; i < cb->count; i++) {
            ecs_cmd *cmd = &cb->commands[i];
            ecs_entity target = cmd->entity;

            switch (cmd->type) {
                case ECS_CMD_DESTROY: ecs_destroy(ecs, target); break;

                case ECS_CMD_ADD: {
                    assert(cmd->component < ecs->comp_count);
                    void *dst = ecs_pool_add(&ecs->components[cmd->component], target);
                    int elem_size = ecs->components[cmd->component].element_size;
                    memcpy(dst, cmd->component_data, (size_t)elem_size);
                    ecs_ensure_entity_bits(ecs, target);
                    ecs_bs_set(&ecs->entity_bits[target], cmd->component);
                    ecs_sync_entity_systems(ecs, target);
                    break;
                }

                case ECS_CMD_REMOVE:
                    assert(cmd->component < ecs->comp_count);
                    ecs_pool_remove(&ecs->components[cmd->component], target);
                    if (target < ecs->entity_bits_cap)
                        ecs_bs_clear(&ecs->entity_bits[target], cmd->component);
                    ecs_sync_entity_systems(ecs, target);
                    break;
            }
        }

        cb->count = 0;
        cb->data_offset = 0;
    }
}

static inline int ecs_run_system_task(void *args_v)
{
    ecs_task_args *args = (ecs_task_args *)args_v;
    ecs_t *ecs = args->ecs;
    ecs_system *s = &ecs->systems[args->sys_index];

    int count = s->matched.count;
    if (!count) return 0;

    ecs_set_tls_task_index(args->task_index);

    int task_count = ecs->task_count;
    int task_idx = args->task_index;
    int start = (count * task_idx) / task_count;
    int end = (count * (task_idx + 1)) / task_count;
    int slice_count = end - start;

    int ret = 0;
    if (slice_count > 0) {
        ecs_view view = { .entities = &s->matched.dense[start], .count = slice_count };
        ret = s->fn(ecs, &view, s->udata);
    }

    ecs_set_tls_task_index(0);
    return ret;
}

// -----------------------------------------------------------------------------
//  Public API Implementation

ecs_t *ecs_new()
{
    ecs_t *ecs = malloc(sizeof(ecs_t));
    assert(ecs);

    memset(ecs, 0, sizeof(*ecs));
    atomic_store(&ecs->next_entity, 1);
    atomic_store(&ecs->free_list_head, -1);
    ecs->task_count = 1;

    ecs->free_list_capacity = 1024;
    ecs->free_list_next = malloc((size_t)ecs->free_list_capacity * sizeof(int));
    assert(ecs->free_list_next);

    for (int i = 0; i < ECS_MT_MAX_TASKS; i++) {
        ecs_cmd_buffer_init(&ecs->cmd_buffers[i], ECS_CMD_BUFFER_CAPACITY);
    }

    return ecs;
}

void ecs_free(ecs_t *ecs)
{
    for (int i = 0; i < ecs->system_count; i++)
        ecs_ss_free(&ecs->systems[i].matched);

    for (int i = 0; i < ecs->comp_count; i++) ecs_pool_free(&ecs->components[i]);
    free(ecs->free_list_next);
    free(ecs->entity_bits);

    for (int i = 0; i < ECS_MT_MAX_TASKS; i++) {
        ecs_cmd_buffer_free(&ecs->cmd_buffers[i]);
    }

    free(ecs);
}

void ecs_set_task_callbacks(
    ecs_t *ecs,
    ecs_enqueue_task_fn enqueue_cb,
    ecs_wait_tasks_fn wait_cb,
    void *task_udata,
    int task_count
)
{
    ecs->enqueue_cb = enqueue_cb;
    ecs->wait_cb = wait_cb;
    ecs->task_udata = task_udata;
    if (task_count < 1) task_count = 1;
    if (task_count > ECS_MT_MAX_TASKS) task_count = ECS_MT_MAX_TASKS;
    ecs->task_count = task_count;
}

ecs_entity ecs_create(ecs_t *ecs)
{
    ecs_entity e = ecs_free_list_pop(ecs);
    if (!e) e = atomic_fetch_add(&ecs->next_entity, 1);
    return e;
}

void ecs_destroy(ecs_t *ecs, ecs_entity e)
{
    if (ecs->in_progress) {
        ecs_destroy_deferred(ecs, e);
        return;
    }

    for (int i = 0; i < ecs->system_count; i++)
        ecs_ss_remove(&ecs->systems[i].matched, e);

    for (int c = 0; c < ecs->comp_count; c++)
        (void)ecs_pool_remove(&ecs->components[c], e);

    if (e < ecs->entity_bits_cap) ecs_bs_zero(&ecs->entity_bits[e]);

    if (e >= ecs->free_list_capacity) {
        int new_cap = ecs->free_list_capacity * 2;
        while (new_cap <= e) new_cap *= 2;
        ecs->free_list_next = realloc(ecs->free_list_next, (size_t)new_cap * sizeof(int));
        assert(ecs->free_list_next);
        ecs->free_list_capacity = new_cap;
    }

    ecs_free_list_push(ecs, e);
}

ecs_comp_t ecs_register_component(ecs_t *ecs, int size)
{
    assert(ecs->comp_count < ECS_MAX_COMPONENTS);
    ecs_comp_t id = (ecs_comp_t)ecs->comp_count++;
    ecs_pool_init(&ecs->components[id], size);
    return id;
}

void *ecs_add(ecs_t *ecs, ecs_entity entity, ecs_comp_t component)
{
    if (ecs->in_progress) return ecs_add_deferred(ecs, entity, component);

    assert(component < ecs->comp_count);
    void *ptr = ecs_pool_add(&ecs->components[component], entity);
    ecs_ensure_entity_bits(ecs, entity);
    ecs_bs_set(&ecs->entity_bits[entity], component);
    ecs_sync_entity_systems(ecs, entity);
    return ptr;
}

void ecs_remove(ecs_t *ecs, ecs_entity entity, ecs_comp_t component)
{
    if (ecs->in_progress) {
        ecs_remove_deferred(ecs, entity, component);
        return;
    }

    assert(component < ecs->comp_count);
    (void)ecs_pool_remove(&ecs->components[component], entity);
    if (entity < ecs->entity_bits_cap)
        ecs_bs_clear(&ecs->entity_bits[entity], component);
    ecs_sync_entity_systems(ecs, entity);
}

void *ecs_get(ecs_t *ecs, ecs_entity entity, ecs_comp_t component)
{
    assert(component < ecs->comp_count);
    return ecs_pool_get(&ecs->components[component], entity);
}

bool ecs_has(ecs_t *ecs, ecs_entity entity, ecs_comp_t component)
{
    assert(component < ecs->comp_count);
    if (entity >= ecs->entity_bits_cap) return false;
    return ecs_bs_test(&ecs->entity_bits[entity], component);
}

ecs_sys_t ecs_sys_create(ecs_t *ecs, ecs_system_fn fn, void *udata)
{
    assert(ecs->system_count < ECS_MAX_SYSTEMS);
    assert(fn);

    ecs_sys_t sys = (ecs_sys_t)ecs->system_count++;
    ecs_system *s = &ecs->systems[sys];

    memset(s, 0, sizeof(*s));
    ecs_ss_init(&s->matched);
    s->fn = fn;
    s->udata = udata;
    s->enabled = true;

    return sys;
}

void ecs_sys_require(ecs_t *ecs, ecs_sys_t sys, ecs_comp_t comp)
{
    assert(sys >= 0 && sys < ecs->system_count);
    ecs_system *s = &ecs->systems[sys];
    ecs_bs_set(&s->all_of, comp);
    ecs_rebuild_system_matched(ecs, s);
}

void ecs_sys_exclude(ecs_t *ecs, ecs_sys_t sys, ecs_comp_t comp)
{
    assert(sys >= 0 && sys < ecs->system_count);
    ecs_system *s = &ecs->systems[sys];
    ecs_bs_set(&s->none_of, comp);
    ecs_rebuild_system_matched(ecs, s);
}

void ecs_sys_enable(ecs_t *ecs, ecs_sys_t sys)
{
    assert(sys >= 0 && sys < ecs->system_count);
    ecs->systems[sys].enabled = true;
}

void ecs_sys_disable(ecs_t *ecs, ecs_sys_t sys)
{
    assert(sys >= 0 && sys < ecs->system_count);
    ecs->systems[sys].enabled = false;
}

void ecs_sys_set_parallel(ecs_t *ecs, ecs_sys_t sys, bool parallel)
{
    assert(sys >= 0 && sys < ecs->system_count);
    ecs->systems[sys].parallel = parallel;
}

void ecs_sys_set_group(ecs_t *ecs, ecs_sys_t sys, int group)
{
    assert(sys >= 0 && sys < ecs->system_count);
    ecs->systems[sys].group = group;
}

int ecs_sys_get_group(ecs_t *ecs, ecs_sys_t sys)
{
    assert(sys >= 0 && sys < ecs->system_count);
    return ecs->systems[sys].group;
}

void ecs_sys_set_udata(ecs_t *ecs, ecs_sys_t sys, void *udata)
{
    assert(sys >= 0 && sys < ecs->system_count);
    ecs->systems[sys].udata = udata;
}

void *ecs_sys_get_udata(ecs_t *ecs, ecs_sys_t sys)
{
    assert(sys >= 0 && sys < ecs->system_count);
    return ecs->systems[sys].udata;
}

int ecs_run_system(ecs_t *ecs, ecs_sys_t sys)
{
    assert(sys >= 0 && sys < ecs->system_count);

    ecs_system *s = &ecs->systems[sys];
    if (!s->enabled) return 0;

    ecs->in_progress = true;

    bool mt = (s->parallel && ecs->enqueue_cb && ecs->wait_cb && ecs->task_count > 1);
    int ret = 0;

    if (!mt) {
        ecs_task_args a = { .ecs = ecs, .sys_index = sys, .task_index = 0 };
        ret = ecs_run_system_task(&a);
    } else {
        ecs_task_args *args = ecs->task_args_storage;

        for (int t = 0; t < ecs->task_count; t++) {
            args[t].ecs = ecs;
            args[t].sys_index = sys;
            args[t].task_index = t;

            int enqueue_ret = ecs->enqueue_cb(ecs_run_system_task, &args[t], ecs->task_udata);
            if (enqueue_ret) {
                ret = enqueue_ret;
                goto done;
            }
        }

        ecs->wait_cb(ecs->task_udata);
    }

done:
    ecs->in_progress = false;
    ecs_sync(ecs);
    return ret;
}

int ecs_progress(ecs_t *ecs, int group_mask)
{
    for (int i = 0; i < ecs->system_count; i++) {
        ecs_system *s = &ecs->systems[i];
        int matches = (group_mask == 0) ? (s->group == 0) : (s->group & group_mask);
        if (!matches) continue;

        int ret = ecs_run_system(ecs, i);
        if (ret) return ret;
    }

    return 0;
}

#endif // BRUTAL_ECS_IMPLEMENTATION
