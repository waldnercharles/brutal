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
 *
 *   static int move_system(ecs_t *ecs, ecs_view *view, void *udata)
 *   {
 *       ecs_comp_t pos_comp = *(ecs_comp_t *)udata;
 *       for (int i = 0; i < view->count; i++) {
 *           ecs_entity e = view->entities[i];
 *           Position *pos = (Position *)ecs_get(ecs, e, pos_comp);
 *           pos->x += 1.0f;
 *       }
 *       return 0;
 *   }
 *
 *   int main()
 *   {
 *       ecs_t *ecs = ecs_new();
 *       ecs_comp_t pos_comp = ECS_COMPONENT(ecs, Position);
 *       ecs_entity e = ecs_create(ecs);
 *       Position *pos = (Position *)ecs_add(ecs, e, pos_comp);
 *       pos->x = 0.0f;
 *       pos->y = 0.0f;
 *
 *       ecs_sys_t move = ecs_sys_create(ecs, move_system, &pos_comp);
 *       ecs_sys_require(ecs, move, pos_comp);
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
#define ECS_MAX_COMPONENTS 256
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
typedef struct ecs_view
{
    ecs_entity *entities;
    int count;
} ecs_view;

typedef int (*ecs_system_fn)(ecs_t *ecs, ecs_view *view, void *udata);
typedef int (*ecs_enqueue_task_fn)(int (*fn)(void *args), void *fn_args, void *udata);
typedef void (*ecs_wait_tasks_fn)(void *udata);

#define ECS_COMPONENT(ecs_ptr, Type)                                           \
    ecs_register_component((ecs_ptr), (int)sizeof(Type))

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
int ecs_has(ecs_t *ecs, ecs_entity entity, ecs_comp_t component);

// Systems
ecs_sys_t ecs_sys_create(ecs_t *ecs, ecs_system_fn fn, void *udata);
void ecs_sys_require(ecs_t *ecs, ecs_sys_t sys, ecs_comp_t comp);
void ecs_sys_exclude(ecs_t *ecs, ecs_sys_t sys, ecs_comp_t comp);
void ecs_sys_read(ecs_t *ecs, ecs_sys_t sys, ecs_comp_t comp);
void ecs_sys_write(ecs_t *ecs, ecs_sys_t sys, ecs_comp_t comp);
void ecs_sys_after(ecs_t *ecs, ecs_sys_t sys, ecs_sys_t dependency);
void ecs_sys_enable(ecs_t *ecs, ecs_sys_t sys);
void ecs_sys_disable(ecs_t *ecs, ecs_sys_t sys);
void ecs_sys_set_group(ecs_t *ecs, ecs_sys_t sys, int group);
int ecs_sys_get_group(ecs_t *ecs, ecs_sys_t sys);
void ecs_sys_set_udata(ecs_t *ecs, ecs_sys_t sys, void *udata);
void *ecs_sys_get_udata(ecs_t *ecs, ecs_sys_t sys);

// Execution
int ecs_run_system(ecs_t *ecs, ecs_sys_t sys, float dt);
int ecs_progress(ecs_t *ecs, int group_mask);

// Debug
void ecs_dump_schedule(ecs_t *ecs);

#endif // BRUTAL_ECS_H

// -----------------------------------------------------------------------------
//  Implementation

#ifdef BRUTAL_ECS_IMPLEMENTATION

#include <assert.h>
#include <limits.h>
#include <stdalign.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef ECS_SCRATCH_BUFFER_SIZE
#define ECS_SCRATCH_BUFFER_SIZE (1024 * 1024)
#endif

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
#elif defined(__GNUC__) || defined(__clang__)
static inline int ecs_ctz64(uint64_t x)
{
    return __builtin_ctzll(x);
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
// clang-format on
#endif

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
    set->sparse = (int *)realloc(set->sparse, cap * sizeof(int));
    assert(set->sparse);
    memset(set->sparse + old, 0, (cap - old) * sizeof(int));
    set->sparse_cap = cap;
}

static inline void ecs_ss_reserve_dense(ecs_sparse_set *set, int need)
{
    if (need <= set->dense_cap) return;
    int cap = set->dense_cap ? set->dense_cap : 1;
    while (cap < need) cap <<= 1;
    set->dense = (ecs_entity *)realloc(set->dense, cap * sizeof(ecs_entity));
    assert(set->dense);
    set->dense_cap = cap;
}

static inline int ecs_ss_has(ecs_sparse_set *set, ecs_entity entity)
{
    return (entity < set->sparse_cap) && (set->sparse[entity] != 0);
}

static inline int ecs_ss_index_of(ecs_sparse_set *s, ecs_entity entity)
{
    assert(ecs_ss_has(s, entity));
    return s->sparse[entity] - 1;
}

static inline int ecs_ss_insert(ecs_sparse_set *set, ecs_entity entity)
{
    ecs_ss_reserve_sparse(set, (int)entity + 1);
    if (set->sparse[entity]) return 0;
    ecs_ss_reserve_dense(set, set->count + 1);
    int idx = set->count++;
    set->dense[idx] = entity;
    set->sparse[entity] = idx + 1;
    return 1;
}

static inline int ecs_ss_remove(ecs_sparse_set *set, ecs_entity entity)
{
    if (entity >= (ecs_entity)set->sparse_cap) return 0;
    int idx = set->sparse[entity];
    if (!idx) return 0;

    idx--;
    int last = set->count - 1;
    ecs_entity last_id = set->dense[last];

    set->dense[idx] = last_id;
    set->count--;

    set->sparse[entity] = 0;
    if (idx != last) set->sparse[last_id] = idx + 1;
    return 1;
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

static inline int ecs_pool_remove(ecs_pool *pool, ecs_entity e)
{
    if (!ecs_ss_has(&pool->set, e)) return 0;
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
    if (!ecs_ss_has(&pool->set, e)) return NULL;
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
    ecs_bitset read;
    ecs_bitset write;
    ecs_bitset rw;
    ecs_bitset after;
    int group;
    ecs_system_fn fn;
    void *udata;
    bool enabled;
} ecs_system;

typedef struct
{
    ecs_bitset read_union;
    ecs_bitset write_union;

    int sys_idx[ECS_MAX_SYSTEMS];
    int sys_count;
} ecs_stage;

typedef struct
{
    ecs_t *ecs;
    int sys_index;
    int task_index;
    int slice_index;
    int slice_count;
    uint8_t *scratch_buffer;
    int scratch_capacity;
} ecs_task_args;

struct ecs_s
{
    // Entities
    _Atomic(ecs_entity) next_entity;
    _Atomic(int) free_list_head;
    int *free_list_next;
    int free_list_capacity;

    // Components
    ecs_pool pools[ECS_MAX_COMPONENTS];
    int comp_count;

    // Systems
    ecs_system systems[ECS_MAX_SYSTEMS];
    int system_count;

    // Schedule cache
    ecs_stage cached_stages[ECS_MAX_SYSTEMS];
    int cached_stage_count;
    bool schedule_dirty;

    // Multithreading
    ecs_enqueue_task_fn enqueue_cb;
    ecs_wait_tasks_fn wait_cb;
    void *task_udata;
    int task_count;

    int in_progress;

    ecs_task_args task_args_storage[ECS_MT_MAX_TASKS];
    uint8_t *task_scratch[ECS_MT_MAX_TASKS];
    int task_scratch_capacity[ECS_MT_MAX_TASKS];

    ecs_cmd_buffer cmd_buffers[ECS_MT_MAX_TASKS];
};

// -----------------------------------------------------------------------------
//  Command Buffer Implementation

static inline void ecs_cmd_buffer_init(ecs_cmd_buffer *cb, int initial_capacity)
{
    memset(cb, 0, sizeof(*cb));
    cb->capacity = initial_capacity;
    cb->commands = (ecs_cmd *)malloc((size_t)initial_capacity * sizeof(ecs_cmd));
    assert(cb->commands);

    cb->data_capacity = 1024 * 1024;
    cb->data_buffer = (uint8_t *)malloc((size_t)cb->data_capacity);
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
    cb->commands = (ecs_cmd *)realloc(cb->commands, (size_t)new_cap * sizeof(ecs_cmd));
    assert(cb->commands);
    cb->capacity = new_cap;
}

static inline void *ecs_cmd_alloc_data(ecs_cmd_buffer *cb, int size)
{
    if (cb->data_offset + size > cb->data_capacity) {
        int new_cap = cb->data_capacity * 2;
        while (new_cap < cb->data_offset + size) new_cap *= 2;
        cb->data_buffer = (uint8_t *)realloc(cb->data_buffer, (size_t)new_cap);
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
    if (idx < 0 || idx >= ecs->task_count) idx = 0;
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
    return (ecs_entity)old_head;
}

static inline void ecs_free_list_push(ecs_t *ecs, ecs_entity entity)
{
    int old_head;
    do {
        old_head = atomic_load(&ecs->free_list_head);
        ecs->free_list_next[entity] = old_head;
    } while (!atomic_compare_exchange_weak(&ecs->free_list_head, &old_head, (int)entity));
}

// -----------------------------------------------------------------------------
//  Deferred Operations

static inline void *ecs_add_deferred(ecs_t *ecs, ecs_entity entity, ecs_comp_t component)
{
    assert(component < ecs->comp_count);

    int element_size = ecs->pools[component].element_size;
    ecs_cmd_buffer *cb = ecs_current_cmd_buffer(ecs);
    void *data = ecs_cmd_alloc_data(cb, (size_t)element_size);
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

    for (int t = 0; t < ecs->task_count; t++) {
        ecs_cmd_buffer *cb = &ecs->cmd_buffers[t];
        for (int i = 0; i < cb->count; i++) {
            ecs_cmd *cmd = &cb->commands[i];
            ecs_entity target = cmd->entity;

            switch (cmd->type) {
                case ECS_CMD_DESTROY: ecs_destroy(ecs, target); break;

                case ECS_CMD_ADD: {
                    assert(cmd->component < ecs->comp_count);
                    void *dst = ecs_pool_add(&ecs->pools[cmd->component], target);
                    int elem_size = ecs->pools[cmd->component].element_size;
                    memcpy(dst, cmd->component_data, (size_t)elem_size);
                    break;
                }

                case ECS_CMD_REMOVE:
                    assert(cmd->component < ecs->comp_count);
                    ecs_pool_remove(&ecs->pools[cmd->component], target);
                    break;
            }
        }

        cb->count = 0;
        cb->data_offset = 0;
    }
}

// -----------------------------------------------------------------------------
//  Scheduling

static inline bool ecs_systems_conflict(ecs_system *a, ecs_system *b)
{
    if (ecs_bs_intersects(&a->write, &b->rw)) return true;
    if (ecs_bs_intersects(&b->write, &a->rw)) return true;
    return false;
}

static inline int ecs_build_stages(ecs_t *ecs, ecs_stage *out_stages, int max_stages)
{
    memset(out_stages, 0, (size_t)max_stages * sizeof(*out_stages));

    int n = ecs->system_count;
    if (n == 0) return 0;

    // Build predecessor bitsets for each system
    ecs_bitset predecessors[ECS_MAX_SYSTEMS];
    for (int i = 0; i < n; i++) { ecs_bs_zero(&predecessors[i]); }

    // Add edges from conflicts (directed by registration order)
    for (int i = 0; i < n; i++) {
        for (int j = i + 1; j < n; j++) {
            if (ecs_systems_conflict(&ecs->systems[i], &ecs->systems[j])) {
                // j depends on i (i -> j)
                ecs_bs_set(&predecessors[j], i);
            }
        }
    }

    // Add explicit 'after' edges
    for (int i = 0; i < n; i++) {
        ecs_system *s = &ecs->systems[i];
        for (int w = 0; w < ECS_BS_WORDS; w++) {
            uint64_t word = s->after.words[w];
            while (word) {
                int bit = ecs_ctz64(word) + (w * 64);
                if (bit < n) {
                    // i depends on bit (bit -> i)
                    ecs_bs_set(&predecessors[i], bit);
                }
                word &= (word - 1ull);
            }
        }
    }

    // Compute stage assignments via BFS (Kahn's algorithm)
    int stage_assignment[ECS_MAX_SYSTEMS];
    bool visited[ECS_MAX_SYSTEMS] = { 0 };
    int max_stage = -1;

    for (int i = 0; i < n; i++) {
        if (visited[i]) continue;

        // Compute stage for system i
        int stage = 0;
        for (int w = 0; w < ECS_BS_WORDS; w++) {
            uint64_t word = predecessors[i].words[w];
            while (word) {
                int pred = ecs_ctz64(word) + (w * 64);
                if (pred < n && visited[pred]) {
                    int pred_stage = stage_assignment[pred];
                    if (pred_stage + 1 > stage) { stage = pred_stage + 1; }
                }
                word &= (word - 1ull);
            }
        }

        stage_assignment[i] = stage;
        visited[i] = true;
        if (stage > max_stage) max_stage = stage;
    }

    int stage_count = max_stage + 1;
    assert(stage_count <= max_stages);

    // Populate output stages
    for (int i = 0; i < n; i++) {
        int stage = stage_assignment[i];
        ecs_stage *s = &out_stages[stage];
        assert(s->sys_count < ECS_MAX_SYSTEMS);
        s->sys_idx[s->sys_count++] = i;
    }

    return stage_count;
}

// -----------------------------------------------------------------------------
//  Matching

static inline int ecs_entity_has_all_of(ecs_t *ecs, ecs_entity e, ecs_bitset *all_of)
{
    for (uint32_t w = 0; w < ECS_BS_WORDS; w++) {
        uint64_t word = all_of->words[w];
        while (word) {
            int bit = ecs_ctz64(word) + (w * 64);
            if (bit >= ecs->comp_count) return 0;
            if (!ecs_ss_has(&ecs->pools[bit].set, e)) return 0;
            word &= (word - 1ull);
        }
    }
    return 1;
}

static inline int ecs_entity_has_any_of(ecs_t *ecs, ecs_entity e, ecs_bitset *any_of)
{
    for (uint32_t w = 0; w < ECS_BS_WORDS; w++) {
        uint64_t word = any_of->words[w];
        while (word) {
            int bit = ecs_ctz64(word) + (w * 64);
            if (bit < ecs->comp_count && ecs_ss_has(&ecs->pools[bit].set, e))
                return 1;
            word &= (word - 1ull);
        }
    }
    return 0;
}

static inline ecs_comp_t ecs_pick_driver(ecs_t *ecs, ecs_bitset *all_of)
{
    ecs_comp_t best = (ecs_comp_t)-1;
    int best_n = INT_MAX;

    for (uint32_t w = 0; w < ECS_BS_WORDS; w++) {
        uint64_t word = all_of->words[w];
        while (word) {
            int bit = ecs_ctz64(word) + (w * 64);
            if (bit < ecs->comp_count) {
                int n = ecs->pools[bit].set.count;
                if (n < best_n) {
                    best = (ecs_comp_t)bit;
                    best_n = n;
                }
            }
            word &= (word - 1ull);
        }
    }
    return best;
}

static inline int ecs_run_system_task(void *args_v)
{
    ecs_task_args *args = (ecs_task_args *)args_v;
    ecs_t *ecs = args->ecs;
    ecs_system *s = &ecs->systems[args->sys_index];

    int ret = 0;
    ecs_set_tls_task_index(args->task_index);

    ecs_comp_t drive = ecs_pick_driver(ecs, &s->all_of);
    if (drive == (ecs_comp_t)-1) { goto done; }

    ecs_sparse_set *set = &ecs->pools[drive].set;
    int entity_count = set->count;
    if (!entity_count) { goto done; }

    int start = (entity_count * args->slice_index) / args->slice_count;
    int end = (entity_count * (args->slice_index + 1)) / args->slice_count;
    int slice_count = end - start;

    int needed = slice_count * sizeof(ecs_entity);
    if (needed > args->scratch_capacity) {
        int new_capacity = needed * 2;
        args->scratch_buffer = (uint8_t *)realloc(args->scratch_buffer, new_capacity);
        assert(args->scratch_buffer);
        args->scratch_capacity = new_capacity;

        ecs->task_scratch[args->task_index] = args->scratch_buffer;
        ecs->task_scratch_capacity[args->task_index] = new_capacity;
    }

    ecs_entity *matched = (ecs_entity *)args->scratch_buffer;
    int matched_count = 0;

    ecs_entity *dense = set->dense;
    bool any_excluded = ecs_bs_any(&s->none_of);
    for (int i = start; i < end; i++) {
        ecs_entity e = dense[i];
        if (!ecs_entity_has_all_of(ecs, e, &s->all_of)) continue;
        if (any_excluded && ecs_entity_has_any_of(ecs, e, &s->none_of))
            continue;
        matched[matched_count++] = e;
    }

    if (matched_count == 0) { goto done; }

    ecs_view view = { .entities = matched, .count = matched_count };
    ret = s->fn(ecs, &view, s->udata);

done:
    ecs_set_tls_task_index(0);
    return ret;
}

// -----------------------------------------------------------------------------
//  Public API Implementation

ecs_t *ecs_new()
{
    ecs_t *ecs = (ecs_t *)malloc(sizeof(ecs_t));
    assert(ecs);

    memset(ecs, 0, sizeof(*ecs));
    atomic_store(&ecs->next_entity, 1);
    atomic_store(&ecs->free_list_head, -1);
    ecs->task_count = 1;
    ecs->schedule_dirty = true;

    ecs->free_list_capacity = 1024;
    ecs->free_list_next = (int *)malloc((size_t)ecs->free_list_capacity * sizeof(int));
    assert(ecs->free_list_next);

    for (int i = 0; i < ECS_MT_MAX_TASKS; i++) {
        ecs->task_scratch[i] = (uint8_t *)malloc(ECS_SCRATCH_BUFFER_SIZE);
        assert(ecs->task_scratch[i]);
        ecs->task_scratch_capacity[i] = ECS_SCRATCH_BUFFER_SIZE;
    }

    for (int i = 0; i < ECS_MT_MAX_TASKS; i++) {
        ecs_cmd_buffer_init(&ecs->cmd_buffers[i], ECS_CMD_BUFFER_CAPACITY);
    }

    return ecs;
}

void ecs_free(ecs_t *ecs)
{
    if (!ecs) return;

    for (int i = 0; i < ecs->comp_count; i++) ecs_pool_free(&ecs->pools[i]);
    free(ecs->free_list_next);

    for (int i = 0; i < ECS_MT_MAX_TASKS; i++) { free(ecs->task_scratch[i]); }

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
    if (e) return e;
    return atomic_fetch_add(&ecs->next_entity, 1);
}

void ecs_destroy(ecs_t *ecs, ecs_entity e)
{
    if (ecs->in_progress) {
        ecs_destroy_deferred(ecs, e);
        return;
    }

    for (int c = 0; c < ecs->comp_count; c++)
        (void)ecs_pool_remove(&ecs->pools[c], e);

    if ((int)e >= ecs->free_list_capacity) {
        int new_cap = ecs->free_list_capacity * 2;
        while (new_cap <= (int)e) new_cap *= 2;
        ecs->free_list_next = (int *)
            realloc(ecs->free_list_next, (size_t)new_cap * sizeof(int));
        assert(ecs->free_list_next);
        ecs->free_list_capacity = new_cap;
    }

    ecs_free_list_push(ecs, e);
}

ecs_comp_t ecs_register_component(ecs_t *ecs, int size)
{
    assert(ecs->comp_count < ECS_MAX_COMPONENTS);
    ecs_comp_t id = (ecs_comp_t)ecs->comp_count++;
    ecs_pool_init(&ecs->pools[id], size);
    return id;
}

void *ecs_add(ecs_t *ecs, ecs_entity entity, ecs_comp_t component)
{
    if (ecs->in_progress) return ecs_add_deferred(ecs, entity, component);

    assert(component < ecs->comp_count);
    return ecs_pool_add(&ecs->pools[component], entity);
}

void ecs_remove(ecs_t *ecs, ecs_entity entity, ecs_comp_t component)
{
    if (ecs->in_progress) {
        ecs_remove_deferred(ecs, entity, component);
        return;
    }

    assert(component < ecs->comp_count);
    (void)ecs_pool_remove(&ecs->pools[component], entity);
}

void *ecs_get(ecs_t *ecs, ecs_entity entity, ecs_comp_t component)
{
    assert(component < ecs->comp_count);
    return ecs_pool_get(&ecs->pools[component], entity);
}

int ecs_has(ecs_t *ecs, ecs_entity entity, ecs_comp_t component)
{
    assert(component < ecs->comp_count);
    return ecs_ss_has(&ecs->pools[component].set, entity);
}

ecs_sys_t ecs_sys_create(ecs_t *ecs, ecs_system_fn fn, void *udata)
{
    assert(ecs->system_count < ECS_MAX_SYSTEMS);
    assert(fn);

    ecs_sys_t sys = (ecs_sys_t)ecs->system_count++;
    ecs_system *s = &ecs->systems[sys];

    memset(s, 0, sizeof(*s));
    s->fn = fn;
    s->udata = udata;
    s->enabled = true;
    ecs->schedule_dirty = true;

    return sys;
}

void ecs_sys_require(ecs_t *ecs, ecs_sys_t sys, ecs_comp_t comp)
{
    assert(sys >= 0 && sys < ecs->system_count);
    ecs_system *s = &ecs->systems[sys];
    ecs_bs_set(&s->all_of, comp);
    ecs_bs_set(&s->read, comp);
    ecs_bs_set(&s->rw, comp);
    ecs->schedule_dirty = true;
}

void ecs_sys_exclude(ecs_t *ecs, ecs_sys_t sys, ecs_comp_t comp)
{
    assert(sys >= 0 && sys < ecs->system_count);
    ecs_bs_set(&ecs->systems[sys].none_of, comp);
    ecs->schedule_dirty = true;
}

void ecs_sys_read(ecs_t *ecs, ecs_sys_t sys, ecs_comp_t comp)
{
    assert(sys >= 0 && sys < ecs->system_count);
    ecs_system *s = &ecs->systems[sys];
    ecs_bs_set(&s->read, comp);
    ecs_bs_set(&s->rw, comp);
    ecs->schedule_dirty = true;
}

void ecs_sys_write(ecs_t *ecs, ecs_sys_t sys, ecs_comp_t comp)
{
    assert(sys >= 0 && sys < ecs->system_count);
    ecs_system *s = &ecs->systems[sys];
    ecs_bs_set(&s->write, comp);
    ecs_bs_set(&s->rw, comp);
    ecs->schedule_dirty = true;
}

void ecs_sys_after(ecs_t *ecs, ecs_sys_t sys, ecs_sys_t dependency)
{
    assert(sys >= 0 && sys < ecs->system_count);
    assert(dependency >= 0 && dependency < ecs->system_count);
    ecs_bs_set(&ecs->systems[sys].after, dependency);
    ecs->schedule_dirty = true;
}

void ecs_sys_enable(ecs_t *ecs, ecs_sys_t sys)
{
    assert(sys >= 0 && sys < ecs->system_count);
    ecs->systems[sys].enabled = true;
    ecs->schedule_dirty = true;
}

void ecs_sys_disable(ecs_t *ecs, ecs_sys_t sys)
{
    assert(sys >= 0 && sys < ecs->system_count);
    ecs->systems[sys].enabled = false;
    ecs->schedule_dirty = true;
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

int ecs_run_system(ecs_t *ecs, ecs_sys_t sys, float dt)
{
    (void)dt;
    assert(sys >= 0 && sys < ecs->system_count);

    ecs_system *s = &ecs->systems[sys];
    if (!s->enabled) return 0;

    ecs->in_progress = 1;

    int mt = (ecs->enqueue_cb && ecs->wait_cb && ecs->task_count > 1);
    int ret = 0;

    if (!mt) {
        ecs_task_args a = { .ecs = ecs,
                            .sys_index = sys,
                            .task_index = 0,
                            .slice_index = 0,
                            .slice_count = 1,
                            .scratch_buffer = ecs->task_scratch[0],
                            .scratch_capacity = ecs->task_scratch_capacity[0] };
        ret = ecs_run_system_task(&a);
    } else {
        ecs_task_args *args = ecs->task_args_storage;

        for (int t = 0; t < ecs->task_count; t++) {
            args[t].ecs = ecs;
            args[t].sys_index = sys;
            args[t].task_index = t;
            args[t].slice_index = t;
            args[t].slice_count = ecs->task_count;
            args[t].scratch_buffer = ecs->task_scratch[t];
            args[t].scratch_capacity = ecs->task_scratch_capacity[t];

            int enqueue_ret = ecs->enqueue_cb(ecs_run_system_task, &args[t], ecs->task_udata);
            if (enqueue_ret) {
                ret = enqueue_ret;
                goto done;
            }
        }

        ecs->wait_cb(ecs->task_udata);
    }

done:
    ecs->in_progress = 0;
    ecs_sync(ecs);
    return ret;
}

int ecs_progress(ecs_t *ecs, int group_mask)
{
    // Rebuild schedule if dirty
    if (ecs->schedule_dirty) {
        ecs->cached_stage_count = ecs_build_stages(ecs, ecs->cached_stages, ECS_MAX_SYSTEMS);
        ecs->schedule_dirty = false;
    }

    ecs->in_progress = 1;

    int mt = (ecs->enqueue_cb && ecs->wait_cb && ecs->task_count > 1);
    int ret = 0;

    for (int p = 0; p < ecs->cached_stage_count; p++) {
        ecs_stage *stage = &ecs->cached_stages[p];

        // Collect active systems for this stage
        int active_systems[ECS_MAX_SYSTEMS];
        int active_count = 0;

        for (int i = 0; i < stage->sys_count; i++) {
            int sys_index = stage->sys_idx[i];
            ecs_system *s = &ecs->systems[sys_index];

            if (!s->enabled) continue;

            int matches = (group_mask == 0) ? (s->group == 0)
                                            : (s->group & group_mask);

            if (!matches) continue;

            active_systems[active_count++] = sys_index;
        }

        // Skip empty stages
        if (active_count == 0) continue;

        // ST path: run each active system sequentially with slice_count = 1
        if (!mt) {
            for (int i = 0; i < active_count; i++) {
                int sys_index = active_systems[i];
                ecs_task_args a = { .ecs = ecs,
                                    .sys_index = sys_index,
                                    .task_index = 0,
                                    .slice_index = 0,
                                    .slice_count = 1,
                                    .scratch_buffer = ecs->task_scratch[0],
                                    .scratch_capacity = ecs->task_scratch_capacity[0] };
                ret = ecs_run_system_task(&a);
                if (ret) goto done;
            }
        } else {
            // MT path: enqueue active_count × task_count tasks per stage
            // Every system gets full entity parallelism AND systems run concurrently
            int tc = ecs->task_count;
            ecs_task_args *args = ecs->task_args_storage;
            int task_slot = 0;

            for (int i = 0; i < active_count; i++) {
                int sys_index = active_systems[i];

                for (int t = 0; t < tc; t++) {
                    assert(task_slot < ECS_MT_MAX_TASKS);
                    args[task_slot].ecs = ecs;
                    args[task_slot].sys_index = sys_index;
                    args[task_slot].task_index = task_slot;
                    args[task_slot].slice_index = t;
                    args[task_slot].slice_count = tc;
                    args[task_slot].scratch_buffer = ecs->task_scratch[task_slot];
                    args[task_slot].scratch_capacity = ecs->task_scratch_capacity[task_slot];

                    int enqueue_ret = ecs->enqueue_cb(
                        ecs_run_system_task, &args[task_slot], ecs->task_udata);
                    if (enqueue_ret) {
                        ret = enqueue_ret;
                        goto done;
                    }
                    task_slot++;
                }
            }

            ecs->wait_cb(ecs->task_udata);
        }

        // Sync at end of each stage
        ecs->in_progress = 0;
        ecs_sync(ecs);
        ecs->in_progress = 1;
    }

done:
    ecs->in_progress = 0;
    ecs_sync(ecs);
    return ret;
}

void ecs_dump_schedule(ecs_t *ecs)
{
    // Rebuild schedule if dirty
    if (ecs->schedule_dirty) {
        ecs->cached_stage_count = ecs_build_stages(ecs, ecs->cached_stages, ECS_MAX_SYSTEMS);
        ecs->schedule_dirty = false;
    }

    fprintf(stderr, "=== ECS Schedule (%d stages) ===\n", ecs->cached_stage_count);

    for (int stage = 0; stage < ecs->cached_stage_count; stage++) {
        ecs_stage *s = &ecs->cached_stages[stage];
        fprintf(stderr, "Stage %d (%d systems):\n", stage, s->sys_count);

        for (int i = 0; i < s->sys_count; i++) {
            int sys_idx = s->sys_idx[i];
            ecs_system *sys = &ecs->systems[sys_idx];

            fprintf(stderr, "  System %d: enabled=%d group=%d\n", sys_idx, sys->enabled, sys->group);

            // Print read components
            fprintf(stderr, "    read: ");
            bool any_read = false;
            for (int c = 0; c < ecs->comp_count; c++) {
                if (ecs_bs_test(&sys->read, c)) {
                    fprintf(stderr, "%d ", c);
                    any_read = true;
                }
            }
            if (!any_read) fprintf(stderr, "(none)");
            fprintf(stderr, "\n");

            // Print write components
            fprintf(stderr, "    write: ");
            bool any_write = false;
            for (int c = 0; c < ecs->comp_count; c++) {
                if (ecs_bs_test(&sys->write, c)) {
                    fprintf(stderr, "%d ", c);
                    any_write = true;
                }
            }
            if (!any_write) fprintf(stderr, "(none)");
            fprintf(stderr, "\n");

            // Print after dependencies
            fprintf(stderr, "    after: ");
            bool any_after = false;
            for (int d = 0; d < ecs->system_count; d++) {
                if (ecs_bs_test(&sys->after, d)) {
                    fprintf(stderr, "%d ", d);
                    any_after = true;
                }
            }
            if (!any_after) fprintf(stderr, "(none)");
            fprintf(stderr, "\n");
        }
    }

    fprintf(stderr, "=== End Schedule ===\n");
}

#endif // BRUTAL_ECS_IMPLEMENTATION
