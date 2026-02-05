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
 * REQUIREMENTS:
 *   - C11 atomics (stdatomic.h)
 */

#ifndef BRUTAL_ECS_H
#define BRUTAL_ECS_H

#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifndef __STDC_NO_ATOMICS__
#include <stdatomic.h>
#else
#error "brutal_ecs.h requires C11 atomic support"
#endif

#ifdef NDEBUG
#define ECS_ASSERT(expr) ((void)0)
#else
#ifndef ECS_ASSERT
#include <assert.h>
#define ECS_ASSERT(expr) (assert(expr))
#endif
#endif

#ifndef ECS_MALLOC
#define ECS_MALLOC(sz) malloc(sz)
#define ECS_REALLOC(p, sz) realloc(p, sz)
#define ECS_FREE(p) free(p)
#endif

#ifndef ECS_MAX_COMPONENTS
#define ECS_MAX_COMPONENTS 256
#endif

#ifndef ECS_MAX_SYSTEMS
#define ECS_MAX_SYSTEMS 256
#endif

#ifndef ECS_MT_MAX_TASKS
#define ECS_MT_MAX_TASKS 64
#endif

#ifndef ECS_SCRATCH_BUFFER_SIZE
#define ECS_SCRATCH_BUFFER_SIZE (1024 * 1024)
#endif

#ifndef ECS_CMD_BUFFER_CAPACITY
#define ECS_CMD_BUFFER_CAPACITY 1024
#endif

/* --------------------------------- Bitset --------------------------------- */

#define ECS_BS_WORD_BITS 64
#define ECS_BS_WORDS                                                           \
    ((ECS_MAX_COMPONENTS + (ECS_BS_WORD_BITS - 1)) / ECS_BS_WORD_BITS)

// Count trailing zeros in 64-bit value
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
    ECS_ASSERT(bit < ECS_MAX_COMPONENTS);
    bs->words[bit >> 6] |= (1ull << (bit & 63u));
}

static inline void ecs_bs_clear(ecs_bitset *bs, int bit)
{
    ECS_ASSERT(bit < ECS_MAX_COMPONENTS);
    bs->words[bit >> 6] &= ~(1ull << (bit & 63u));
}

static inline bool ecs_bs_test(ecs_bitset *bs, int bit)
{
    ECS_ASSERT(bit < ECS_MAX_COMPONENTS);
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

/* ------------------------------- Sparse Set ------------------------------- */

typedef int ecs_entity;
typedef uint8_t ecs_comp_t;
typedef int ecs_sys_t;

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
    ECS_FREE(set->sparse);
    ECS_FREE(set->dense);
    memset(set, 0, sizeof(*set));
}

static inline void ecs_ss_reserve_sparse(ecs_sparse_set *set, int need)
{
    if (need <= set->sparse_cap) return;
    int old = set->sparse_cap;
    int cap = old ? old : 1;
    while (cap < need) cap <<= 1;
    set->sparse = (int *)ECS_REALLOC(set->sparse, cap * sizeof(int));
    ECS_ASSERT(set->sparse);
    memset(set->sparse + old, 0, (cap - old) * sizeof(int));
    set->sparse_cap = cap;
}

static inline void ecs_ss_reserve_dense(ecs_sparse_set *set, int need)
{
    if (need <= set->dense_cap) return;
    int cap = set->dense_cap ? set->dense_cap : 1;
    while (cap < need) cap <<= 1;
    set->dense = (ecs_entity *)ECS_REALLOC(set->dense, cap * sizeof(ecs_entity));
    ECS_ASSERT(set->dense);
    set->dense_cap = cap;
}

static inline int ecs_ss_has(ecs_sparse_set *set, ecs_entity entity)
{
    return (entity < set->sparse_cap) && (set->sparse[entity] != 0);
}

static inline int ecs_ss_index_of(ecs_sparse_set *s, ecs_entity entity)
{
    ECS_ASSERT(ecs_ss_has(s, entity));
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

/* ----------------------------- Component Pool ----------------------------- */

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
    ECS_FREE(pool->data);
    ecs_ss_free(&pool->set);
    memset(pool, 0, sizeof(*pool));
}

static inline void ecs_pool_reserve(ecs_pool *pool, int need)
{
    if (need <= pool->set.dense_cap) return;
    ecs_ss_reserve_dense(&pool->set, need);
    pool->data = ECS_REALLOC(
        pool->data,
        (size_t)pool->set.dense_cap * (size_t)pool->element_size
    );
    ECS_ASSERT(pool->data);
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

/* ----------------------------- Command Buffer ----------------------------- */

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
    ecs_cmd *commands;
    int count;
    int capacity;

    uint8_t *data_buffer;
    int data_offset;
    int data_capacity;
} ecs_cmd_buffer;

/* ---------------------------------- Core ---------------------------------- */

typedef struct ecs_s ecs_t;

// View of matching entities passed to system callbacks
typedef struct ecs_view
{
    ecs_entity *entities;
    int count;
    ecs_t *ecs;
} ecs_view;

typedef int (*ecs_system_fn)(ecs_t *ecs, ecs_view *view, void *udata);

typedef int (*ecs_enqueue_task_fn)(int (*fn)(void *args), void *fn_args, void *udata);
typedef void (*ecs_wait_tasks_fn)(void *udata);

typedef struct
{
    ecs_bitset all_of;
    ecs_bitset none_of;
    ecs_bitset read;
    ecs_bitset write;
    ecs_bitset rw; // derived: read | write
    int phase;
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
} ecs_phase;

typedef struct
{
    ecs_t *ecs;
    int sys_index;
    int task_index;
    uint8_t *scratch_buffer;
    int scratch_capacity;
} ecs_task_args;

struct ecs_s
{
    // Entities
    _Atomic(ecs_entity) next_entity;
    _Atomic(int) free_list_head; // Index of first free slot (-1 == empty)
    int *free_list_next;
    int free_list_capacity;

    // Components
    ecs_pool pools[ECS_MAX_COMPONENTS];
    int comp_count;

    // Systems
    ecs_system systems[ECS_MAX_SYSTEMS];
    int system_count;

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

/* ------------------------- Command Buffer Ops ----------------------------- */

static inline void ecs_cmd_buffer_init(ecs_cmd_buffer *cb, int initial_capacity)
{
    memset(cb, 0, sizeof(*cb));
    cb->capacity = initial_capacity;
    cb->commands = (ecs_cmd *)ECS_MALLOC((size_t)initial_capacity * sizeof(ecs_cmd));
    ECS_ASSERT(cb->commands);

    cb->data_capacity = 1024 * 1024;
    cb->data_buffer = (uint8_t *)ECS_MALLOC((size_t)cb->data_capacity);
    ECS_ASSERT(cb->data_buffer);
}

static inline void ecs_cmd_buffer_free(ecs_cmd_buffer *cb)
{
    ECS_FREE(cb->commands);
    ECS_FREE(cb->data_buffer);
    memset(cb, 0, sizeof(*cb));
}

static inline void ecs_cmd_buffer_grow(ecs_cmd_buffer *cb)
{
    int new_cap = cb->capacity * 2;
    cb->commands = (ecs_cmd *)ECS_REALLOC(cb->commands, (size_t)new_cap * sizeof(ecs_cmd));
    ECS_ASSERT(cb->commands);
    cb->capacity = new_cap;
}

static inline void *ecs_cmd_alloc_data(ecs_cmd_buffer *cb, int size)
{
    if (cb->data_offset + size > cb->data_capacity) {
        int new_cap = cb->data_capacity * 2;
        while (new_cap < cb->data_offset + size) new_cap *= 2;
        cb->data_buffer = (uint8_t *)ECS_REALLOC(cb->data_buffer, (size_t)new_cap);
        ECS_ASSERT(cb->data_buffer);
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

/* --------------------------- ECS new/free  --------------------------- */

static inline ecs_t *ecs_new(void)
{
    ecs_t *ecs = (ecs_t *)ECS_MALLOC(sizeof(ecs_t));
    ECS_ASSERT(ecs);

    memset(ecs, 0, sizeof(*ecs));
    atomic_store(&ecs->next_entity, 1);
    atomic_store(&ecs->free_list_head, -1); // Empty
    ecs->task_count = 1;

    // Allocate free list (grows as needed)
    ecs->free_list_capacity = 1024;
    ecs->free_list_next = (int *)ECS_MALLOC((size_t)ecs->free_list_capacity * sizeof(int));
    ECS_ASSERT(ecs->free_list_next);

    // Allocate scratch buffers
    for (int i = 0; i < ECS_MT_MAX_TASKS; i++) {
        ecs->task_scratch[i] = (uint8_t *)ECS_MALLOC(ECS_SCRATCH_BUFFER_SIZE);
        ECS_ASSERT(ecs->task_scratch[i]);
        ecs->task_scratch_capacity[i] = ECS_SCRATCH_BUFFER_SIZE;
    }

    // Initialize command buffer
    for (int i = 0; i < ECS_MT_MAX_TASKS; i++) {
        ecs_cmd_buffer_init(&ecs->cmd_buffers[i], ECS_CMD_BUFFER_CAPACITY);
    }

    return ecs;
}

static inline void ecs_free(ecs_t *ecs)
{
    if (!ecs) return;

    for (int i = 0; i < ecs->comp_count; i++) ecs_pool_free(&ecs->pools[i]);
    ECS_FREE(ecs->free_list_next);

    // Free scratch buffers
    for (int i = 0; i < ECS_MT_MAX_TASKS; i++) {
        ECS_FREE(ecs->task_scratch[i]);
    }

    // Free command buffer
    for (int i = 0; i < ECS_MT_MAX_TASKS; i++) {
        ecs_cmd_buffer_free(&ecs->cmd_buffers[i]);
    }

    ECS_FREE(ecs);
}

static inline void ecs_set_task_callbacks(
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

/* ------------------------- Free List Operations --------------------------- */

static inline ecs_entity ecs_free_list_pop(ecs_t *ecs)
{
    int old_head, new_head;
    do {
        old_head = atomic_load(&ecs->free_list_head);
        if (old_head == -1) return 0; // Empty
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

/* -------------------------- Deferred Operations --------------------------- */

static inline void *ecs_add_deferred(ecs_t *ecs, ecs_entity entity, ecs_comp_t component)
{
    ECS_ASSERT(component < ecs->comp_count);

    // Allocate component data from command buffer's data arena
    int element_size = ecs->pools[component].element_size;
    ecs_cmd_buffer *cb = ecs_current_cmd_buffer(ecs);
    void *data = ecs_cmd_alloc_data(cb, (size_t)element_size);
    memset(data, 0, (size_t)element_size);

    // Queue ADD command
    ecs_cmd cmd = { .type = ECS_CMD_ADD,
                    .entity = entity,
                    .component = component,
                    .component_data = data };
    ecs_cmd_enqueue(ecs, &cmd);

    return data; // User initializes this
}

static inline void ecs_remove_deferred(ecs_t *ecs, ecs_entity entity, ecs_comp_t component)
{
    ECS_ASSERT(component < ecs->comp_count);

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

/* --------------------------------- Entity --------------------------------- */

// Creates an entity. Lock-free via atomic counter and CAS free list.
// Safe to call from any thread at any time.
static inline ecs_entity ecs_create(ecs_t *ecs)
{
    ecs_entity e = ecs_free_list_pop(ecs);
    if (e) return e;
    return atomic_fetch_add(&ecs->next_entity, 1);
}

static inline void ecs_destroy(ecs_t *ecs, ecs_entity e)
{
    if (ecs->in_progress) {
        ecs_destroy_deferred(ecs, e);
        return;
    }

    for (int c = 0; c < ecs->comp_count; c++)
        (void)ecs_pool_remove(&ecs->pools[c], e);

    // Grow free list if needed
    if ((int)e >= ecs->free_list_capacity) {
        int new_cap = ecs->free_list_capacity * 2;
        while (new_cap <= (int)e) new_cap *= 2;
        ecs->free_list_next = (int *)
            ECS_REALLOC(ecs->free_list_next, (size_t)new_cap * sizeof(int));
        ECS_ASSERT(ecs->free_list_next);
        ecs->free_list_capacity = new_cap;
    }

    ecs_free_list_push(ecs, e);
}

/* ------------------------------- Components ------------------------------- */

static inline ecs_comp_t ecs_register_component(ecs_t *ecs, int size)
{
    ECS_ASSERT(ecs->comp_count < ECS_MAX_COMPONENTS);
    ecs_comp_t id = (ecs_comp_t)ecs->comp_count++;
    ecs_pool_init(&ecs->pools[id], size);
    return id;
}

#define ECS_COMPONENT(ecs_ptr, Type)                                           \
    ecs_register_component((ecs_ptr), (int)sizeof(Type))

// Adds a component to an entity. During system execution, the operation is
// queued and returns a pointer to scratch-allocated memory for initialization.
// The data is copied to the permanent pool when the command is applied.
// Thread-safe via per-task command buffers.
static inline void *ecs_add(ecs_t *ecs, ecs_entity entity, ecs_comp_t component)
{
    if (ecs->in_progress) return ecs_add_deferred(ecs, entity, component);

    ECS_ASSERT(component < ecs->comp_count);
    return ecs_pool_add(&ecs->pools[component], entity);
}

// Removes a component from an entity. During system execution, queued and
// applied at phase boundary. Thread-safe via per-task command buffers.
static inline void ecs_remove(ecs_t *ecs, ecs_entity entity, ecs_comp_t component)
{
    if (ecs->in_progress) {
        ecs_remove_deferred(ecs, entity, component);
        return;
    }

    ECS_ASSERT(component < ecs->comp_count);
    (void)ecs_pool_remove(&ecs->pools[component], entity);
}
static inline void *ecs_get(ecs_t *ecs, ecs_entity entity, ecs_comp_t component)
{
    ECS_ASSERT(component < ecs->comp_count);
    return ecs_pool_get(&ecs->pools[component], entity);
}
static inline int ecs_has(ecs_t *ecs, ecs_entity entity, ecs_comp_t component)
{
    ECS_ASSERT(component < ecs->comp_count);
    return ecs_ss_has(&ecs->pools[component].set, entity);
}

/* -------------------------------- Systems  -------------------------------- */

static inline ecs_sys_t ecs_sys_create(ecs_t *ecs, ecs_system_fn fn, void *udata)
{
    ECS_ASSERT(ecs->system_count < ECS_MAX_SYSTEMS);
    ECS_ASSERT(fn);

    ecs_sys_t sys = (ecs_sys_t)ecs->system_count++;
    ecs_system *s = &ecs->systems[sys];

    memset(s, 0, sizeof(*s));
    s->fn = fn;
    s->udata = udata;
    s->enabled = true;

    return sys;
}

static inline void ecs_sys_require(ecs_t *ecs, ecs_sys_t sys, ecs_comp_t comp)
{
    ECS_ASSERT(sys >= 0 && sys < ecs->system_count);
    ecs_system *s = &ecs->systems[sys];
    ecs_bs_set(&s->all_of, comp);
    ecs_bs_set(&s->read, comp);
    ecs_bs_set(&s->rw, comp);
}

static inline void ecs_sys_exclude(ecs_t *ecs, ecs_sys_t sys, ecs_comp_t comp)
{
    ECS_ASSERT(sys >= 0 && sys < ecs->system_count);
    ecs_bs_set(&ecs->systems[sys].none_of, comp);
}

static inline void ecs_sys_read(ecs_t *ecs, ecs_sys_t sys, ecs_comp_t comp)
{
    ECS_ASSERT(sys >= 0 && sys < ecs->system_count);
    ecs_system *s = &ecs->systems[sys];
    ecs_bs_set(&s->read, comp);
    ecs_bs_set(&s->rw, comp);
}

static inline void ecs_sys_write(ecs_t *ecs, ecs_sys_t sys, ecs_comp_t comp)
{
    ECS_ASSERT(sys >= 0 && sys < ecs->system_count);
    ecs_system *s = &ecs->systems[sys];
    ecs_bs_set(&s->write, comp);
    ecs_bs_set(&s->rw, comp);
}

static inline void ecs_sys_enable(ecs_t *ecs, ecs_sys_t sys)
{
    ECS_ASSERT(sys >= 0 && sys < ecs->system_count);
    ecs->systems[sys].enabled = true;
}

static inline void ecs_sys_disable(ecs_t *ecs, ecs_sys_t sys)
{
    ECS_ASSERT(sys >= 0 && sys < ecs->system_count);
    ecs->systems[sys].enabled = false;
}

static inline void ecs_sys_set_phase(ecs_t *ecs, ecs_sys_t sys, int phase)
{
    ECS_ASSERT(sys >= 0 && sys < ecs->system_count);
    ecs->systems[sys].phase = phase;
}

static inline int ecs_sys_get_phase(ecs_t *ecs, ecs_sys_t sys)
{
    ECS_ASSERT(sys >= 0 && sys < ecs->system_count);
    return ecs->systems[sys].phase;
}

static inline void ecs_sys_set_udata(ecs_t *ecs, ecs_sys_t sys, void *udata)
{
    ECS_ASSERT(sys >= 0 && sys < ecs->system_count);
    ecs->systems[sys].udata = udata;
}

static inline void *ecs_sys_get_udata(ecs_t *ecs, ecs_sys_t sys)
{
    ECS_ASSERT(sys >= 0 && sys < ecs->system_count);
    return ecs->systems[sys].udata;
}

/* ------------------------------- Scheduling ------------------------------- */

static inline int ecs_phase_can_accept(ecs_phase *p, ecs_system *s)
{
    // Check for read/write conflicts
    // write conflicts with (read_union | write_union)
    ecs_bitset phase_rw;
    ecs_bs_or(&phase_rw, &p->read_union, &p->write_union);

    if (ecs_bs_intersects(&s->write, &phase_rw)) return 0;
    ecs_bitset s_rw;
    ecs_bs_or(&s_rw, &s->read, &s->write);
    if (ecs_bs_intersects(&p->write_union, &s_rw)) return 0;

    return 1;
}

static inline void ecs_phase_add_system(ecs_phase *p, int sys_index, ecs_system *s)
{
    ECS_ASSERT(p->sys_count < ECS_MAX_SYSTEMS);
    p->sys_idx[p->sys_count++] = sys_index;
    ecs_bs_or_into(&p->read_union, &s->read);
    ecs_bs_or_into(&p->write_union, &s->write);
}

// Greedy, registration-order stable phase building.
// Systems are placed into phases based on read/write conflict detection
static inline int ecs_build_phases(ecs_t *ecs, ecs_phase *out_phases, int max_phases)
{
    int phase_count = 0;
    memset(out_phases, 0, (size_t)max_phases * sizeof(*out_phases));

    for (int i = 0; i < ecs->system_count; i++) {
        ecs_system *s = &ecs->systems[i];

        // Try to place system in earliest compatible phase
        int placed = 0;
        for (int p = 0; p < phase_count; p++) {
            if (ecs_phase_can_accept(&out_phases[p], s)) {
                ecs_phase_add_system(&out_phases[p], i, s);
                placed = 1;
                break;
            }
        }

        if (placed) continue;

        // Create new phase for this system
        ECS_ASSERT(phase_count < max_phases);
        ecs_phase *p = &out_phases[phase_count];
        ECS_ASSERT(ecs_phase_can_accept(p, s));
        ecs_phase_add_system(p, i, s);
        phase_count++;
    }

    return phase_count;
}

static inline void ecs_sync(ecs_t *ecs)
{
    ECS_ASSERT(!ecs->in_progress);

    // Process all commands (order between tasks is not guaranteed)
    for (int t = 0; t < ecs->task_count; t++) {
        ecs_cmd_buffer *cb = &ecs->cmd_buffers[t];
        for (int i = 0; i < cb->count; i++) {
            ecs_cmd *cmd = &cb->commands[i];
            ecs_entity target = cmd->entity;

            switch (cmd->type) {
                case ECS_CMD_DESTROY: ecs_destroy(ecs, target); break;

                case ECS_CMD_ADD: {
                    ECS_ASSERT(cmd->component < ecs->comp_count);
                    void *dst = ecs_pool_add(&ecs->pools[cmd->component], target);
                    int elem_size = ecs->pools[cmd->component].element_size;
                    memcpy(dst, cmd->component_data, (size_t)elem_size);
                    break;
                }

                case ECS_CMD_REMOVE:
                    ECS_ASSERT(cmd->component < ecs->comp_count);
                    ecs_pool_remove(&ecs->pools[cmd->component], target);
                    break;
            }
        }

        cb->count = 0;
        cb->data_offset = 0;
    }
}

// --------------------------------- Matching / execution ---------------------------------

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

    // Pick smallest pool to drive iteration
    ecs_comp_t drive = ecs_pick_driver(ecs, &s->all_of);
    if (drive == (ecs_comp_t)-1) { goto done; }

    ecs_sparse_set *set = &ecs->pools[drive].set;
    int entity_count = set->count;
    if (!entity_count) { goto done; }

    // Calculate this task's slice
    int task_count = ecs->task_count;
    int task_idx = args->task_index;
    int start = (entity_count * task_idx) / task_count;
    int end = (entity_count * (task_idx + 1)) / task_count;
    int slice_count = end - start;

    // Ensure scratch buffer is large enough for matched entity array
    int needed = slice_count * sizeof(ecs_entity);
    if (needed > args->scratch_capacity) {
        int new_capacity = needed * 2;
        args->scratch_buffer = (uint8_t *)ECS_REALLOC(args->scratch_buffer, new_capacity);
        ECS_ASSERT(args->scratch_buffer);
        args->scratch_capacity = new_capacity;

        ecs->task_scratch[args->task_index] = args->scratch_buffer;
        ecs->task_scratch_capacity[args->task_index] = new_capacity;
    }

    // Collect matched entities into scratch buffer
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

    ecs_view view = { .entities = matched, .count = matched_count, .ecs = ecs };
    ret = s->fn(ecs, &view, s->udata);

done:
    ecs_set_tls_task_index(0);
    return ret;
}

static inline int ecs_run_system(ecs_t *ecs, ecs_sys_t sys, float dt)
{
    (void)dt;
    ECS_ASSERT(sys >= 0 && sys < ecs->system_count);

    ecs_system *s = &ecs->systems[sys];
    if (!s->enabled) return 0;

    ecs->in_progress = 1;

    int mt = (ecs->enqueue_cb && ecs->wait_cb && ecs->task_count > 1);
    int ret = 0;

    if (!mt) {
        ecs_task_args a = { .ecs = ecs,
                            .sys_index = sys,
                            .task_index = 0,
                            .scratch_buffer = ecs->task_scratch[0],
                            .scratch_capacity = ecs->task_scratch_capacity[0] };
        ret = ecs_run_system_task(&a);
    } else {
        ecs_task_args *args = ecs->task_args_storage;

        for (int t = 0; t < ecs->task_count; t++) {
            args[t].ecs = ecs;
            args[t].sys_index = sys;
            args[t].task_index = t;
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

static inline int ecs_progress(ecs_t *ecs, int phase_mask)
{
    ecs->in_progress = 1;

    ecs_phase phases[ECS_MAX_SYSTEMS];
    int phase_count = ecs_build_phases(ecs, phases, ECS_MAX_SYSTEMS);

    int mt = (ecs->enqueue_cb && ecs->wait_cb && ecs->task_count > 1);
    int ret = 0;

    for (int p = 0; p < phase_count; p++) {
        ecs_phase *ph = &phases[p];

        for (int i = 0; i < ph->sys_count; i++) {
            int sys_index = ph->sys_idx[i];
            ecs_system *s = &ecs->systems[sys_index];

            // Skip disabled systems
            if (!s->enabled) continue;

            // Filter by phase mask
            int matches = (phase_mask == 0)
                            ? (s->phase == 0) // phase_mask=0: only phase 0
                            : (s->phase & phase_mask); // else: bitwise test

            if (!matches) continue;

            if (!mt) {
                ecs_task_args a = { .ecs = ecs,
                                    .sys_index = sys_index,
                                    .task_index = 0,
                                    .scratch_buffer = ecs->task_scratch[0],
                                    .scratch_capacity = ecs->task_scratch_capacity[0] };
                ret = ecs_run_system_task(&a);
                if (ret) goto done;
                continue;
            }

            ecs_task_args *args = ecs->task_args_storage;

            for (int t = 0; t < ecs->task_count; t++) {
                args[t].ecs = ecs;
                args[t].sys_index = sys_index;
                args[t].task_index = t;
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

        // Apply deferred commands between phases
        ecs->in_progress = 0;
        ecs_sync(ecs);
        ecs->in_progress = 1;
    }

done:
    // Apply any remaining commands after all phases complete
    ecs->in_progress = 0;
    ecs_sync(ecs);
    return ret;
}

#endif // BRUTAL_ECS_H
