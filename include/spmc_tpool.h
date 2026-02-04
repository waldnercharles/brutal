#pragma once

#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#ifndef CACHE_SIZE
#define CACHE_SIZE 64
#endif

#ifndef cache_align
#define cache_align _Alignas(CACHE_SIZE)
#endif

#ifndef atomic
#define atomic _Atomic
#endif

#ifndef TPOOL_HANDLE_SLAB_SIZE
#define TPOOL_HANDLE_SLAB_SIZE 256
#endif

// -------------------- public types --------------------

typedef struct tpool tpool_t;

typedef int (*tpool_task_fn)(void *arg);

typedef struct tpool_task_handle
{
    // Workers decrement this; waiters poll under the pool mutex.
    cache_align atomic int remaining;

    // Private freelist metadata.
    tpool_t *owner;
    struct tpool_task_handle *next_free;
} tpool_task_handle;

typedef struct
{
    tpool_task_fn fn;
    void *arg;
    tpool_task_handle *h;
} tpool_task;

struct tpool
{
    pthread_t *threads;
    size_t nthreads;

    // Main-thread-only build buffer.
    tpool_task *tasks;
    size_t cap;
    size_t build_count;

    // Hot shared state.
    cache_align atomic size_t task_count;
    cache_align atomic size_t next_index;
    cache_align atomic size_t pending;
    cache_align atomic bool stop;

    // Sleep/wake.
    pthread_mutex_t mtx;
    pthread_cond_t cv_work;
    pthread_cond_t cv_done;

    // Main-thread-only handle slab allocator.
    tpool_task_handle *handle_free;
    tpool_task_handle **handle_slabs;
    size_t handle_slab_cap;
    size_t handle_slab_count;
    size_t handle_outstanding;
};

// -------------------- tiny helpers --------------------

static inline void tpool_cpu_relax(int spin_hint)
{
    (void)spin_hint;
#if defined(__x86_64__) || defined(__i386__)
    __asm__ __volatile__("pause" ::: "memory");
#elif defined(__aarch64__) || defined(__arm__)
    __asm__ __volatile__("yield" ::: "memory");
#else
    (void)0;
#endif
}

static inline int tpool_is_running_(tpool_t *tp)
{
    return atomic_load_explicit(&tp->task_count, memory_order_acquire) != 0;
}

static inline void tpool_assert_idle_(tpool_t *tp)
{
#ifdef NDEBUG
    (void)tp;
#else
    assert(!tpool_is_running_(tp));
    assert(atomic_load_explicit(&tp->pending, memory_order_acquire) == 0);
#endif
}

static inline void tpool_handle_init_storage_(tpool_task_handle *h, tpool_t *owner)
{
    atomic_store_explicit(&h->remaining, 0, memory_order_relaxed);
    h->owner = owner;
    h->next_free = NULL;
}

static inline void tpool_wake_waiters_(tpool_t *tp)
{
    pthread_mutex_lock(&tp->mtx);
    pthread_cond_broadcast(&tp->cv_done);
    pthread_mutex_unlock(&tp->mtx);
}

static inline void tpool_handle_done_(tpool_task_handle *h)
{
    int left = atomic_fetch_sub_explicit(&h->remaining, 1, memory_order_acq_rel) - 1;
    if (left == 0) tpool_wake_waiters_(h->owner);
}

static inline void tpool_handle_wait(tpool_task_handle *h)
{
    assert(h);
    tpool_t *tp = h->owner;
    pthread_mutex_lock(&tp->mtx);
    while (atomic_load_explicit(&h->remaining, memory_order_acquire) != 0) {
        pthread_cond_wait(&tp->cv_done, &tp->mtx);
    }
    pthread_mutex_unlock(&tp->mtx);
}

static int tpool_reserve_(tpool_t *tp, size_t need)
{
    if (need <= tp->cap) return 1;

    size_t ncap = tp->cap ? tp->cap : 1024;
    while (ncap < need) ncap *= 2;

    tpool_task *nt = (tpool_task *)realloc(tp->tasks, ncap * sizeof(*nt));
    if (!nt) return 0;

    tp->tasks = nt;
    tp->cap = ncap;
    return 1;
}

static int tpool_handle_slabs_reserve_(tpool_t *tp, size_t need)
{
    if (need <= tp->handle_slab_cap) return 1;

    size_t ncap = tp->handle_slab_cap ? tp->handle_slab_cap : 4;
    while (ncap < need) ncap *= 2;

    tpool_task_handle **np = (tpool_task_handle **)realloc(
        tp->handle_slabs,
        ncap * sizeof(*np)
    );
    if (!np) return 0;

    tp->handle_slabs = np;
    tp->handle_slab_cap = ncap;
    return 1;
}

static int tpool_handle_add_slab_(tpool_t *tp)
{
    if (!tpool_handle_slabs_reserve_(tp, tp->handle_slab_count + 1)) return 0;

    tpool_task_handle *slab =
        (tpool_task_handle *)calloc(TPOOL_HANDLE_SLAB_SIZE, sizeof(*slab));
    if (!slab) return 0;

    tp->handle_slabs[tp->handle_slab_count++] = slab;

    for (size_t i = 0; i < TPOOL_HANDLE_SLAB_SIZE; i++) {
        tpool_handle_init_storage_(&slab[i], tp);
        slab[i].next_free = tp->handle_free;
        tp->handle_free = &slab[i];
    }

    return 1;
}

static tpool_task_handle *tpool_handle_acquire_(tpool_t *tp, int count)
{
    assert(tp);
    assert(count >= 0);
    tpool_assert_idle_(tp);

    if (!tp->handle_free) {
        if (!tpool_handle_add_slab_(tp)) {
            return NULL;
        }
    }

    tpool_task_handle *h = tp->handle_free;
    tp->handle_free = h->next_free;
    h->next_free = NULL;
    tp->handle_outstanding++;

    atomic_store_explicit(&h->remaining, count, memory_order_relaxed);
    return h;
}

static inline void tpool_handle_release_(tpool_task_handle *h)
{
    tpool_t *tp = h->owner;

    h->next_free = tp->handle_free;
    tp->handle_free = h;
    assert(tp->handle_outstanding > 0);
    tp->handle_outstanding--;
}

static int tpool_enqueue_(tpool_t *tp, tpool_task_fn fn, void *arg, tpool_task_handle *h)
{
    assert(tp);
    assert(fn);

    tpool_assert_idle_(tp);

    if (!tpool_reserve_(tp, tp->build_count + 1)) return 0;

    tp->tasks[tp->build_count++] = (tpool_task){ .fn = fn, .arg = arg, .h = h };
    return 1;
}

// -------------------- worker --------------------

static void *tpool_worker_(void *p)
{
    tpool_t *tp = (tpool_t *)p;

    for (;;) {
        while (atomic_load_explicit(&tp->task_count, memory_order_acquire) == 0) {
            if (atomic_load_explicit(&tp->stop, memory_order_relaxed)) return NULL;

            pthread_mutex_lock(&tp->mtx);
            while (atomic_load_explicit(&tp->task_count, memory_order_acquire) == 0 &&
                   !atomic_load_explicit(&tp->stop, memory_order_relaxed)) {
                pthread_cond_wait(&tp->cv_work, &tp->mtx);
            }
            pthread_mutex_unlock(&tp->mtx);

            if (atomic_load_explicit(&tp->stop, memory_order_relaxed)) return NULL;
        }

        for (;;) {
            size_t i = atomic_fetch_add_explicit(&tp->next_index, 1, memory_order_relaxed);
            size_t n = atomic_load_explicit(&tp->task_count, memory_order_acquire);
            if (i >= n) break;

            tpool_task t = tp->tasks[i];
            (void)t.fn(t.arg);

            if (t.h) tpool_handle_done_(t.h);

            size_t left = atomic_fetch_sub_explicit(&tp->pending, 1, memory_order_acq_rel) - 1;
            if (left == 0) {
                atomic_store_explicit(&tp->task_count, 0, memory_order_release);
                tpool_wake_waiters_(tp);
            }
        }

        for (int k = 0; k < 32; k++) tpool_cpu_relax(k);
    }
}

// -------------------- API --------------------

static tpool_t *tpool_create(size_t num_threads)
{
    if (num_threads == 0) num_threads = 1;

    tpool_t *tp = (tpool_t *)calloc(1, sizeof(*tp));
    if (!tp) return NULL;

    tp->nthreads = num_threads;
    tp->threads = (pthread_t *)calloc(num_threads, sizeof(*tp->threads));
    if (!tp->threads) {
        free(tp);
        return NULL;
    }

    pthread_mutex_init(&tp->mtx, NULL);
    pthread_cond_init(&tp->cv_work, NULL);
    pthread_cond_init(&tp->cv_done, NULL);

    atomic_store(&tp->task_count, 0);
    atomic_store(&tp->next_index, 0);
    atomic_store(&tp->pending, 0);
    atomic_store(&tp->stop, 0);

    for (size_t i = 0; i < num_threads; i++) {
        if (pthread_create(&tp->threads[i], NULL, tpool_worker_, tp) != 0) {
            atomic_store(&tp->stop, 1);

            pthread_mutex_lock(&tp->mtx);
            pthread_cond_broadcast(&tp->cv_work);
            pthread_mutex_unlock(&tp->mtx);

            for (size_t j = 0; j < i; j++) pthread_join(tp->threads[j], NULL);

            pthread_cond_destroy(&tp->cv_done);
            pthread_cond_destroy(&tp->cv_work);
            pthread_mutex_destroy(&tp->mtx);
            free(tp->threads);
            free(tp);
            return NULL;
        }
    }

    return tp;
}

static tpool_task_handle *tpool_handle_create(tpool_t *tp, int count)
{
    return tpool_handle_acquire_(tp, count);
}

static inline void tpool_handle_destroy(tpool_task_handle *h)
{
    assert(h);
    assert(atomic_load_explicit(&h->remaining, memory_order_acquire) == 0);
    tpool_handle_release_(h);
}

// Reserve extra tasks in the current build batch.
// Main-thread-only and idle-only.
static int tpool_reserve_tasks(tpool_t *tp, size_t extra)
{
    assert(tp);
    tpool_assert_idle_(tp);
    return tpool_reserve_(tp, tp->build_count + extra) ? 0 : -1;
}

// Enqueue one task with a shared task handle.
// Main-thread-only and idle-only.
static int tpool_enqueue_with_handle(
    tpool_t *tp,
    tpool_task_fn fn,
    void *arg,
    tpool_task_handle *h
)
{
    assert(h);
    assert(h->owner == tp);
    return tpool_enqueue_(tp, fn, arg, h) ? 0 : -1;
}

// Enqueue one task and return a dedicated handle for this task.
// Main-thread-only and idle-only.
static tpool_task_handle *tpool_add_work(tpool_t *tp, tpool_task_fn fn, void *arg)
{
    assert(tp);
    assert(fn);

    tpool_task_handle *h = tpool_handle_create(tp, 1);
    if (!h) return NULL;

    if (!tpool_enqueue_(tp, fn, arg, h)) {
        atomic_store_explicit(&h->remaining, 0, memory_order_release);
        tpool_handle_release_(h);
        return NULL;
    }

    return h;
}

// Enqueue one task without allocating a handle.
// Useful for ECS-style batch enqueue+wait-all loops.
static int tpool_enqueue(tpool_t *tp, tpool_task_fn fn, void *arg)
{
    return tpool_enqueue_(tp, fn, arg, NULL) ? 0 : -1;
}

// Publish current batch once. Main-thread-only.
static void tpool_kick(tpool_t *tp)
{
    assert(tp);

    if (tpool_is_running_(tp)) return;

    size_t n = tp->build_count;
    if (n == 0) return;

    tp->build_count = 0;

    atomic_store_explicit(&tp->next_index, 0, memory_order_relaxed);
    atomic_store_explicit(&tp->pending, n, memory_order_release);
    atomic_store_explicit(&tp->task_count, n, memory_order_release);

    pthread_mutex_lock(&tp->mtx);
    pthread_cond_broadcast(&tp->cv_work);
    pthread_mutex_unlock(&tp->mtx);
}

// Wait for a specific task handle. Kicks automatically if needed.
static void tpool_wait_task(tpool_t *tp, tpool_task_handle *h)
{
    assert(tp);
    assert(h);

    if (!tpool_is_running_(tp)) tpool_kick(tp);
    tpool_handle_wait(h);
}

// Wait for current batch completion. Kicks automatically if needed.
static void tpool_wait_all(tpool_t *tp)
{
    assert(tp);

    if (!tpool_is_running_(tp)) tpool_kick(tp);

    pthread_mutex_lock(&tp->mtx);
    while (atomic_load_explicit(&tp->task_count, memory_order_acquire) != 0) {
        pthread_cond_wait(&tp->cv_done, &tp->mtx);
    }
    pthread_mutex_unlock(&tp->mtx);
}

// Compatibility alias for queue-style users (e.g. ECS callbacks).
static void tpool_wait(tpool_t *tp)
{
    tpool_wait_all(tp);
}

static void tpool_destroy(tpool_t *tp)
{
    assert(tp);

    // Drain queued and running work before shutdown.
    tpool_wait_all(tp);

    atomic_store_explicit(&tp->stop, 1, memory_order_relaxed);

    pthread_mutex_lock(&tp->mtx);
    pthread_cond_broadcast(&tp->cv_work);
    pthread_mutex_unlock(&tp->mtx);

    for (size_t i = 0; i < tp->nthreads; i++) {
        pthread_join(tp->threads[i], NULL);
    }

#ifndef NDEBUG
    assert(tp->handle_outstanding == 0);
#endif

    for (size_t s = 0; s < tp->handle_slab_count; s++) {
        free(tp->handle_slabs[s]);
    }

    pthread_cond_destroy(&tp->cv_done);
    pthread_cond_destroy(&tp->cv_work);
    pthread_mutex_destroy(&tp->mtx);

    free(tp->handle_slabs);
    free(tp->tasks);
    free(tp->threads);
    free(tp);
}
