#ifndef SPMC_TPOOL_H
#define SPMC_TPOOL_H

#include <stddef.h>

typedef struct tpool tpool_t;
typedef struct tpool_task_handle tpool_task_handle;
typedef int (*tpool_task_fn)(void *arg);

tpool_t *tpool_create(size_t num_threads);
void tpool_destroy(tpool_t *tp);

tpool_task_handle *tpool_handle_create(tpool_t *tp, int count);
void tpool_handle_destroy(tpool_task_handle *h);

int tpool_reserve_tasks(tpool_t *tp, size_t extra);
int tpool_enqueue_with_handle(tpool_t *tp, tpool_task_fn fn, void *arg, tpool_task_handle *h);
tpool_task_handle *tpool_add_work(tpool_t *tp, tpool_task_fn fn, void *arg);
int tpool_enqueue(tpool_t *tp, tpool_task_fn fn, void *arg);

void tpool_kick(tpool_t *tp);
void tpool_wait_task(tpool_t *tp, tpool_task_handle *h);
void tpool_wait_all(tpool_t *tp);
void tpool_wait(tpool_t *tp);

#ifdef SPMC_TPOOL_IMPLEMENTATION

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#ifdef NDEBUG
#define TPOOL_ASSERT(expr) ((void)0)
#else
#ifndef TPOOL_ASSERT
#include <assert.h>
#define TPOOL_ASSERT(x) assert(x)
#endif
#endif

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

typedef struct
{
    tpool_task_fn fn;
    void *arg;
    tpool_task_handle *h;
} tpool_task;

struct tpool_task_handle
{
    cache_align atomic int remaining;
    tpool_t *owner;
    tpool_task_handle *next_free;
};

struct tpool
{
    pthread_t *threads;
    size_t nthreads;

    tpool_task *tasks;
    size_t cap;
    size_t build_count;

    cache_align atomic size_t task_count;
    cache_align atomic size_t next_index;
    cache_align atomic size_t pending;
    cache_align atomic bool stop;

    pthread_mutex_t mtx;
    pthread_cond_t cv_work;
    pthread_cond_t cv_done;

    tpool_task_handle *handle_free;
    tpool_task_handle **handle_slabs;
    size_t handle_slab_cap;
    size_t handle_slab_count;
#ifndef NDEBUG
    size_t handle_outstanding;
#endif
};

// -----------------------------------------------------------------------------
// Private implementation details

static inline int tpool_is_running(tpool_t *tp)
{
    return atomic_load_explicit(&tp->task_count, memory_order_acquire) != 0;
}

static inline void tpool_assert_idle(tpool_t *tp)
{
#ifndef NDEBUG
    TPOOL_ASSERT(!tpool_is_running(tp));
    TPOOL_ASSERT(atomic_load_explicit(&tp->pending, memory_order_acquire) == 0);
#else
    (void)tp;
#endif
}

static int tpool_grow_pow2(void **ptr, size_t *cap, size_t need, size_t elem_size, size_t min_cap)
{
    if (need <= *cap) return 1;

    size_t ncap = *cap ? *cap : min_cap;
    while (ncap < need) ncap *= 2;

    void *np = realloc(*ptr, ncap * elem_size);
    if (!np) return 0;

    *ptr = np;
    *cap = ncap;
    return 1;
}

static inline void tpool_wake_done(tpool_t *tp)
{
    pthread_mutex_lock(&tp->mtx);
    pthread_cond_broadcast(&tp->cv_done);
    pthread_mutex_unlock(&tp->mtx);
}

static inline int tpool_wait_for_work(tpool_t *tp)
{
    if (tpool_is_running(tp)) return 1;

    pthread_mutex_lock(&tp->mtx);
    while (!tpool_is_running(tp) &&
           !atomic_load_explicit(&tp->stop, memory_order_relaxed)) {
        pthread_cond_wait(&tp->cv_work, &tp->mtx);
    }

    int stop = atomic_load_explicit(&tp->stop, memory_order_relaxed);
    pthread_mutex_unlock(&tp->mtx);
    return !stop;
}

static int tpool_add_handle_slab(tpool_t *tp)
{
    if (!tpool_grow_pow2(
            (void **)&tp->handle_slabs,
            &tp->handle_slab_cap,
            tp->handle_slab_count + 1,
            sizeof(*tp->handle_slabs),
            4
        )) {
        return 0;
    }

    tpool_task_handle *slab = (tpool_task_handle *)
        calloc(TPOOL_HANDLE_SLAB_SIZE, sizeof(*slab));
    if (!slab) return 0;

    tp->handle_slabs[tp->handle_slab_count++] = slab;
    for (size_t i = 0; i < TPOOL_HANDLE_SLAB_SIZE; i++) {
        slab[i].owner = tp;
        slab[i].next_free = tp->handle_free;
        tp->handle_free = &slab[i];
    }

    return 1;
}

static tpool_task_handle *tpool_pop_handle(tpool_t *tp)
{
    TPOOL_ASSERT(tp);
    if (!tp->handle_free && !tpool_add_handle_slab(tp)) return NULL;

    tpool_task_handle *h = tp->handle_free;
    tp->handle_free = h->next_free;
    h->next_free = NULL;
    return h;
}

static inline void tpool_push_handle(tpool_task_handle *h)
{
    tpool_t *tp = h->owner;
    h->next_free = tp->handle_free;
    tp->handle_free = h;
#ifndef NDEBUG
    TPOOL_ASSERT(tp->handle_outstanding > 0);
    tp->handle_outstanding--;
#endif
}

static int tpool_enqueue_one(tpool_t *tp, tpool_task_fn fn, void *arg, tpool_task_handle *h)
{
    TPOOL_ASSERT(tp);
    TPOOL_ASSERT(fn);
    tpool_assert_idle(tp);

    if (!tpool_grow_pow2((void **)&tp->tasks, &tp->cap, tp->build_count + 1, sizeof(*tp->tasks), 1024))
        return 0;

    tp->tasks[tp->build_count++] = (tpool_task){ .fn = fn, .arg = arg, .h = h };
    return 1;
}

static inline void tpool_finish_task(tpool_t *tp, tpool_task_handle *h)
{
    if (h) {
        int left = atomic_fetch_sub_explicit(&h->remaining, 1, memory_order_acq_rel) - 1;
        if (left == 0) tpool_wake_done(tp);
    }

    size_t left = atomic_fetch_sub_explicit(&tp->pending, 1, memory_order_acq_rel) - 1;
    if (left == 0) {
        atomic_store_explicit(&tp->task_count, 0, memory_order_release);
        tpool_wake_done(tp);
    }
}

static void *tpool_worker(void *p)
{
    tpool_t *tp = (tpool_t *)p;

    while (tpool_wait_for_work(tp)) {
        for (;;) {
            size_t i = atomic_fetch_add_explicit(&tp->next_index, 1, memory_order_relaxed);
            size_t n = atomic_load_explicit(&tp->task_count, memory_order_acquire);
            if (i >= n) break;

            tpool_task t = tp->tasks[i];
            (void)t.fn(t.arg);
            tpool_finish_task(tp, t.h);
        }
    }

    return NULL;
}

tpool_t *tpool_create(size_t num_threads)
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
        if (pthread_create(&tp->threads[i], NULL, tpool_worker, tp) == 0)
            continue;

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

    return tp;
}

tpool_task_handle *tpool_handle_create(tpool_t *tp, int count)
{
    TPOOL_ASSERT(tp);
    TPOOL_ASSERT(count >= 0);
    tpool_assert_idle(tp);

    tpool_task_handle *h = tpool_pop_handle(tp);
    if (!h) return NULL;

    atomic_store_explicit(&h->remaining, count, memory_order_relaxed);
#ifndef NDEBUG
    tp->handle_outstanding++;
#endif
    return h;
}

void tpool_handle_destroy(tpool_task_handle *h)
{
    TPOOL_ASSERT(h);
    TPOOL_ASSERT(atomic_load_explicit(&h->remaining, memory_order_acquire) == 0);
    tpool_push_handle(h);
}

int tpool_reserve_tasks(tpool_t *tp, size_t extra)
{
    TPOOL_ASSERT(tp);
    tpool_assert_idle(tp);
    return tpool_grow_pow2((void **)&tp->tasks, &tp->cap, tp->build_count + extra, sizeof(*tp->tasks), 1024)
             ? 0
             : -1;
}

int tpool_enqueue_with_handle(tpool_t *tp, tpool_task_fn fn, void *arg, tpool_task_handle *h)
{
    TPOOL_ASSERT(h);
    TPOOL_ASSERT(h->owner == tp);
    return tpool_enqueue_one(tp, fn, arg, h) ? 0 : -1;
}

tpool_task_handle *tpool_add_work(tpool_t *tp, tpool_task_fn fn, void *arg)
{
    TPOOL_ASSERT(tp);
    TPOOL_ASSERT(fn);

    tpool_task_handle *h = tpool_handle_create(tp, 1);
    if (!h) return NULL;

    if (tpool_enqueue_one(tp, fn, arg, h)) return h;

    atomic_store_explicit(&h->remaining, 0, memory_order_release);
    tpool_push_handle(h);
    return NULL;
}

int tpool_enqueue(tpool_t *tp, tpool_task_fn fn, void *arg)
{
    return tpool_enqueue_one(tp, fn, arg, NULL) ? 0 : -1;
}

void tpool_kick(tpool_t *tp)
{
    TPOOL_ASSERT(tp);
    if (tpool_is_running(tp)) return;

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

void tpool_wait_task(tpool_t *tp, tpool_task_handle *h)
{
    TPOOL_ASSERT(tp);
    TPOOL_ASSERT(h);

    if (!tpool_is_running(tp)) tpool_kick(tp);

    pthread_mutex_lock(&tp->mtx);
    while (atomic_load_explicit(&h->remaining, memory_order_acquire) != 0) {
        pthread_cond_wait(&tp->cv_done, &tp->mtx);
    }
    pthread_mutex_unlock(&tp->mtx);
}

void tpool_wait_all(tpool_t *tp)
{
    TPOOL_ASSERT(tp);
    if (!tpool_is_running(tp)) tpool_kick(tp);

    pthread_mutex_lock(&tp->mtx);
    while (atomic_load_explicit(&tp->task_count, memory_order_acquire) != 0) {
        pthread_cond_wait(&tp->cv_done, &tp->mtx);
    }
    pthread_mutex_unlock(&tp->mtx);
}

void tpool_wait(tpool_t *tp)
{
    tpool_wait_all(tp);
}

void tpool_destroy(tpool_t *tp)
{
    TPOOL_ASSERT(tp);

    tpool_wait_all(tp);
    atomic_store_explicit(&tp->stop, 1, memory_order_relaxed);

    pthread_mutex_lock(&tp->mtx);
    pthread_cond_broadcast(&tp->cv_work);
    pthread_mutex_unlock(&tp->mtx);

    for (size_t i = 0; i < tp->nthreads; i++)
        pthread_join(tp->threads[i], NULL);

    TPOOL_ASSERT(tp->handle_outstanding == 0);

    for (size_t s = 0; s < tp->handle_slab_count; s++)
        free(tp->handle_slabs[s]);

    pthread_cond_destroy(&tp->cv_done);
    pthread_cond_destroy(&tp->cv_work);
    pthread_mutex_destroy(&tp->mtx);

    free(tp->handle_slabs);
    free(tp->tasks);
    free(tp->threads);
    free(tp);
}
#endif // SPMC_TPOOL_IMPLEMENTATION
#endif // SPMC_TPOOL_H
