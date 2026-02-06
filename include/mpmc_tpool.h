
#include <assert.h>
#include <pthread.h>
#include <stdalign.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifndef CACHE_LINE
#define CACHE_LINE 64
#endif

#ifndef TPOOL_DEFAULT_QUEUE_SIZE
#define TPOOL_DEFAULT_QUEUE_SIZE 1024u
#endif

#if defined(__x86_64__) || defined(__i386__)
#define tpool_relax() __asm__ __volatile__("pause" ::: "memory")
#elif defined(__aarch64__) || defined(__arm__)
#define tpool_relax() __asm__ __volatile__("yield" ::: "memory")
#else
#define tpool_relax() ((void)0)
#endif

typedef struct
{
    int (*fn)(void *);
    void *arg;
} tpool_job_t;

#define JOB_SIZE (sizeof(tpool_job_t))

typedef struct
{
    alignas(CACHE_LINE) _Atomic int turn;
    uint8_t data[JOB_SIZE];
} tpool_slot_t;

typedef struct
{
    alignas(CACHE_LINE) _Atomic int head;
    alignas(CACHE_LINE) _Atomic int tail;
    int capacity;
    tpool_slot_t *slots;
} tpool_queue_t;

static inline void queue_init(tpool_queue_t *q, int capacity)
{
    if (capacity == 0) capacity = TPOOL_DEFAULT_QUEUE_SIZE;
    q->capacity = capacity;
    q->slots = (tpool_slot_t *)calloc(capacity, sizeof(tpool_slot_t));
    assert(q->slots);
    atomic_store_explicit(&q->head, 0, memory_order_relaxed);
    atomic_store_explicit(&q->tail, 0, memory_order_relaxed);
    for (int i = 0; i < capacity; i++)
        atomic_store_explicit(&q->slots[i].turn, 0, memory_order_relaxed);
}

static inline bool try_enqueue(tpool_queue_t *q, const void *item)
{
    int head = atomic_load_explicit(&q->head, memory_order_acquire);
    for (;;) {
        tpool_slot_t *s = &q->slots[head % q->capacity];
        int want = (head / q->capacity) * 2;
        if (want == atomic_load_explicit(&s->turn, memory_order_acquire)) {
            if (atomic_compare_exchange_strong_explicit(&q->head, &head, head + 1, memory_order_acq_rel, memory_order_acquire)) {
                memcpy(s->data, item, JOB_SIZE);
                atomic_store_explicit(&s->turn, want + 1, memory_order_release);
                return true;
            } else {
                tpool_relax();
            }
        } else {
            int prev = head;
            head = atomic_load_explicit(&q->head, memory_order_acquire);
            if (head == prev) return false; // appears full
            tpool_relax();
        }
    }
}

static inline bool try_dequeue(tpool_queue_t *q, void *item)
{
    int tail = atomic_load_explicit(&q->tail, memory_order_acquire);
    for (;;) {
        tpool_slot_t *s = &q->slots[tail % q->capacity];
        int want = (tail / q->capacity) * 2 + 1;

        if (want == atomic_load_explicit(&s->turn, memory_order_acquire)) {
            if (atomic_compare_exchange_strong_explicit(&q->tail, &tail, tail + 1, memory_order_acq_rel, memory_order_acquire)) {
                memcpy(item, s->data, JOB_SIZE);
                atomic_store_explicit(&s->turn, want + 1, memory_order_release);
                return true;
            } else {
                tpool_relax();
            }
        } else {
            int prev = tail;
            tail = atomic_load_explicit(&q->tail, memory_order_acquire);
            if (tail == prev) return false; // appears empty
            tpool_relax();
        }
    }
}

// ------------------------------ Thread Pool  ------------------------------

typedef struct
{
    tpool_queue_t q;

    pthread_t *threads;
    int nthreads;

    alignas(CACHE_LINE) _Atomic int queued; // number of jobs currently in the ring
    alignas(CACHE_LINE) _Atomic int in_flight; // jobs not yet completed (queued + running)
    alignas(CACHE_LINE) _Atomic bool stop;

    pthread_mutex_t mtx;
    pthread_cond_t cv_work; // workers wait for queued == 0
    pthread_cond_t cv_done; // waiters wait for in_flight == 0
} tpool_t;

static inline void tpool_job_done(tpool_t *p)
{
    int n = atomic_fetch_sub_explicit(&p->in_flight, 1, memory_order_acq_rel) - 1;
    if (n == 0) {
        pthread_mutex_lock(&p->mtx);
        pthread_cond_broadcast(&p->cv_done);
        pthread_mutex_unlock(&p->mtx);
    }
}
static void *tpool_worker(void *arg)
{
    tpool_t *p = arg;

    for (;;) {
        if (atomic_load_explicit(&p->queued, memory_order_acquire) != 0) {
            tpool_job_t job;
            if (try_dequeue(&p->q, &job)) {
                atomic_fetch_sub_explicit(&p->queued, 1, memory_order_acq_rel);
                job.fn(job.arg);
                tpool_job_done(p);
                continue;
            }
            tpool_relax();
        }

        // No job visible right now.
        if (atomic_load_explicit(&p->stop, memory_order_acquire) &&
            atomic_load_explicit(&p->in_flight, memory_order_acquire) == 0) {
            return NULL;
        }

        // Park until work arrives or stop requested.
        pthread_mutex_lock(&p->mtx);
        while (!atomic_load_explicit(&p->stop, memory_order_relaxed) &&
               atomic_load_explicit(&p->queued, memory_order_relaxed) == 0) {
            pthread_cond_wait(&p->cv_work, &p->mtx);
        }
        pthread_mutex_unlock(&p->mtx);
    }
}

static inline tpool_t *tpool_init(int nthreads, int queue_capacity)
{
    if (nthreads <= 0) nthreads = 1;

    tpool_t *p = (tpool_t *)calloc(1, sizeof(*p));
    assert(p);

    queue_init(&p->q, queue_capacity);
    atomic_store_explicit(&p->queued, 0, memory_order_relaxed);
    atomic_store_explicit(&p->in_flight, 0, memory_order_relaxed);
    atomic_store_explicit(&p->stop, false, memory_order_relaxed);

    // clang-format off
    if (pthread_mutex_init(&p->mtx, NULL) != 0) assert(0 && "Failed to init mutex");
    if (pthread_cond_init(&p->cv_work, NULL) != 0) assert(0 && "Failed to init condvar");
    if (pthread_cond_init(&p->cv_done, NULL) != 0) assert(0 && "Failed to init condvar");
    // clang-format on

    p->threads = (pthread_t *)calloc(nthreads, sizeof(*p->threads));
    assert(p->threads);
    p->nthreads = nthreads;

    for (int i = 0; i < nthreads; i++) {
        if (pthread_create(&p->threads[i], NULL, tpool_worker, p) != 0) {
            atomic_store_explicit(&p->stop, true, memory_order_release);

            pthread_mutex_lock(&p->mtx);
            pthread_cond_broadcast(&p->cv_work);
            pthread_mutex_unlock(&p->mtx);

            for (int j = 0; j < i; j++) pthread_join(p->threads[j], NULL);
            assert(0 && "Failed to create thread");
        }
    }

    return p;
}

// Submit one job. Runs synchronously when full.
static inline void tpool_submit(tpool_t *p, int (*fn)(void *), void *arg)
{
    assert(p);

    if (!fn) return;
    if (atomic_load_explicit(&p->stop, memory_order_acquire)) return;

    // Reserve completion slot first. Prevents tpool_wait from observing 0 spuriously
    atomic_fetch_add_explicit(&p->in_flight, 1, memory_order_acq_rel);

    tpool_job_t job = { fn, arg };

    if (try_enqueue(&p->q, &job)) {
        // Avoid waking all at once.
        // Wake at most N workers per burt. (Signals first nthreads, enqueus after idle.)
        int prev = atomic_fetch_add_explicit(&p->queued, 1, memory_order_release);
        if (prev < p->nthreads) {
            pthread_mutex_lock(&p->mtx);
            pthread_cond_signal(&p->cv_work);
            pthread_mutex_unlock(&p->mtx);
        }
    } else {
        // Queue is full: Just run it immediately.
        fn(arg);
        tpool_job_done(p);
    }
}

// Wait until all submitted jobs complete.
static inline void tpool_wait(tpool_t *p)
{
    assert(p);

    for (;;) {
        if (atomic_load_explicit(&p->in_flight, memory_order_acquire) == 0)
            return;

        // Maybe we can help!
        if (atomic_load_explicit(&p->queued, memory_order_acquire) != 0) {
            tpool_job_t job;
            if (try_dequeue(&p->q, &job)) {
                atomic_fetch_sub_explicit(&p->queued, 1, memory_order_acq_rel);
                job.fn(job.arg);
                tpool_job_done(p);
                continue;
            } else {
                tpool_relax();
            }
        }

        // Nothing left to help with right now; Zzz Zzz.
        pthread_mutex_lock(&p->mtx);
        while (atomic_load_explicit(&p->in_flight, memory_order_acquire) != 0 &&
               atomic_load_explicit(&p->queued, memory_order_acquire) == 0) {
            pthread_cond_wait(&p->cv_done, &p->mtx);
        }
        pthread_mutex_unlock(&p->mtx);
    }
}

// Stop workers after draining current work, then join.
static inline void tpool_destroy(tpool_t *p)
{
    if (!p) return;

    tpool_wait(p);

    atomic_store_explicit(&p->stop, true, memory_order_release);

    pthread_mutex_lock(&p->mtx);
    pthread_cond_broadcast(&p->cv_work);
    pthread_mutex_unlock(&p->mtx);

    for (int i = 0; i < p->nthreads; i++) pthread_join(p->threads[i], NULL);
    free(p->threads);
    free(p->q.slots);

    pthread_cond_destroy(&p->cv_work);
    pthread_cond_destroy(&p->cv_done);
    pthread_mutex_destroy(&p->mtx);

    free(p);
}
