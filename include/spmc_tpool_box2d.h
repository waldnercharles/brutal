#pragma once

#include "spmc_tpool.h"

#include <assert.h>
#include <stdlib.h>

typedef void b2TaskCallback(int startIndex, int endIndex, void *taskContext);

typedef void *(*b2EnqueueTaskCallback)(
    b2TaskCallback *task,
    int itemCount,
    int minRange,
    void *taskContext,
    void *userContext
);

typedef void (*b2FinishTaskCallback)(void *userTask, void *userContext);

typedef struct
{
    b2TaskCallback *cb;
    void *task_context;
    int start;
    int end;
} tpool_b2_range_call;

typedef struct
{
    tpool_t *tp;
    tpool_b2_range_call *calls;
    int calls_cap;
} tpool_b2_bridge;

static inline void tpool_b2_bridge_init(tpool_b2_bridge *bridge, tpool_t *tp)
{
    assert(bridge);
    assert(tp);
    bridge->tp = tp;
    bridge->calls = NULL;
    bridge->calls_cap = 0;
}

static inline void tpool_b2_bridge_destroy(tpool_b2_bridge *bridge)
{
    assert(bridge);
    free(bridge->calls);
    bridge->tp = NULL;
    bridge->calls = NULL;
    bridge->calls_cap = 0;
}

static int tpool_b2_range_thunk_(void *p)
{
    tpool_b2_range_call *call = (tpool_b2_range_call *)p;
    call->cb(call->start, call->end, call->task_context);
    return 0;
}

static void *tpool_b2_enqueue_task_(
    b2TaskCallback *task,
    int itemCount,
    int minRange,
    void *taskContext,
    void *userContext
)
{
    tpool_b2_bridge *bridge = (tpool_b2_bridge *)userContext;
    assert(bridge);
    assert(bridge->tp);

    if (!task || itemCount <= 0) return NULL;
    if (minRange <= 0) minRange = 1;

    int grain = minRange;
    int chunks = (itemCount + grain - 1) / grain;
    if (chunks <= 0) chunks = 1;

    if (bridge->calls_cap < chunks) {
        int new_cap = bridge->calls_cap ? bridge->calls_cap : 64;
        while (new_cap < chunks) new_cap *= 2;

        tpool_b2_range_call *calls = (tpool_b2_range_call *)realloc(
            bridge->calls,
            (size_t)new_cap * sizeof(*calls)
        );
        if (!calls) return NULL;

        bridge->calls = calls;
        bridge->calls_cap = new_cap;
    }

    if (tpool_reserve_tasks(bridge->tp, (size_t)chunks) != 0) return NULL;

    tpool_task_handle *h = tpool_handle_create(bridge->tp, chunks);
    if (!h) return NULL;

    for (int c = 0; c < chunks; c++) {
        int start = c * grain;
        int end = start + grain;
        if (end > itemCount) end = itemCount;

        bridge->calls[c] = (tpool_b2_range_call){
            .cb = task,
            .task_context = taskContext,
            .start = start,
            .end = end,
        };

        int rc = tpool_enqueue_with_handle(
            bridge->tp,
            tpool_b2_range_thunk_,
            &bridge->calls[c],
            h
        );
        assert(rc == 0);
        if (rc != 0) {
            // Should be unreachable because capacity was pre-reserved.
            return NULL;
        }
    }

    return (void *)h;
}

static void tpool_b2_finish_task_(void *userTask, void *userContext)
{
    tpool_b2_bridge *bridge = (tpool_b2_bridge *)userContext;
    tpool_task_handle *h = (tpool_task_handle *)userTask;
    assert(bridge);
    assert(bridge->tp);
    assert(h);

    tpool_wait_task(bridge->tp, h);
    tpool_handle_destroy(h);
}
