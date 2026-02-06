/**
 * brutal_bench.h - Minimalist benchmark framework (single header)
 *
 * Usage:
 *   #define BRUTAL_BENCH_IMPLEMENTATION
 *   #include "brutal_bench.h"
 */

#ifndef BRUTAL_BENCH_H
#define BRUTAL_BENCH_H

#include <stdbool.h>
#include <stddef.h>

typedef struct bb_run
{
    void *udata;
    int iteration;
    int is_warmup;
} bb_run;

typedef void (*bb_bench_fn)(bb_run *run);
typedef void (*bb_hook_fn)(bb_run *run);
typedef void (*bb_suite_fn)(void *suite_ctx);

#define BENCH_CASE(name) static void name(bb_run *bb_run_ctx)
#define BENCH_SETUP(name) static void name(bb_run *bb_run_ctx)
#define BENCH_TEARDOWN(name) static void name(bb_run *bb_run_ctx)
#define BENCH_SUITE(name) static void name(void *bb_suite_ctx)

#define BENCH_REQUIRE(expr)                                                    \
    do {                                                                       \
        if (!bb_require_((expr) ? true : false, (#expr), __FILE__, __LINE__))  \
            return;                                                            \
    } while (false)

#define RUN_BENCH_CASE(bench_fn, setup_fn, teardown_fn, udata_ptr)             \
    bb_run_bench_(#bench_fn, bench_fn, setup_fn, teardown_fn, udata_ptr)

#define RUN_BENCH_SUITE(suite_fn, suite_ctx)                                   \
    bb_run_suite_(#suite_fn, suite_fn, suite_ctx)

// -----------------------------------------------------------------------------
// Public API

void bb_set_iterations(int iterations);
void bb_set_warmup(int warmup);
void bb_display_colors(bool enabled);
void bb_display_cpu_time(bool enabled);
void bb_print_stats(void);
bool bb_failed(void);

// -----------------------------------------------------------------------------
// Internal (used by macros, not intended for direct use)

bool bb_require_(bool passed, const char *expr, const char *file, int line);

void bb_run_bench_(
    const char *name,
    bb_bench_fn bench_fn,
    bb_hook_fn setup_fn,
    bb_hook_fn teardown_fn,
    void *udata
);

void bb_run_suite_(const char *name, bb_suite_fn suite_fn, void *suite_ctx);

#endif // BRUTAL_BENCH_H

#ifdef BRUTAL_BENCH_IMPLEMENTATION

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define BB_TERM_COLOR_CODE 0x1B
#define BB_TERM_RED "[1;31m"
#define BB_TERM_GREEN "[1;32m"
#define BB_TERM_BOLD "[1m"
#define BB_TERM_RESET "[0m"

typedef struct bb_stats
{
    int samples;
    int capacity;
    double *data;
    double mean;
    double m2;
    double min;
    double max;
} bb_stats;

static int bb_iterations = 50;
static int bb_warmup = 5;
static bool bb_colors = false;
static bool bb_cpu_time = true;

static int bb_num_benches = 0;
static int bb_num_failures = 0;
static int bb_num_asserts = 0;
static int bb_num_suites = 0;

// -----------------------------------------------------------------------------
// Private implementation details

static double bb_now_wall_ms_(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1000000.0;
}

static double bb_now_cpu_ms_(void)
{
    return 1000.0 * (double)clock() / (double)CLOCKS_PER_SEC;
}

static void bb_stats_reset_(bb_stats *s, int capacity)
{
    s->samples = 0;
    s->capacity = capacity;
    s->data = capacity > 0 ? malloc(sizeof(double) * capacity) : NULL;
    s->mean = 0.0;
    s->m2 = 0.0;
    s->min = 0.0;
    s->max = 0.0;
}

static void bb_stats_free_(bb_stats *s)
{
    if (s->data) {
        free(s->data);
        s->data = NULL;
    }
}

static void bb_stats_push_(bb_stats *s, double x)
{
    if (s->samples == 0) {
        s->samples = 1;
        s->mean = x;
        s->m2 = 0.0;
        s->min = x;
        s->max = x;
        if (s->data) s->data[0] = x;
        return;
    }

    s->samples++;
    if (x < s->min) s->min = x;
    if (x > s->max) s->max = x;

    double delta = x - s->mean;
    s->mean += delta / (double)s->samples;
    double delta2 = x - s->mean;
    s->m2 += delta * delta2;

    if (s->data && s->samples <= s->capacity) {
        s->data[s->samples - 1] = x;
    }
}

static double bb_stats_stddev_(bb_stats *s)
{
    if (s->samples < 2) return 0.0;
    return sqrt(s->m2 / (double)(s->samples - 1));
}

static int bb_compare_double_(const void *a, const void *b)
{
    double da = *(const double *)a;
    double db = *(const double *)b;
    if (da < db) return -1;
    if (da > db) return 1;
    return 0;
}

static double bb_stats_median_(bb_stats *s)
{
    if (s->samples == 0 || !s->data) return 0.0;
    if (s->samples == 1) return s->data[0];

    qsort(s->data, s->samples, sizeof(double), bb_compare_double_);

    if (s->samples % 2 == 0) {
        int mid = s->samples / 2;
        return (s->data[mid - 1] + s->data[mid]) / 2.0;
    } else {
        return s->data[s->samples / 2];
    }
}

static void bb_print_stats_row_(const char *label, bb_stats *s)
{
    double median = bb_stats_median_(s);
    double stddev = bb_stats_stddev_(s);

    printf(
        "| %-4s | %8.3f | %8.3f | %8.3f | %8.3f | %8.3f | %8.3f |\n",
        label,
        s->min,
        median,
        s->mean,
        s->max,
        stddev,
        (stddev / s->mean) * 100.0
    );
}

// -----------------------------------------------------------------------------
// Public API

void bb_set_iterations(int iterations)
{
    if (iterations < 1) iterations = 1;
    bb_iterations = iterations;
}

void bb_set_warmup(int warmup)
{
    if (warmup < 0) warmup = 0;
    bb_warmup = warmup;
}

void bb_display_colors(bool enabled)
{
    bb_colors = enabled;
}

void bb_display_cpu_time(bool enabled)
{
    bb_cpu_time = enabled;
}

bool bb_failed(void)
{
    return bb_num_failures != 0;
}

bool bb_require_(bool passed, const char *expr, const char *file, int line)
{
    bb_num_asserts++;
    if (passed) return true;

    bb_num_failures++;
    if (bb_colors) {
        printf(
            "(%c%sFAILED%c%s: %s (%d): %s)\n",
            BB_TERM_COLOR_CODE,
            BB_TERM_RED,
            BB_TERM_COLOR_CODE,
            BB_TERM_RESET,
            file,
            line,
            expr
        );
    } else {
        printf("(FAILED: %s (%d): %s)\n", file, line, expr);
    }

    return false;
}

void bb_run_bench_(const char *name, bb_bench_fn bench_fn, bb_hook_fn setup_fn, bb_hook_fn teardown_fn, void *udata)
{
    bb_num_benches++;

    bb_stats wall;
    bb_stats cpu;
    bb_stats_reset_(&wall, bb_iterations);
    bb_stats_reset_(&cpu, bb_iterations);

    bb_run run = { .udata = udata, .iteration = 0, .is_warmup = 0 };

    for (int i = 0; i < bb_warmup; i++) {
        run.iteration = i;
        run.is_warmup = 1;
        if (setup_fn) setup_fn(&run);
        bench_fn(&run);
        if (teardown_fn) teardown_fn(&run);
    }

    for (int i = 0; i < bb_iterations; i++) {
        run.iteration = i;
        run.is_warmup = 0;

        if (setup_fn) setup_fn(&run);
        double wall_start = bb_now_wall_ms_();
        double cpu_start = bb_now_cpu_ms_();
        bench_fn(&run);
        double cpu_elapsed = bb_now_cpu_ms_() - cpu_start;
        double wall_elapsed = bb_now_wall_ms_() - wall_start;
        if (teardown_fn) teardown_fn(&run);

        bb_stats_push_(&wall, wall_elapsed);
        bb_stats_push_(&cpu, cpu_elapsed);
    }

    printf("============================================================================\n");
    if (bb_colors) {
        printf("| %c%s%-74s%c%s |\n", BB_TERM_COLOR_CODE, BB_TERM_BOLD, name, BB_TERM_COLOR_CODE, BB_TERM_RESET);
    } else {
        printf("| %-74s |\n", name);
    }
    printf("============================================================================\n");
    printf("|      |      min |   median |     mean |      max |   stddev |   cv%% |\n");
    printf("+------+----------+----------+----------+----------+----------+--------+\n");
    bb_print_stats_row_("wall", &wall);

    if (bb_cpu_time) { bb_print_stats_row_("cpu", &cpu); }
    printf("\n");

    bb_stats_free_(&wall);
    bb_stats_free_(&cpu);
}

void bb_run_suite_(const char *name, bb_suite_fn suite_fn, void *suite_ctx)
{
    bb_num_suites++;
    if (bb_colors) {
        printf(
            "============================================================================\n"
            "%c%sRunning Suite: %s%c%s\n"
            "----------------------------------------------------------------------------\n",
            BB_TERM_COLOR_CODE,
            BB_TERM_BOLD,
            name,
            BB_TERM_COLOR_CODE,
            BB_TERM_RESET
        );
    } else {
        printf(
            "============================================================================\n"
            "Running Suite: %s\n"
            "----------------------------------------------------------------------------\n",
            name
        );
    }

    suite_fn(suite_ctx);
}

void bb_print_stats(void)
{
    if (bb_colors) {
        printf(
            "============================================================================\n"
            "Summary: benches %-4d suites %-4d asserts %-6d failures %c%s%-4d%c%s\n",
            bb_num_benches,
            bb_num_suites,
            bb_num_asserts,
            BB_TERM_COLOR_CODE,
            bb_num_failures ? BB_TERM_RED : BB_TERM_GREEN,
            bb_num_failures,
            BB_TERM_COLOR_CODE,
            BB_TERM_RESET
        );
    } else {
        printf(
            "============================================================================\n"
            "Summary: benches %-4d suites %-4d asserts %-6d failures %d\n",
            bb_num_benches,
            bb_num_suites,
            bb_num_asserts,
            bb_num_failures
        );
    }
}

#endif // BRUTAL_BENCH_IMPLEMENTATION
