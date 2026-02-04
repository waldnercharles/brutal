/**
 * pico_bench.h - Tiny benchmark framework (single header)
 *
 * Usage:
 *   #define PICO_BENCH_IMPLEMENTATION
 *   #include "pico_bench.h"
 */

#ifndef PICO_BENCH_H
#define PICO_BENCH_H

#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C"
{
#endif

    typedef struct pb_run
    {
        void *udata;
        int iteration;
        int is_warmup;
    } pb_run;

    typedef void (*pb_bench_fn)(pb_run *run);
    typedef void (*pb_hook_fn)(pb_run *run);
    typedef void (*pb_suite_fn)(void *suite_ctx);

#define BENCH_CASE(name) static void name(pb_run *pb_run_ctx)
#define BENCH_SETUP(name) static void name(pb_run *pb_run_ctx)
#define BENCH_TEARDOWN(name) static void name(pb_run *pb_run_ctx)
#define BENCH_SUITE(name) static void name(void *pb_suite_ctx)

#define BENCH_REQUIRE(expr)                                                    \
    do {                                                                       \
        if (!pb_require((expr) ? true : false, (#expr), __FILE__, __LINE__))   \
            return;                                                            \
    } while (false)

#define RUN_BENCH_CASE(bench_fn, setup_fn, teardown_fn, udata_ptr)             \
    pb_run_bench(#bench_fn, bench_fn, setup_fn, teardown_fn, udata_ptr)

#define RUN_BENCH_SUITE(suite_fn, suite_ctx)                                   \
    pb_run_suite(#suite_fn, suite_fn, suite_ctx)

    void pb_set_iterations(int iterations);
    void pb_set_warmup(int warmup);
    void pb_display_colors(bool enabled);
    void pb_display_cpu_time(bool enabled);
    void pb_print_stats();
    bool pb_failed();

    bool pb_require(bool passed, const char *expr, const char *file, int line);

    void pb_run_bench(
        const char *name,
        pb_bench_fn bench_fn,
        pb_hook_fn setup_fn,
        pb_hook_fn teardown_fn,
        void *udata
    );

    void pb_run_suite(const char *name, pb_suite_fn suite_fn, void *suite_ctx);

#ifdef __cplusplus
}
#endif

#endif // PICO_BENCH_H

#ifdef PICO_BENCH_IMPLEMENTATION

#include <math.h>
#include <stdio.h>
#include <time.h>

#define PB_TERM_COLOR_CODE 0x1B
#define PB_TERM_RED "[1;31m"
#define PB_TERM_GREEN "[1;32m"
#define PB_TERM_BOLD "[1m"
#define PB_TERM_RESET "[0m"

typedef struct pb_stats
{
    int samples;
    double mean;
    double m2;
    double min;
    double max;
} pb_stats;

static int pb_iterations = 50;
static int pb_warmup = 5;
static bool pb_colors = false;
static bool pb_cpu_time = true;

static int pb_num_benches = 0;
static int pb_num_failures = 0;
static int pb_num_asserts = 0;
static int pb_num_suites = 0;

static double pb_now_wall_ms_()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1000000.0;
}

static double pb_now_cpu_ms_()
{
    return 1000.0 * (double)clock() / (double)CLOCKS_PER_SEC;
}

static void pb_stats_reset_(pb_stats *s)
{
    s->samples = 0;
    s->mean = 0.0;
    s->m2 = 0.0;
    s->min = 0.0;
    s->max = 0.0;
}

static void pb_stats_push_(pb_stats *s, double x)
{
    if (s->samples == 0) {
        s->samples = 1;
        s->mean = x;
        s->m2 = 0.0;
        s->min = x;
        s->max = x;
        return;
    }

    s->samples++;
    if (x < s->min) s->min = x;
    if (x > s->max) s->max = x;

    double delta = x - s->mean;
    s->mean += delta / (double)s->samples;
    double delta2 = x - s->mean;
    s->m2 += delta * delta2;
}

static double pb_stats_stddev_(pb_stats *s)
{
    if (s->samples < 2) return 0.0;
    return sqrt(s->m2 / (double)(s->samples - 1));
}

static void pb_print_stats_row_(const char *label, pb_stats *s)
{
    printf(
        "| %-6s | %9.3f ms | %9.3f ms | %9.3f ms | %9.3f ms |\n",
        label,
        s->mean,
        s->min,
        s->max,
        pb_stats_stddev_(s)
    );
}

void pb_set_iterations(int iterations)
{
    if (iterations < 1) iterations = 1;
    pb_iterations = iterations;
}

void pb_set_warmup(int warmup)
{
    if (warmup < 0) warmup = 0;
    pb_warmup = warmup;
}

void pb_display_colors(bool enabled)
{
    pb_colors = enabled;
}

void pb_display_cpu_time(bool enabled)
{
    pb_cpu_time = enabled;
}

bool pb_failed()
{
    return pb_num_failures != 0;
}

bool pb_require(bool passed, const char *expr, const char *file, int line)
{
    pb_num_asserts++;
    if (passed) return true;

    pb_num_failures++;
    if (pb_colors) {
        printf(
            "(%c%sFAILED%c%s: %s (%d): %s)\n",
            PB_TERM_COLOR_CODE,
            PB_TERM_RED,
            PB_TERM_COLOR_CODE,
            PB_TERM_RESET,
            file,
            line,
            expr
        );
    } else {
        printf("(FAILED: %s (%d): %s)\n", file, line, expr);
    }

    return false;
}

void pb_run_bench(const char *name, pb_bench_fn bench_fn, pb_hook_fn setup_fn, pb_hook_fn teardown_fn, void *udata)
{
    pb_num_benches++;

    pb_stats wall;
    pb_stats cpu;
    pb_stats_reset_(&wall);
    pb_stats_reset_(&cpu);

    pb_run run = { .udata = udata, .iteration = 0, .is_warmup = 0 };

    for (int i = 0; i < pb_warmup; i++) {
        run.iteration = i;
        run.is_warmup = 1;
        if (setup_fn) setup_fn(&run);
        bench_fn(&run);
        if (teardown_fn) teardown_fn(&run);
    }

    for (int i = 0; i < pb_iterations; i++) {
        run.iteration = i;
        run.is_warmup = 0;

        if (setup_fn) setup_fn(&run);
        double wall_start = pb_now_wall_ms_();
        double cpu_start = pb_now_cpu_ms_();
        bench_fn(&run);
        double cpu_elapsed = pb_now_cpu_ms_() - cpu_start;
        double wall_elapsed = pb_now_wall_ms_() - wall_start;
        if (teardown_fn) teardown_fn(&run);

        pb_stats_push_(&wall, wall_elapsed);
        pb_stats_push_(&cpu, cpu_elapsed);
    }

    printf("----------------------------------------------------------------------\n");
    if (pb_colors) {
        printf("| %c%s%-66s%c%s |\n", PB_TERM_COLOR_CODE, PB_TERM_BOLD, name, PB_TERM_COLOR_CODE, PB_TERM_RESET);
    } else {
        printf("| %-66s |\n", name);
    }
    printf("----------------------------------------------------------------------\n");
    printf("| %-6s | %12s | %12s | %12s | %12s |\n", "metric", "mean", "min", "max", "stddev");
    printf("---------+--------------+--------------+--------------+--------------+\n");
    pb_print_stats_row_("wall", &wall);

    if (pb_cpu_time) { pb_print_stats_row_("cpu", &cpu); }
    printf("\n");
}

void pb_run_suite(const char *name, pb_suite_fn suite_fn, void *suite_ctx)
{
    pb_num_suites++;
    if (pb_colors) {
        printf(
            "======================================================================\n"
            "%c%sRunning Suite: %s%c%s\n"
            "----------------------------------------------------------------------\n",
            PB_TERM_COLOR_CODE,
            PB_TERM_BOLD,
            name,
            PB_TERM_COLOR_CODE,
            PB_TERM_RESET
        );
    } else {
        printf(
            "======================================================================\n"
            "Running Suite: %s\n"
            "----------------------------------------------------------------------\n",
            name
        );
    }

    suite_fn(suite_ctx);
}

void pb_print_stats()
{
    if (pb_colors) {
        printf(
            "======================================================================\n"
            "Summary: benches %-4d suites %-4d asserts %-6d failures "
            "%c%s%-4d%c%s\n",
            pb_num_benches,
            pb_num_suites,
            pb_num_asserts,
            PB_TERM_COLOR_CODE,
            pb_num_failures ? PB_TERM_RED : PB_TERM_GREEN,
            pb_num_failures,
            PB_TERM_COLOR_CODE,
            PB_TERM_RESET
        );
    } else {
        printf(
            "======================================================================\n"
            "Summary: benches %-4d suites %-4d asserts %-6d failures %d\n",
            pb_num_benches,
            pb_num_suites,
            pb_num_asserts,
            pb_num_failures
        );
    }
}

#endif // PICO_BENCH_IMPLEMENTATION
