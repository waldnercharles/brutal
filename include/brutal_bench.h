#ifndef BRUTAL_BENCH_H
#define BRUTAL_BENCH_H

#include <stdbool.h>
#include <stddef.h>

struct benchmark_s;
typedef struct benchmark_s benchmark_t;

typedef void (*bench_report_fn)(benchmark_t *b);

void bench_start(benchmark_t *b);
void bench_stop(benchmark_t *b);
void bench_clear(benchmark_t *b);

double bench_get_min_real(benchmark_t *b);
double bench_get_min_cpu(benchmark_t *b);
double bench_get_max_real(benchmark_t *b);
double bench_get_max_cpu(benchmark_t *b);
double bench_get_mean_real(benchmark_t *b);
double bench_get_mean_cpu(benchmark_t *b);
double bench_get_sum_real(benchmark_t *b);
double bench_get_sum_cpu(benchmark_t *b);
double bench_get_variance_real(benchmark_t *b);
double bench_get_variance_cpu(benchmark_t *b);

void bench_default_reporter_stdout(benchmark_t *data);
void bench_report(benchmark_t *b);

typedef struct bench_run
{
    void *udata;
    int iteration;
    bool is_warmup;
} bench_run;

typedef void (*bench_bench_fn)(bench_run *run);
typedef void (*bench_hook_fn)(bench_run *run);
typedef void (*bench_suite_fn)(void *suite_ctx);

#define BENCH_CASE(name) static void name(bench_run *bench_run_ctx)
#define BENCH_SETUP(name) static void name(bench_run *bench_run_ctx)
#define BENCH_TEARDOWN(name) static void name(bench_run *bench_run_ctx)
#define BENCH_SUITE(name) static void name(void *bench_suite_ctx)

#define BENCH_REQUIRE(expr)                                                     \
    do {                                                                        \
        if (!bench_require((expr) ? true : false, (#expr), __FILE__, __LINE__)) \
            return;                                                             \
    } while (0)

#define RUN_BENCH_CASE(bench_fn, setup_fn, teardown_fn, udata_ptr)             \
    bench_run_bench(#bench_fn, bench_fn, setup_fn, teardown_fn, udata_ptr)

#define RUN_BENCH_SUITE(suite_fn, suite_ctx)                                   \
    bench_run_suite(#suite_fn, suite_fn, suite_ctx)

void bench_set_iterations(int iterations);
void bench_set_warmup(int warmup);
void bench_display_colors(bool enabled);
void bench_print_stats();
bool bench_failed();

bool bench_require(bool passed, const char *expr, const char *file, int line);

void bench_run_bench(
    const char *name,
    bench_bench_fn bench_fn,
    bench_hook_fn setup_fn,
    bench_hook_fn teardown_fn,
    void *udata
);

void bench_run_suite(const char *name, bench_suite_fn suite_fn, void *suite_ctx);

#ifdef BRUTAL_BENCH_IMPLEMENTATION

#include <stdio.h>
#include <time.h>

struct benchmark_s
{
    double min_cpu, min_real;
    double max_cpu, max_real;
    double sum_cpu, sum_real;
    double mean_cpu, mean_real;

    double M2_cpu, M2_real; // Running sum of squared deviations from the mean.
    double variance_cpu, variance_real;
    long unsigned int iterations;

    clock_t start_time_cpu;
    struct timespec start_time_real;
};

void bench_start(benchmark_t *b)
{
    if (!b) return;
    b->start_time_cpu = clock();
    clock_gettime(CLOCK_MONOTONIC, &b->start_time_real);
    return;
}

void bench_stop(benchmark_t *b)
{
    if (!b) return;

    double stop_time_cpu = clock();
    struct timespec stop_time_real;
    clock_gettime(CLOCK_MONOTONIC, &stop_time_real);
    double diff_cpu = (double)(stop_time_cpu - b->start_time_cpu) / CLOCKS_PER_SEC;
    double diff_real = (stop_time_real.tv_sec - b->start_time_real.tv_sec) +
                       (stop_time_real.tv_nsec - b->start_time_real.tv_nsec) / 1e9;

    if (diff_cpu < b->min_cpu || b->min_cpu == 0.0) b->min_cpu = diff_cpu;
    if (diff_real < b->min_real || b->min_real == 0.0) b->min_real = diff_real;
    if (diff_cpu > b->max_cpu) b->max_cpu = diff_cpu;
    if (diff_real > b->max_real) b->max_real = diff_real;

    b->sum_cpu += diff_cpu;
    b->sum_real += diff_real;
    b->iterations++;

    // Welford's online algorithm to calculate variance
    double delta_cpu = diff_cpu - b->mean_cpu;
    b->mean_cpu += delta_cpu / b->iterations;
    double delta2_cpu = diff_cpu - b->mean_cpu;
    b->M2_cpu += delta_cpu * delta2_cpu;
    b->variance_cpu = b->M2_cpu / b->iterations;

    double delta_real = diff_real - b->mean_real;
    b->mean_real += delta_real / b->iterations;
    double delta2_real = diff_real - b->mean_real;
    b->M2_real += delta_real * delta2_real;
    b->variance_real = b->M2_real / b->iterations;

    return;
}

void bench_clear(benchmark_t *b)
{
    if (!b) return;
    *b = (benchmark_t){ 0 };
    return;
}

double bench_get_min_real(benchmark_t *b)
{
    return b->min_real;
}

double bench_get_min_cpu(benchmark_t *b)
{
    return b->min_cpu;
}

double bench_get_max_real(benchmark_t *b)
{
    return b->max_real;
}

double bench_get_max_cpu(benchmark_t *b)
{
    return b->max_cpu;
}

double bench_get_mean_real(benchmark_t *b)
{
    return b->mean_real;
}

double bench_get_mean_cpu(benchmark_t *b)
{
    return b->mean_cpu;
}

double bench_get_sum_real(benchmark_t *b)
{
    return b->max_real;
}

double bench_get_sum_cpu(benchmark_t *b)
{
    return b->max_cpu;
}

double bench_get_variance_real(benchmark_t *b)
{
    return b->variance_real;
}

double bench_get_variance_cpu(benchmark_t *b)
{
    return b->variance_cpu;
}

#include <math.h>

#define BENCH_TERM_COLOR_CODE 0x1B
#define BENCH_TERM_RED "[1;31m"
#define BENCH_TERM_GREEN "[1;32m"
#define BENCH_TERM_BOLD "[1m"
#define BENCH_TERM_RESET "[0m"

static int bench_iterations = 50;
static int bench_warmup = 5;
static bool bench_colors = true;

static int bench_num_benches = 0;
static int bench_num_failures = 0;
static int bench_num_asserts = 0;
static int bench_num_suites = 0;

// Column widths (interior space between separators)
#define COL_LABEL_WIDTH 6
#define COL_MIN_WIDTH 10
#define COL_MEAN_WIDTH 10
#define COL_MAX_WIDTH 10
#define COL_STDDEV_WIDTH 10
#define COL_CV_WIDTH 8

static void bench_print_line(int n)
{
    for (int i = 0; i < n; i++) printf("─");
}

static void bench_print_border(const char *left, const char *sep, const char *right)
{
    printf("%s", left);
    bench_print_line(COL_LABEL_WIDTH);
    printf("%s", sep);
    bench_print_line(COL_MIN_WIDTH);
    printf("%s", sep);
    bench_print_line(COL_MEAN_WIDTH);
    printf("%s", sep);
    bench_print_line(COL_MAX_WIDTH);
    printf("%s", sep);
    bench_print_line(COL_STDDEV_WIDTH);
    printf("%s", sep);
    bench_print_line(COL_CV_WIDTH);
    printf("%s\n", right);
}

static void bench_print_top_border()
{
    bench_print_border("┌", "┬", "┐");
}

static void bench_print_separator()
{
    bench_print_border("├", "┼", "┤");
}

static void bench_print_bottom_border()
{
    bench_print_border("└", "┴", "┘");
}

static void bench_print_stats_row(const char *label, double min, double mean, double max, double stddev)
{
    // Convert seconds to milliseconds
    min *= 1000.0;
    mean *= 1000.0;
    max *= 1000.0;
    stddev *= 1000.0;

    printf(
        "│ %-4s │ %8.3f │ %8.3f │ %8.3f │ %8.3f │ %6.2f │\n",
        label,
        min,
        mean,
        max,
        stddev,
        (stddev / mean) * 100.0
    );
}

static void bench_print_wall_results(benchmark_t *b)
{
    double variance = bench_get_variance_real(b);
    bench_print_stats_row(
        "wall",
        bench_get_min_real(b),
        bench_get_mean_real(b),
        bench_get_max_real(b),
        sqrt(variance)
    );
}

static void bench_print_cpu_results(benchmark_t *b)
{
    double variance = bench_get_variance_cpu(b);
    bench_print_stats_row(
        "cpu",
        bench_get_min_cpu(b),
        bench_get_mean_cpu(b),
        bench_get_max_cpu(b),
        sqrt(variance)
    );
}

void bench_default_reporter_stdout(benchmark_t *b)
{
    bench_print_top_border();
    printf("│      │      min │     mean │      max │   stddev │    cv%% │\n");
    bench_print_separator();
    bench_print_wall_results(b);
    bench_print_cpu_results(b);
    bench_print_bottom_border();
}

void bench_report_with(benchmark_t *b, bench_report_fn reporter)
{
    reporter(b);
}

void bench_report(benchmark_t *b)
{
    bench_report_with(b, bench_default_reporter_stdout);
}

void bench_set_iterations(int iterations)
{
    if (iterations < 1) iterations = 1;
    bench_iterations = iterations;
}

void bench_set_warmup(int warmup)
{
    if (warmup < 0) warmup = 0;
    bench_warmup = warmup;
}

void bench_display_colors(bool enabled)
{
    bench_colors = enabled;
}

bool bench_failed()
{
    return bench_num_failures != 0;
}

bool bench_require(bool passed, const char *expr, const char *file, int line)
{
    bench_num_asserts++;
    if (passed) return true;

    bench_num_failures++;
    if (bench_colors) {
        printf(
            "(%c%sFAILED%c%s: %s (%d): %s)\n",
            BENCH_TERM_COLOR_CODE,
            BENCH_TERM_RED,
            BENCH_TERM_COLOR_CODE,
            BENCH_TERM_RESET,
            file,
            line,
            expr
        );
    } else {
        printf("(FAILED: %s (%d): %s)\n", file, line, expr);
    }

    return false;
}

void bench_run_bench(
    const char *name,
    bench_bench_fn bench_fn,
    bench_hook_fn setup_fn,
    bench_hook_fn teardown_fn,
    void *udata
)
{
    bench_num_benches++;

    benchmark_t b = { 0 };
    bench_run run = { .udata = udata, .iteration = 0, .is_warmup = 0 };

    for (int i = 0; i < bench_warmup; i++) {
        run.iteration = i;
        run.is_warmup = true;
        if (setup_fn) setup_fn(&run);
        bench_fn(&run);
        if (teardown_fn) teardown_fn(&run);
    }

    for (int i = 0; i < bench_iterations; i++) {
        run.iteration = i;
        run.is_warmup = false;

        if (setup_fn) setup_fn(&run);
        bench_start(&b);
        bench_fn(&run);
        bench_stop(&b);
        if (teardown_fn) teardown_fn(&run);
    }

    if (bench_colors) {
        printf("  %c%s%-74s%c%s\n", BENCH_TERM_COLOR_CODE, BENCH_TERM_BOLD, name, BENCH_TERM_COLOR_CODE, BENCH_TERM_RESET);
    } else {
        printf("  %-74s\n", name);
    }
    bench_report(&b);
    printf("\n");
}

void bench_run_suite(const char *name, bench_suite_fn suite_fn, void *suite_ctx)
{
    bench_num_suites++;
    if (bench_colors) {
        printf("────────────────────────────────────────────────────────────────────────────\n");
        printf(
            "%c%sRunning Suite: %s%c%s\n",
            BENCH_TERM_COLOR_CODE,
            BENCH_TERM_BOLD,
            name,
            BENCH_TERM_COLOR_CODE,
            BENCH_TERM_RESET
        );
    } else {
        printf("────────────────────────────────────────────────────────────────────────────\n");
        printf("Running Suite: %s\n", name);
    }

    suite_fn(suite_ctx);
}

void bench_print_stats()
{
    if (bench_colors) {
        printf("════════════════════════════════════════════════════════════════════════════\n");
        printf(
            "Summary: benches %-4d suites %-4d asserts %-6d failures "
            "%c%s%-4d%c%s\n",
            bench_num_benches,
            bench_num_suites,
            bench_num_asserts,
            BENCH_TERM_COLOR_CODE,
            bench_num_failures ? BENCH_TERM_RED : BENCH_TERM_GREEN,
            bench_num_failures,
            BENCH_TERM_COLOR_CODE,
            BENCH_TERM_RESET
        );
    } else {
        printf("════════════════════════════════════════════════════════════════════════════\n");
        printf(
            "Summary: benches %-4d suites %-4d asserts %-6d failures %d\n",
            bench_num_benches,
            bench_num_suites,
            bench_num_asserts,
            bench_num_failures
        );
    }
}

#endif // BRUTAL_BENCH_WRAPPER_IMPLEMENTATION
#endif // BRUTAL_BENCH_H
