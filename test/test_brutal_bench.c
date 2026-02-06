#define BRUTAL_BENCH_IMPLEMENTATION
#include "brutal_bench.h"

#include <stdio.h>
#include <unistd.h>

static int counter = 0;

BENCH_SETUP(setup_counter)
{
    (void)bb_run_ctx;
    counter = 0;
}

BENCH_CASE(bench_simple_increment)
{
    (void)bb_run_ctx;
    counter++;
}

BENCH_CASE(bench_with_sleep)
{
    (void)bb_run_ctx;
    usleep(100); // 100 microseconds
    counter++;
}

BENCH_CASE(bench_with_validation)
{
    (void)bb_run_ctx;
    counter++;
    BENCH_REQUIRE(counter > 0);
}

BENCH_SUITE(basic_suite)
{
    (void)bb_suite_ctx;
    RUN_BENCH_CASE(bench_simple_increment, setup_counter, NULL, NULL);
    RUN_BENCH_CASE(bench_with_sleep, setup_counter, NULL, NULL);
    RUN_BENCH_CASE(bench_with_validation, setup_counter, NULL, NULL);
}

int main(void)
{
    bb_set_iterations(100);
    bb_set_warmup(10);
    bb_display_colors(true);
    bb_display_cpu_time(true);

    RUN_BENCH_SUITE(basic_suite, NULL);

    bb_print_stats();
    return bb_failed() ? 1 : 0;
}
