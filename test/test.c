
#define PICO_UNIT_IMPLEMENTATION
#include "pico_unit.h"

#include <assert.h>
#include <stdio.h>

extern void brutal_ecs_suite(void);
extern void mpmc_tpool_suite(void);
extern void dyna_suite(void);

int main(void)
{
    pu_display_colors(true);

    RUN_TEST_SUITE(brutal_ecs_suite);
    RUN_TEST_SUITE(mpmc_tpool_suite);
    RUN_TEST_SUITE(dyna_suite);

    pu_print_stats();
    return pu_test_failed();
}
