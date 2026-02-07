
#define PICO_UNIT_IMPLEMENTATION
#include "pico_unit.h"

#include <assert.h>
#include <stdio.h>

extern void ecs_suite();
extern void tpool_suite();
extern void dyna_suite();

int main()
{
    pu_display_colors(true);

    RUN_TEST_SUITE(ecs_suite);
    RUN_TEST_SUITE(tpool_suite);
    RUN_TEST_SUITE(dyna_suite);

    pu_print_stats();
    return pu_test_failed();
}
