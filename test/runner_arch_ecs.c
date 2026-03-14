#define PICO_UNIT_IMPLEMENTATION
#include "pico_unit.h"

extern void arch_ecs_suite();

int main()
{
    pu_display_colors(true);
    RUN_TEST_SUITE(arch_ecs_suite);
    pu_print_stats();
    return pu_test_failed();
}
