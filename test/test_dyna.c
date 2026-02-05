#include "dyna.h"
#include "pico_unit.h"

TEST_CASE(test_dyna_push_pop_and_last)
{
    int *values = NULL;

    REQUIRE(alen(values) == 0);
    REQUIRE(acap(values) == 0);

    apush(values, 10);
    apush(values, 20);
    apush(values, 30);

    REQUIRE(alen(values) == 3);
    REQUIRE(acap(values) >= 3);
    REQUIRE(alast(values) == 30);
    REQUIRE(aend(values) == values + 3);

    REQUIRE(apop(values) == 30);
    REQUIRE(alen(values) == 2);
    REQUIRE(alast(values) == 20);

    afree(values);
    return true;
}

TEST_CASE(test_dyna_setcap_and_setlen)
{
    int *values = NULL;

    asetcap(values, 32);
    REQUIRE(values != NULL);
    REQUIRE(alen(values) == 0);
    REQUIRE(acap(values) >= 32);

    asetlen(values, 8);
    REQUIRE(alen(values) == 8);
    REQUIRE(acap(values) >= 8);

    for (int i = 0; i < alen(values); i++) values[i] = i * 3;

    REQUIRE(values[0] == 0);
    REQUIRE(values[7] == 21);

    asetlen(values, 3);
    REQUIRE(alen(values) == 3);
    REQUIRE(values[0] == 0);
    REQUIRE(values[1] == 3);
    REQUIRE(values[2] == 6);

    afree(values);
    return true;
}

TEST_CASE(test_dyna_delete_is_swap_remove)
{
    int *values = NULL;

    apush(values, 10);
    apush(values, 20);
    apush(values, 30);
    apush(values, 40);

    adel(values, 1);
    REQUIRE(alen(values) == 3);
    REQUIRE(values[0] == 10);
    REQUIRE(values[1] == 40);
    REQUIRE(values[2] == 30);

    afree(values);
    return true;
}

TEST_CASE(test_dyna_delete_sole_element)
{
    int *values = NULL;

    apush(values, 42);
    adel(values, 0);
    REQUIRE(alen(values) == 0);

    afree(values);
    return true;
}

TEST_CASE(test_dyna_clear_keeps_capacity)
{
    int *values = NULL;

    for (int i = 0; i < 20; i++) apush(values, i);
    REQUIRE(alen(values) == 20);

    int cap_before_clear = acap(values);
    aclear(values);

    REQUIRE(alen(values) == 0);
    REQUIRE(acap(values) == cap_before_clear);

    apush(values, 99);
    REQUIRE(alen(values) == 1);
    REQUIRE(values[0] == 99);

    afree(values);
    return true;
}

TEST_CASE(test_dyna_setcap_does_not_shrink)
{
    int *values = NULL;

    asetcap(values, 64);
    REQUIRE(acap(values) >= 64);
    int cap_before = acap(values);

    asetcap(values, 8);
    REQUIRE(acap(values) == cap_before);
    REQUIRE(alen(values) == 0);

    afree(values);
    return true;
}

TEST_CASE(test_dyna_afree_resets_and_allows_reuse)
{
    int *values = NULL;

    apush(values, 7);
    REQUIRE(values != NULL);

    afree(values);
    REQUIRE(values == NULL);
    REQUIRE(alen(values) == 0);
    REQUIRE(acap(values) == 0);

    apush(values, 42);
    REQUIRE(alen(values) == 1);
    REQUIRE(values[0] == 42);

    afree(values);
    return true;
}

TEST_CASE(test_dyna_push_compound_literal)
{
    typedef struct { int x, y; } point;
    point *pts = NULL;

    apush(pts, ((point){1, 2}));
    apush(pts, ((point){3, 4}));

    REQUIRE(alen(pts) == 2);
    REQUIRE(pts[0].x == 1);
    REQUIRE(pts[0].y == 2);
    REQUIRE(pts[1].x == 3);
    REQUIRE(pts[1].y == 4);

    afree(pts);
    return true;
}

TEST_SUITE(dyna_suite)
{
    RUN_TEST_CASE(test_dyna_push_pop_and_last);
    RUN_TEST_CASE(test_dyna_setcap_and_setlen);
    RUN_TEST_CASE(test_dyna_delete_is_swap_remove);
    RUN_TEST_CASE(test_dyna_delete_sole_element);
    RUN_TEST_CASE(test_dyna_clear_keeps_capacity);
    RUN_TEST_CASE(test_dyna_setcap_does_not_shrink);
    RUN_TEST_CASE(test_dyna_afree_resets_and_allows_reuse);
    RUN_TEST_CASE(test_dyna_push_compound_literal);
}
