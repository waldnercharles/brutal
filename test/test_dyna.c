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

TEST_CASE(test_dyna_afit_ensures_capacity)
{
    int *values = NULL;

    // afit on existing array should ensure capacity
    apush(values, 1);
    afit(values, 100);
    REQUIRE(values != NULL);
    REQUIRE(acap(values) >= 100);
    REQUIRE(alen(values) == 1);

    // afit should not reduce capacity
    int cap_before = acap(values);
    afit(values, 10);
    REQUIRE(acap(values) == cap_before);

    afree(values);
    return true;
}

TEST_CASE(test_dyna_acopy)
{
    int *src = NULL;
    int *dst = NULL;

    apush(src, 1);
    apush(src, 2);
    apush(src, 3);

    acopy(dst, src);
    REQUIRE(alen(dst) == 3);
    REQUIRE(dst[0] == 1);
    REQUIRE(dst[1] == 2);
    REQUIRE(dst[2] == 3);

    // Modifying dst should not affect src
    dst[0] = 99;
    REQUIRE(src[0] == 1);

    // Copy NULL into existing array clears it
    acopy(dst, NULL);
    REQUIRE(alen(dst) == 0);

    afree(src);
    afree(dst);
    return true;
}

TEST_CASE(test_dyna_arev)
{
    int *values = NULL;

    // Reverse empty array (should handle gracefully)
    arev(values);
    REQUIRE(values == NULL);

    // Reverse single element
    apush(values, 42);
    arev(values);
    REQUIRE(alen(values) == 1);
    REQUIRE(values[0] == 42);

    // Reverse odd count
    apush(values, 10);
    apush(values, 20);
    arev(values);
    REQUIRE(alen(values) == 3);
    REQUIRE(values[0] == 20);
    REQUIRE(values[1] == 10);
    REQUIRE(values[2] == 42);

    afree(values);

    // Reverse even count
    int *even = NULL;
    apush(even, 1);
    apush(even, 2);
    apush(even, 3);
    apush(even, 4);
    arev(even);
    REQUIRE(even[0] == 4);
    REQUIRE(even[1] == 3);
    REQUIRE(even[2] == 2);
    REQUIRE(even[3] == 1);

    afree(even);
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
    RUN_TEST_CASE(test_dyna_afit_ensures_capacity);
    RUN_TEST_CASE(test_dyna_acopy);
    RUN_TEST_CASE(test_dyna_arev);
}
