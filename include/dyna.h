/**
 * dyna.h - Tiny stretchy-buffer macros for C arrays.
 *
 * USAGE EXAMPLE:
 *   int *values = NULL;
 *   apush(values, 10);
 *   apush(values, 20);
 *
 *   int last = apop(values); // last == 20
 *   (void)last;
 *
 *   afree(values);
 */

#ifndef DYNA_H
#define DYNA_H

#include <stdlib.h>

#define DYNA_REALLOC(ptr, sz) realloc(ptr, sz)
#define DYNA_FREE(ptr) free(ptr)

#ifdef NDEBUG
#define DYNA_ASSERT(expr) ((void)0)
#else
#ifndef DYNA_ASSERT
#include <assert.h>
#define DYNA_ASSERT(expr) (assert(expr))
#endif
#endif

typedef struct
{
    int len, cap;
} dyna_hdr;

#define DYNA_HDR(a) ((dyna_hdr *)(a) - 1)

// clang-format off
#define dyna_grow(a, n, min_cap) ((a) = dyna_growf((a), sizeof *(a), (n), (min_cap)))
#define dyna_maybegrow(a, n)     ((!(a) || DYNA_HDR(a)->len + (n) > DYNA_HDR(a)->cap) ? (dyna_grow(a, n, 0), 0) : 0)
#define alen(a)       ((a) ? DYNA_HDR(a)->len : 0)
#define asize(a)      alen(a)
#define acount(a)     alen(a)
#define acap(a)       ((a) ? DYNA_HDR(a)->cap : 0)
#define asetcap(a, n) (dyna_grow(a, 0, n))
#define asetlen(a, n) ((acap(a) < (n) ? asetcap((a), (n)) : 0), (a) ? DYNA_HDR(a)->len = (n) : 0)
#define afit(a, n)    asetlen(a, n)
#define apush(a, ...) (dyna_maybegrow(a, 1), (a)[DYNA_HDR(a)->len++] = (__VA_ARGS__))
#define apop(a)       (DYNA_HDR(a)->len--, (a)[DYNA_HDR(a)->len])
#define aend(a)       (a + DYNA_HDR(a)->len)
#define alast(a)      (a[DYNA_HDR(a)->len - 1])
#define aclear(a)     ((a) ? DYNA_HDR(a)->len = 0 : 0)
#define adel(a, i)    (a[i] = a[--DYNA_HDR(a)->len])
#define afree(a)      do { DYNA_FREE(DYNA_HDR(a)); (a) = NULL; } while (0)
// clang-format on

static inline void *dyna_growf(void *a, int elem_sz, int add_len, int min_cap)
{
    int min_len = alen(a) + add_len;
    int cap = acap(a);

    if (min_len > min_cap) min_cap = min_len;
    if (min_cap <= cap) return a;

    if (min_cap < 2 * cap) min_cap = 2 * cap;
    else if (min_cap < 16) min_cap = 16;

    dyna_hdr *h = DYNA_REALLOC((a) ? DYNA_HDR(a) : NULL, elem_sz * min_cap + sizeof(dyna_hdr));
    DYNA_ASSERT(h && "dyna: allocation failed");

    if (!a) h->len = 0;
    h->cap = min_cap;
    return (void *)(h + 1);
}
#endif
