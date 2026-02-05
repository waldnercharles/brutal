/**
 * dyna.h - Tiny stretchy-buffer macros for C arrays.
 *
 * Heavily inspired by: https://github.com/RandyGaul/ckit.h/blob/main/ckit.h
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

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

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

// clang-format off
typedef struct
{
    int len, cap;
    union { uint32_t cookie; char dbg[4]; };
} dyna_hdr;

#define DYNA_HDR(a) ((dyna_hdr *)(a) - 1)
#define DYNA_COOKIE 'DYNA'

#ifdef NDEBUG
    #define DYNA_ASSERT(expr) ((void)0)
    #define DYNA_CANARY(a) ((void)0)
#else
    #ifndef DYNA_ASSERT
        #include <assert.h>
        #define DYNA_ASSERT(expr) (assert(expr))
    #endif

    #define DYNA_CANARY(a)((a) ? DYNA_ASSERT(DYNA_HDR(a)->cookie == DYNA_COOKIE) : (void)0)
#endif

#define dyna_grow(a, n, min_cap) ((a) = dyna_grow_n((a), sizeof *(a), (n), (min_cap)))

// Number of elements. Returns 0 for NULL.
#define alen(a)       ((a) ? DYNA_HDR(a)->len : 0)
#define asize(a)      alen(a)
#define acount(a)     alen(a)

// Allocated capacity. Returns 0 for NULL
#define acap(a)       ((a) ? DYNA_HDR(a)->cap : 0)

// Ensure capacity for n elements. May reallocate.
#define asetcap(a, n) (dyna_grow(a, 0, n))

// Set number of elements directly. Will reallocate if a is NULL or n is greater than current capacity.
#define asetlen(a, n) ((acap(a) < (n) ? asetcap((a), (n)) : 0), (a) ? DYNA_HDR(a)->len = (n) : 0)

// Ensure capacity for n elements. May reallocate.
#define afit(a, n)    ((!(a) || (n) > DYNA_HDR(a)->cap) ? (dyna_grow(a, (n) - alen(a), 0), 0) : 0)

// Append element. May reallocate.
#define apush(a, ...) (DYNA_CANARY(a), afit(a, alen(a) + 1), (a)[DYNA_HDR(a)->len++] = (__VA_ARGS__))

// Remove and return last element. Array must not be empty.
#define apop(a)       ((a)[--DYNA_HDR(a)->len])

// Pointer one past the last element.
#define aend(a)       ((a) + alen(a))

// Last element. Array must not be empty.
#define alast(a)      (a[DYNA_HDR(a)->len - 1])

// Set length to 0, but keep allocated memory.
#define aclear(a)     (DYNA_CANARY(a), (a) ? DYNA_HDR(a)->len = 0 : 0)

// Remove element at index i by swapping with last element. (O(1), unordered).
#define adel(a, i)    (a[i] = a[--DYNA_HDR(a)->len])

// Copy array b into a.
#define acopy(a, b)   (a) = dyna_copy((a), (b), sizeof(*(a)))

// Reverse array in-place.
#define arev(a)       ((a) ? dyna_rev(a, sizeof(*(a))) : (void *)0)

// Free array memory and set pointer to NULL
#define afree(a)      do { DYNA_CANARY(a); if (a) DYNA_FREE(DYNA_HDR(a)); (a) = NULL; } while (0)
// clang-format on

static inline void *dyna_grow_n(void *a, int elem_sz, int add_len, int min_cap)
{
    DYNA_CANARY(a);
    int min_len = alen(a) + add_len;
    int cap = acap(a);

    if (min_len > min_cap) min_cap = min_len;
    if (min_cap <= cap) return a;

    if (min_cap < 2 * cap) min_cap = 2 * cap;
    else if (min_cap < 16) min_cap = 16;

    dyna_hdr *h = DYNA_REALLOC((a) ? DYNA_HDR(a) : NULL, elem_sz * min_cap + sizeof(dyna_hdr));
    DYNA_ASSERT(h && "dyna: allocation failed");

    if (!a) {
        h->len = 0;
        h->cookie = DYNA_COOKIE;
    }
    h->cap = min_cap;
    return (void *)(h + 1);
}

static inline void *dyna_copy(void *a, void *b, int elem_sz)
{
    DYNA_CANARY(a);
    if (!b) {
        aclear(a);
        return (void *)a;
    }
    DYNA_CANARY(b);
    a = dyna_grow_n(a, elem_sz, 0, DYNA_HDR(b)->len);
    memcpy(a, b, DYNA_HDR(b)->len * elem_sz);

    DYNA_HDR(a)->len = DYNA_HDR(b)->len;
    return (void *)a;
}

void *dyna_rev(const void *a_ptr, int elem_sz)
{
    DYNA_CANARY(a_ptr);
    uint8_t *a = (uint8_t *)a_ptr;
    int n = acount(a_ptr);
    if (n <= 1) return (void *)a_ptr;
    uint8_t *b = a + elem_sz * (n - 1);
    uint8_t tmp[elem_sz];
    while (a < b) {
        memcpy(tmp, a, elem_sz);
        memcpy(a, b, elem_sz);
        memcpy(b, tmp, elem_sz);
        a += elem_sz;
        b -= elem_sz;
    }
    return (void *)a_ptr;
}
#endif
