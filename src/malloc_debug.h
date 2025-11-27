// malloc_debug.h
#pragma once
#include <stdlib.h>
#include <stdio.h>

#ifdef DEBUG_MALLOC_FAIL

static int malloc_call_count = 0;
static int fail_every_n = 5; // fail every 5th call (adjust as needed)

void* dbg_malloc(size_t size, const char* file, int line) {
    malloc_call_count++;
    if (fail_every_n > 0 && malloc_call_count % fail_every_n == 0) {
        fprintf(stderr, "[malloc fail] Simulated failure at %s:%d\n", file, line);
        return NULL;
    }
    return malloc(size);
}
void* dbg_realloc(void* ptr, size_t size, const char* file, int line) {
    malloc_call_count++;
    if (fail_every_n > 0 && malloc_call_count % fail_every_n == 0) {
        fprintf(stderr, "[realloc fail] Simulated failure at %s:%d\n", file, line);
        return NULL;
    }
    return realloc(ptr, size);
}

// macros override normal calls
#define malloc(size) dbg_malloc(size, __FILE__, __LINE__)
#define realloc(ptr, size) dbg_realloc(ptr, size, __FILE__, __LINE__)

#endif
