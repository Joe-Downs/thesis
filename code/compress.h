#ifndef COMPRESS_H
#define COMPRESS_H

#include <stddef.h>

enum Algorithm { ZSTD, OPENZL, LZ4, LZMA, SZ3 };
extern const char *algoNames[];

size_t compress(enum Algorithm method, void *dst, size_t dstCapacity,
                const void *src, size_t srcSize);

#endif /* COMPRESS_H */
