#ifndef DECOMPRESS_H
#define DECOMPRESS_H

#include <stddef.h>
#include "compress.h"

size_t decompress(enum Algorithm method, void *dst, size_t dstCapacity,
                  const void *src, size_t srcSize);

#endif /* DECOMPRESS_H */
