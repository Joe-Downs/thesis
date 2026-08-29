#include "decompress.h"

#include <stdio.h>
#include <zstd.h>
#include <lz4.h>
#include <lzma.h>
#include <openzl/zl_decompress.h>
#include <openzl/zl_errors.h>

static size_t zstd_decompress(void *dst, size_t dstCapacity, const void *src,
                               size_t srcSize) {
  size_t result = ZSTD_decompress(dst, dstCapacity, src, srcSize);
  return ZSTD_isError(result) ? 0 : result;
}

static size_t openzl_decompress(void *dst, size_t dstCapacity, const void *src,
                                 size_t srcSize) {
  ZL_DCtx *dctx = ZL_DCtx_create();
  ZL_Report report = ZL_DCtx_decompress(dctx, dst, dstCapacity, src, srcSize);
  ZL_DCtx_free(dctx);

  if (ZL_isError(report)) {
    printf("OpenZL decompression failed: %s\n",
           ZL_ErrorCode_toString(ZL_errorCode(report)));
    return 0;
  }
  return ZL_validResult(report);
}

static size_t lz4_decompress(void *dst, size_t dstCapacity, const void *src,
                              size_t srcSize) {
  int result = LZ4_decompress_safe(src, dst, (int)srcSize, (int)dstCapacity);
  return result > 0 ? (size_t)result : 0;
}

static size_t lzma_decompress(void *dst, size_t dstCapacity, const void *src,
                               size_t srcSize) {
  uint64_t memlimit = UINT64_MAX;
  size_t inPos = 0, outPos = 0;
  lzma_ret ret = lzma_stream_buffer_decode(&memlimit, 0, NULL, src, &inPos,
                                           srcSize, dst, &outPos, dstCapacity);
  return ret == LZMA_OK ? outPos : 0;
}

size_t decompress(enum Algorithm method, void *dst, size_t dstCapacity,
                  const void *src, size_t srcSize) {
  switch (method) {
  case ZSTD:   return zstd_decompress(dst, dstCapacity, src, srcSize);
  case OPENZL: return openzl_decompress(dst, dstCapacity, src, srcSize);
  case LZ4:    return lz4_decompress(dst, dstCapacity, src, srcSize);
  case LZMA:   return lzma_decompress(dst, dstCapacity, src, srcSize);
  default:
    printf("Unsupported decompression method: %s\n", algoNames[method]);
    return 0;
  }
}
