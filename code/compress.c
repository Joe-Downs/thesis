#include "compress.h"

#include <stdio.h>
#include <zstd.h>
#include <lz4.h>
#include <lzma.h>
#include <openzl/zl_compress.h>
#include <openzl/zl_compressor.h>
#include <openzl/zl_errors.h>
#include <openzl/codecs/zl_zstd.h>

const char *algoNames[] = {"zstd", "OpenZL", "LZ4", "LZMA", "SZ3"};

static size_t zstd_compress(void *dst, size_t dstCapacity, const void *src,
                             size_t srcSize) {
  size_t result = ZSTD_compress(dst, dstCapacity, src, srcSize, 15);
  return ZSTD_isError(result) ? 0 : result;
}

static size_t openzl_compress(void *dst, size_t dstCapacity, const void *src,
                               size_t srcSize) {
  ZL_CCtx *cctx = ZL_CCtx_create();
  ZL_Compressor *cgraph = ZL_Compressor_create();
  ZL_Report rep = ZL_Compressor_setParameter(
      cgraph, ZL_CParam_formatVersion, ZL_getDefaultEncodingVersion());
  if (!ZL_isError(rep))
    rep = ZL_Compressor_selectStartingGraphID(cgraph, ZL_GRAPH_ZSTD);
  if (!ZL_isError(rep))
    rep = ZL_CCtx_refCompressor(cctx, cgraph);

  size_t compressedSize = 0;
  if (ZL_isError(rep)) {
    printf("OpenZL setup failed: %s\n",
           ZL_ErrorCode_toString(ZL_errorCode(rep)));
  } else {
    ZL_Report r = ZL_CCtx_compress(cctx, dst, dstCapacity, src, srcSize);
    if (ZL_isError(r))
      printf("OpenZL compression failed: %s\n",
             ZL_ErrorCode_toString(ZL_errorCode(r)));
    else
      compressedSize = ZL_validResult(r);
  }

  ZL_Compressor_free(cgraph);
  ZL_CCtx_free(cctx);
  return compressedSize;
}

static size_t lz4_compress(void *dst, size_t dstCapacity, const void *src,
                            size_t srcSize) {
  int r = LZ4_compress_default(src, dst, (int)srcSize, (int)dstCapacity);
  return r > 0 ? (size_t)r : 0;
}

static size_t lzma_compress(void *dst, size_t dstCapacity, const void *src,
                             size_t srcSize) {
  size_t outPos = 0;
  lzma_ret ret = lzma_easy_buffer_encode(
      LZMA_PRESET_DEFAULT, LZMA_CHECK_CRC64, NULL, src, srcSize,
      dst, &outPos, dstCapacity);
  return ret == LZMA_OK ? outPos : 0;
}

size_t compress(enum Algorithm method, void *dst, size_t dstCapacity,
                const void *src, size_t srcSize) {
  size_t compressedSize = 0;
  switch (method) {
  case ZSTD:   compressedSize = zstd_compress(dst, dstCapacity, src, srcSize);   break;
  case OPENZL: compressedSize = openzl_compress(dst, dstCapacity, src, srcSize); break;
  case LZ4:    compressedSize = lz4_compress(dst, dstCapacity, src, srcSize);    break;
  case LZMA:   compressedSize = lzma_compress(dst, dstCapacity, src, srcSize);   break;
  default:
    printf("Unsupported compression method: %s\n", algoNames[method]);
  }
  printf("Compressed %zu to %zu bytes\n", srcSize, compressedSize);
  return compressedSize;
}
