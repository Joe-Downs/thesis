#include <stdio.h>
#include <string.h>
#include <zstd.h>
#include <lz4.h>
#include <lzma.h>
#include <openzl/zl_compress.h>
#include <openzl/zl_compressor.h>
#include <openzl/zl_errors.h>
#include <openzl/codecs/zl_zstd.h>

#include "otel/tracing.h"

#define DSTSIZE 1024

enum Algorithm { ZSTD, OPENZL, LZ4, LZMA, SZ3 };

const char *algoNames[] = {"zstd", "OpenZL", "LZ4", "LZMA", "SZ3"};

size_t zstd_compress(void *dst, size_t dstCapacity, const void *src,
                     size_t srcSize) {
  size_t result = ZSTD_compress(dst, dstCapacity, src, srcSize, 15);
  return ZSTD_isError(result) ? 0 : result;
}

size_t openzl_compress(void *dst, size_t dstCapacity, const void *src,
                       size_t srcSize) {
  ZL_CCtx *cctx = ZL_CCtx_create();
  ZL_Compressor *cgraph = ZL_Compressor_create();
  ZL_Report setupReport = ZL_Compressor_setParameter(
      cgraph, ZL_CParam_formatVersion, ZL_getDefaultEncodingVersion());
  if (!ZL_isError(setupReport)) {
    setupReport =
        ZL_Compressor_selectStartingGraphID(cgraph, ZL_GRAPH_ZSTD);
  }
  if (!ZL_isError(setupReport)) {
    setupReport = ZL_CCtx_refCompressor(cctx, cgraph);
  }

  size_t compressedSize = 0;
  if (ZL_isError(setupReport)) {
    printf("OpenZL setup failed: %s\n",
           ZL_ErrorCode_toString(ZL_errorCode(setupReport)));
  } else {
    ZL_Report report = ZL_CCtx_compress(cctx, dst, dstCapacity, src, srcSize);
    if (ZL_isError(report)) {
      printf("OpenZL compression failed: %s\n",
             ZL_ErrorCode_toString(ZL_errorCode(report)));
    } else {
      compressedSize = ZL_validResult(report);
    }
  }

  ZL_Compressor_free(cgraph);
  ZL_CCtx_free(cctx);
  return compressedSize;
}

size_t lz4_compress(void *dst, size_t dstCapacity, const void *src,
                    size_t srcSize) {
  int compressedSize = LZ4_compress_default(src, dst, (int)srcSize,
                                            (int)dstCapacity);
  return compressedSize > 0 ? (size_t)compressedSize : 0;
}

size_t lzma_compress(void *dst, size_t dstCapacity, const void *src,
                     size_t srcSize) {
  size_t outPos = 0;
  lzma_ret ret = lzma_easy_buffer_encode(
      LZMA_PRESET_DEFAULT, LZMA_CHECK_CRC64, NULL, src, srcSize, dst,
      &outPos, dstCapacity);
  return ret == LZMA_OK ? outPos : 0;
}

size_t compress(enum Algorithm method, void *dst, size_t dstCapacity,
                const void *src, size_t srcSize) {
  size_t compressedSize = 0;
  switch (method) {
  case ZSTD:
    compressedSize = zstd_compress(dst, dstCapacity, src, srcSize);
    break;
  case OPENZL:
    compressedSize = openzl_compress(dst, dstCapacity, src, srcSize);
    break;
  case LZ4:
    compressedSize = lz4_compress(dst, dstCapacity, src, srcSize);
    break;
  case LZMA:
    compressedSize = lzma_compress(dst, dstCapacity, src, srcSize);
    break;
  default:
    printf("Unsupported compression method: %s\n", algoNames[method]);
  }

  printf("Compressed %zu to %zu bytes\n", srcSize, compressedSize);
  return compressedSize;
}

int main() {
  char uncompressedText[] =
      "Vestibulum convallis, lorem a tempus semper, dui dui euismod elit,"
      "vitae placerat urna tortor vitae lacus. Nunc rutrum turpis sed pede.  "
      "Mauris ac felis vel velit tristique imperdiet.  Donec pretium posuere "
      "tellus.  Cum sociis natoque penatibus et magnis dis parturient montes, "
      "nascetur ridiculus mus.  Vestibulum convallis, lorem a tempus semper, "
      "dui dui euismod elit, vitae placerat urna tortor vitae lacus.  "
      "Phasellus purus.  Pellentesque dapibus suscipit ligula.  Praesent "
      "augue.  Donec pretium posuere tellus.  Vestibulum convallis, lorem a "
      "tempus semper, dui dui euismod elit, vitae placerat urna tortor vitae "
      "lacus.";
  char compressedText[DSTSIZE];

  TraceWriter tw = trace_open("compress-benchmark");

  for (enum Algorithm method = ZSTD; method <= SZ3; method++) {
    printf("--- %s ---\n", algoNames[method]);
    memset(compressedText, 0, DSTSIZE);

    struct timespec t0, t1;
    clock_gettime(CLOCK_REALTIME, &t0);
    size_t outSize = compress(method, compressedText, DSTSIZE,
                              uncompressedText, sizeof(uncompressedText));
    clock_gettime(CLOCK_REALTIME, &t1);

    trace_span(&tw, "compress.run", algoNames[method],
               sizeof(uncompressedText), outSize, t0, t1);
  }

  trace_close(&tw);
  return 0;
}
