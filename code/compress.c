#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <zstd.h>
#include <lz4.h>
#include <lzma.h>
#include <openzl/zl_compress.h>
#include <openzl/zl_compressor.h>
#include <openzl/zl_errors.h>
#include <openzl/codecs/zl_zstd.h>

#define DSTSIZE 1024

/* ---- tracing ---- */

typedef struct {
    FILE *fp;
    char  trace_id[33]; /* 32 hex chars + NUL */
} TraceWriter;

static void gen_hex(char *out, size_t n_bytes) {
    FILE *rng = fopen("/dev/urandom", "rb");
    for (size_t i = 0; i < n_bytes; i++) {
        unsigned char b = 0;
        fread(&b, 1, 1, rng);
        sprintf(out + i * 2, "%02x", b);
    }
    out[n_bytes * 2] = '\0';
    fclose(rng);
}

static void ts_to_iso8601(struct timespec ts, char *buf, size_t cap) {
    struct tm t;
    gmtime_r(&ts.tv_sec, &t);
    char base[24];
    strftime(base, sizeof(base), "%Y-%m-%dT%H:%M:%S", &t);
    snprintf(buf, cap, "%s.%09ldZ", base, ts.tv_nsec);
}

static TraceWriter trace_open(const char *experiment) {
    mkdir("traces", 0755);

    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    struct tm t;
    gmtime_r(&now.tv_sec, &t);
    char stamp[20];
    strftime(stamp, sizeof(stamp), "%Y%m%dT%H%M%SZ", &t);

    char path[256];
    snprintf(path, sizeof(path), "traces/%s_%s.jsonl", experiment, stamp);

    TraceWriter tw;
    gen_hex(tw.trace_id, 16);
    tw.fp = fopen(path, "w");
    fprintf(stderr, "tracing -> %s\n", path);
    return tw;
}

static void trace_span(TraceWriter *tw, const char *algorithm,
                       size_t input_size, size_t output_size,
                       struct timespec t0, struct timespec t1) {
    char span_id[17];
    gen_hex(span_id, 8);

    char start_s[40], end_s[40];
    ts_to_iso8601(t0, start_s, sizeof(start_s));
    ts_to_iso8601(t1, end_s,   sizeof(end_s));

    long duration_ns = (long)(t1.tv_sec - t0.tv_sec) * 1000000000L
                     + (t1.tv_nsec - t0.tv_nsec);

    fprintf(tw->fp,
        "{\"name\":\"compress.run\","
        "\"context\":{\"trace_id\":\"0x%s\",\"span_id\":\"0x%s\",\"trace_state\":\"[]\"},"
        "\"kind\":\"SpanKind.INTERNAL\","
        "\"parent_id\":null,"
        "\"start_time\":\"%s\","
        "\"end_time\":\"%s\","
        "\"status\":{\"status_code\":\"OK\"},"
        "\"attributes\":{"
            "\"algorithm\":\"%s\","
            "\"input_size_b\":%zu,"
            "\"output_size_b\":%zu,"
            "\"duration_ns\":%ld,"
            "\"experiment\":\"compress-benchmark\""
        "},"
        "\"events\":[],"
        "\"links\":[],"
        "\"resource\":{\"attributes\":{\"service.name\":\"compress\"},\"schema_url\":\"\"}}"
        "\n",
        tw->trace_id, span_id,
        start_s, end_s,
        algorithm, input_size, output_size, duration_ns);
}

static void trace_close(TraceWriter *tw) {
    if (tw->fp) fclose(tw->fp);
}

/* ---- compression ---- */

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

    trace_span(&tw, algoNames[method],
               sizeof(uncompressedText), outSize, t0, t1);
  }

  trace_close(&tw);
  return 0;
}
