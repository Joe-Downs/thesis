#include <stdio.h>
#include <string.h>

#include "compress.h"
#include "decompress.h"
#include "otel/tracing.h"

#define BUFSIZE 1024

int main() {
  char original[] =
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

  char compressed[BUFSIZE];
  char decompressed[BUFSIZE];

  TraceWriter tw = trace_open("roundtrip-benchmark");

  for (enum Algorithm method = ZSTD; method <= SZ3; method++) {
    printf("--- %s ---\n", algoNames[method]);

    if (method == SZ3) {
      printf("Unsupported method: %s\n", algoNames[method]);
      continue;
    }

    memset(compressed,   0, BUFSIZE);
    memset(decompressed, 0, BUFSIZE);

    struct timespec t0, t1;

    clock_gettime(CLOCK_REALTIME, &t0);
    size_t cSize = compress(method, compressed, BUFSIZE,
                            original, sizeof(original));
    clock_gettime(CLOCK_REALTIME, &t1);
    trace_span(&tw, "compress.run", algoNames[method],
               sizeof(original), cSize, t0, t1);

    if (cSize == 0) {
      printf("Compression failed\n");
      continue;
    }

    clock_gettime(CLOCK_REALTIME, &t0);
    size_t dSize = decompress(method, decompressed, BUFSIZE,
                              compressed, cSize);
    clock_gettime(CLOCK_REALTIME, &t1);
    trace_span(&tw, "decompress.run", algoNames[method],
               cSize, dSize, t0, t1);

    if (dSize == 0) {
      printf("Decompression failed\n");
      continue;
    }

    int ok = (dSize == sizeof(original)) &&
             (memcmp(original, decompressed, sizeof(original)) == 0);
    printf("Decompressed %zu bytes — %s\n", dSize, ok ? "OK" : "MISMATCH");
  }

  trace_close(&tw);
  return 0;
}
