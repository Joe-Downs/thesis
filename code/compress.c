#include <stdio.h>
#include <string.h>
#include <zstd.h>

#define DSTSIZE 256

enum Algorithm { ZSTD, OPENZL, LZ4, LZMA, SZ3 };

const char *algoNames[] = {"zstd", "OpenZL", "LZ4", "LZMA", "SZ3"};

size_t zstd_compress(void *dst, size_t dstCapacity, const void *src,
                   size_t srcSize) {
  return ZSTD_compress(dst, dstCapacity, src, srcSize, 15);
}

void openzl_compress() {}

void lz4_compress() {}

void lzma_compress() {}

void sz3_compress() {}


void compress(enum Algorithm method, void *dst, size_t dstCapacity,
              const void *src, size_t srcSize) {
  size_t compressedSize = 0;
  switch (method) {
  case ZSTD:
    compressedSize = zstd_compress(dst, dstCapacity, src, srcSize);
    break;
  default:
    printf("Unsupported compression method: %s\n", algoNames[method]);
  }

  printf("Compressed %zu to %zu bytes\n", srcSize, compressedSize);
}

int main() {
  enum Algorithm compressMethod = ZSTD;
  char uncompressedText[] =
      "Vestibulum convallis, lorem a tempus semper, dui dui euismod elit,"
      "vitae placerat urna tortor vitae lacus.";
  char compressedText[DSTSIZE];
  compress(compressMethod, compressedText, DSTSIZE, uncompressedText, 106);
  printf("%s\n", compressedText);
  return 0;
}
