#include <mpi.h>
#include <stdio.h>
#include <string.h>

#include "openzl/common/assertion.h" // ZL_REQUIRE, ZL_REQUIRE_NN
#include "openzl/zl_compress.h"      // ZL_compressBound, ZL_COMPRESSBOUND
#include "openzl/zl_compressor.h"    // ZL_Compressor_*, ZL_CCtx_*
#include "openzl/zl_decompress.h"
#include "openzl/zl_errors.h" // ZL_isError, ZL_validResult
#include "openzl/zl_public_nodes.h"

#define FORMAT_VERSION (16)
#define BUF_LENGTH 512

static size_t compress_buf(void *dst, size_t dstCapacity, const void *src,
                           size_t srcSize) {
  ZL_CCtx *cctx = ZL_CCtx_create();
  ZL_REQUIRE_NN(cctx);
  ZL_Compressor *cgraph = ZL_Compressor_create();
  ZL_REQUIRE_NN(cgraph);

  ZL_REQUIRE(!ZL_isError(ZL_Compressor_setParameter(
      cgraph, ZL_CParam_formatVersion, FORMAT_VERSION)));
  ZL_REQUIRE(
      !ZL_isError(ZL_Compressor_selectStartingGraphID(cgraph, ZL_GRAPH_ZSTD)));
  ZL_REQUIRE(!ZL_isError(ZL_CCtx_refCompressor(cctx, cgraph)));

  ZL_Report report = ZL_CCtx_compress(cctx, dst, dstCapacity, src, srcSize);
  ZL_REQUIRE(!ZL_isError(report));
  size_t compressedSize = ZL_validResult(report);

  ZL_Compressor_free(cgraph);
  ZL_CCtx_free(cctx);
  return compressedSize;
}

static void decompress_buf(void *uncompressed, size_t decompressed_size,
                           void *compressed, size_t compressed_size) {
  ZL_Report report = ZL_decompress(uncompressed, decompressed_size, compressed,
                                   compressed_size);
}

int main(int argc, char **argv) {
  MPI_Init(&argc, &argv);

  int rank, size;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  MPI_Status status;

  char decompressed[BUF_LENGTH];
  char tmp_compressed[ZL_COMPRESSBOUND(BUF_LENGTH)];
  if (rank == 0) {
    char src[BUF_LENGTH] = "Vivamus id enim. Nulla posuere. Pellentesque tristique imperdiet tortor. Sed diam. Phasellus neque orci, porta a, aliquet quis, semper a, massa. Nullam tempus. Nulla facilisis, risus a rhoncus fermentum, tellus tellus lacinia purus, et dictum nunc justo sit amet elit. Donec neque quam, dignissim in, mollis nec, sagittis eu, wisi. Donec neque quam, dignissim in, mollis nec, sagittis eu, wisi. Aliquam posuere.";
    size_t srcSize = strlen(src);
    size_t compressed_size = compress_buf(tmp_compressed, sizeof(tmp_compressed), src, srcSize);

    char *compressed = malloc(compressed_size);
    compressed = tmp_compressed;

    printf("Uncompressed: %ld\n", srcSize);
    printf("Compressed:   %ld\n", compressed_size);

    decompress_buf(decompressed, BUF_LENGTH, compressed, compressed_size);
    printf("Decompressed: %s\n", decompressed);
    //MPI_Send(compressed, compressed_size, MPI_CHAR, 1, 1, MPI_COMM_WORLD);
  }
  /* if (rank == 1) { */
  /*   char compressed[BUF_LENGTH]; */
  /*   char decompressed[BUF_LENGTH]; */
  /*   MPI_Recv(compressed, BUF_LENGTH, MPI_CHAR, 1, 1, MPI_COMM_WORLD, &status); */
  /*   decompress_buf(decompressed, BUF_LENGTH, ); */
  /*   printf("Uncompressed: %s\n", dst); */
  /* } */
  MPI_Finalize();
  return 0;
}
