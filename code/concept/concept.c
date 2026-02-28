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

    double t0 = MPI_Wtime();
    size_t compressed_size = compress_buf(tmp_compressed, sizeof(tmp_compressed), src, srcSize);
    double t1 = MPI_Wtime();

    printf("Uncompressed: %ld\n", srcSize);
    printf("Compressed:   %ld\n", compressed_size);
    printf("[rank 0] Compression:   %.6f s\n", t1 - t0);

    // Send the size first so the receiver knows how many bytes to expect
    double t2 = MPI_Wtime();
    MPI_Send(&compressed_size, 1, MPI_UNSIGNED_LONG, 1, 0, MPI_COMM_WORLD);
    MPI_Send(tmp_compressed, (int)compressed_size, MPI_BYTE, 1, 1, MPI_COMM_WORLD);
    double t3 = MPI_Wtime();

    printf("[rank 0] Send:          %.6f s\n", t3 - t2);
  }

  if (rank == 1) {
    size_t compressed_size;
    MPI_Recv(&compressed_size, 1, MPI_UNSIGNED_LONG, 0, 0, MPI_COMM_WORLD, &status);

    char compressed[ZL_COMPRESSBOUND(BUF_LENGTH)];
    double t0 = MPI_Wtime();
    MPI_Recv(compressed, (int)compressed_size, MPI_BYTE, 0, 1, MPI_COMM_WORLD, &status);
    double t1 = MPI_Wtime();

    char decompressed[BUF_LENGTH];
    double t2 = MPI_Wtime();
    decompress_buf(decompressed, BUF_LENGTH, compressed, compressed_size);
    double t3 = MPI_Wtime();

    printf("[rank 1] Receive:       %.6f s\n", t1 - t0);
    printf("[rank 1] Decompression: %.6f s\n", t3 - t2);
    printf("Decompressed: %s\n", decompressed);
  }
  MPI_Finalize();
  return 0;
}
