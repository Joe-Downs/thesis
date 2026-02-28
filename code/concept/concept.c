#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "custom_parsers/shared_components/clustering.h" // ZS2_createGraph_genericClustering
#include "openzl/codecs/zl_sddl.h"   // ZL_Compressor_buildSDDLGraph
#include "openzl/common/assertion.h" // ZL_REQUIRE, ZL_REQUIRE_NN
#include "openzl/zl_compress.h"      // ZL_compressBound, ZL_COMPRESSBOUND
#include "openzl/zl_compressor.h"    // ZL_Compressor_*, ZL_CCtx_*
#include "openzl/zl_decompress.h"
#include "openzl/zl_errors.h" // ZL_isError, ZL_validResult
#include "openzl/zl_public_nodes.h"
#include "openzl/zl_version.h" // ZL_MAX_FORMAT_VERSION

#include "hdf5_sddl_bytecode.h"

#define BUF_LENGTH 512
#define C_NODE 1
#define U_NODE 2

static size_t compress_buf(void *dst, size_t dstCapacity, const void *src,
                           size_t srcSize) {
  ZL_CCtx *cctx = ZL_CCtx_create();
  ZL_REQUIRE_NN(cctx);
  ZL_Compressor *cgraph = ZL_Compressor_create();
  ZL_REQUIRE_NN(cgraph);

  ZL_REQUIRE(!ZL_isError(ZL_Compressor_setParameter(cgraph, ZL_CParam_formatVersion, ZL_MAX_FORMAT_VERSION)));

  ZL_GraphID clustering = ZS2_createGraph_genericClustering(cgraph);
  ZL_Result_ZL_GraphID sddl_result = ZL_Compressor_buildSDDLGraph(cgraph, hdf5_sddl_bytecode, hdf5_sddl_bytecode_size, clustering);
  ZL_REQUIRE(sddl_result._value._code == ZL_ErrorCode_no_error);
  ZL_REQUIRE(!ZL_isError(ZL_Compressor_selectStartingGraphID(cgraph, sddl_result._value._value)));
  ZL_REQUIRE(!ZL_isError(ZL_CCtx_refCompressor(cctx, cgraph)));

  ZL_Report report = ZL_CCtx_compress(cctx, dst, dstCapacity, src, srcSize);
  if (ZL_isError(report)) {
    fprintf(stderr, "ZL_CCtx_compress error: %s\n", ZL_ErrorCode_toString(ZL_errorCode(report)));
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
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
  if (rank == 0) {
    FILE *fileptr = fopen("data-1996-04-16-03-1.h5", "rb");
    if (!fileptr) { perror("fopen"); MPI_Abort(MPI_COMM_WORLD, 1); }
    fseek(fileptr, 0, SEEK_END);
    size_t srcSize = (size_t)ftell(fileptr);
    rewind(fileptr);

    char *src = (char *)malloc(srcSize);
    fread(src, 1, srcSize, fileptr);
    fclose(fileptr);

    size_t compressBound = ZL_compressBound(srcSize);
    char *tmp_compressed = (char *)malloc(compressBound);

    double t0 = MPI_Wtime();
    size_t compressed_size = compress_buf(tmp_compressed, compressBound, src, srcSize);
    double t1 = MPI_Wtime();

    printf("Uncompressed: %ld\n", srcSize);
    printf("Compressed:   %ld\n", compressed_size);
    printf("[rank 0] Compression:   %.6f s\n", t1 - t0);

    // Send the size first so the receiver knows how many bytes to expect
    double start = MPI_Wtime();
    MPI_Send(&compressed_size, 1, MPI_UNSIGNED_LONG, C_NODE, 0, MPI_COMM_WORLD);
    MPI_Send(tmp_compressed, (int)compressed_size, MPI_BYTE, C_NODE, 1, MPI_COMM_WORLD);
    printf("[rank 0] Send (C):      %.6f s\n", MPI_Wtime() - start);

    start = MPI_Wtime();
    MPI_Send(&srcSize, 1, MPI_UNSIGNED_LONG, U_NODE, 0, MPI_COMM_WORLD);
    MPI_Send(src, (int)srcSize, MPI_BYTE, U_NODE, 1, MPI_COMM_WORLD);
    printf("[rank 0] Send (U):      %.6f s\n", MPI_Wtime() - start);

    free(src);
    free(tmp_compressed);
  }

  if (rank == C_NODE) {
    // Receiving and decompressing
    size_t compressed_size;
    MPI_Recv(&compressed_size, 1, MPI_UNSIGNED_LONG, 0, 0, MPI_COMM_WORLD, &status);

    char *compressed = (char *)malloc(compressed_size);
    double start = MPI_Wtime();
    MPI_Recv(compressed, (int)compressed_size, MPI_BYTE, 0, 1, MPI_COMM_WORLD, &status);
    printf("[rank 1] Receive:       %.6f s\n", MPI_Wtime() - start);

    ZL_Report dr = ZL_getDecompressedSize(compressed, compressed_size);
    size_t decompressed_size = ZL_validResult(dr);
    char *decompressed_buf = (char *)malloc(decompressed_size);

    start = MPI_Wtime();
    decompress_buf(decompressed_buf, decompressed_size, compressed, compressed_size);
    printf("[rank 1] Decompression: %.6f s\n", MPI_Wtime() - start);

    free(compressed);
    free(decompressed_buf);
  }

  if (rank == U_NODE) {
    // Receiving uncompressed
    size_t recv_size;
    MPI_Recv(&recv_size, 1, MPI_UNSIGNED_LONG, 0, 0, MPI_COMM_WORLD, &status);
    char *data = (char *)malloc(recv_size);
    double recv_start = MPI_Wtime();
    MPI_Recv(data, (int)recv_size, MPI_BYTE, 0, 1, MPI_COMM_WORLD, &status);
    printf("[rank 2] Receive:       %.6f s\n", MPI_Wtime() - recv_start);
    printf("[rank 2] Decompress:    %.6f s\n", 0.0);
    free(data);
  }
  MPI_Finalize();
  return 0;
}
