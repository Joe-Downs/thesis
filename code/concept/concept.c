#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

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

/* Resolve the local machine's IP address into out (dotted-decimal string). */
static void get_local_ip(char *out, size_t outlen) {
  char hostname[256];
  gethostname(hostname, sizeof(hostname));
  struct addrinfo hints = {0}, *res;
  hints.ai_family   = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  if (getaddrinfo(hostname, NULL, &hints, &res) == 0) {
    struct sockaddr_in *sa = (struct sockaddr_in *)res->ai_addr;
    inet_ntop(AF_INET, &sa->sin_addr, out, (socklen_t)outlen);
    freeaddrinfo(res);
  } else {
    strncpy(out, "127.0.0.1", outlen);
  }
}

/* Connect to host:port and send all len bytes of data. */
static void tcp_connect_send(const char *host, int port, const void *data, size_t len) {
  struct addrinfo hints = {0}, *res;
  hints.ai_family   = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  char port_str[16];
  snprintf(port_str, sizeof(port_str), "%d", port);
  if (getaddrinfo(host, port_str, &hints, &res) != 0) {
    perror("getaddrinfo"); MPI_Abort(MPI_COMM_WORLD, 1);
  }
  int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
  if (fd < 0) { perror("socket"); MPI_Abort(MPI_COMM_WORLD, 1); }
  if (connect(fd, res->ai_addr, res->ai_addrlen) != 0) {
    perror("connect"); MPI_Abort(MPI_COMM_WORLD, 1);
  }
  freeaddrinfo(res);
  const char *p = (const char *)data;
  size_t sent = 0;
  while (sent < len) {
    ssize_t n = send(fd, p + sent, len - sent, 0);
    if (n <= 0) { perror("send"); MPI_Abort(MPI_COMM_WORLD, 1); }
    sent += (size_t)n;
  }
  close(fd);
}

/* Accept one connection on server_fd and receive exactly len bytes into buf. */
static void tcp_accept_recv(int server_fd, void *buf, size_t len) {
  int client_fd = accept(server_fd, NULL, NULL);
  if (client_fd < 0) { perror("accept"); MPI_Abort(MPI_COMM_WORLD, 1); }
  char *p = (char *)buf;
  size_t recvd = 0;
  while (recvd < len) {
    ssize_t n = recv(client_fd, p + recvd, len - recvd, 0);
    if (n <= 0) { perror("recv"); MPI_Abort(MPI_COMM_WORLD, 1); }
    recvd += (size_t)n;
  }
  close(client_fd);
}

int main(int argc, char **argv) {
  MPI_Init(&argc, &argv);

  int rank, size;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  MPI_Status status;

  char decompressed[BUF_LENGTH];
  if (rank == 0) {
    if (argc < 2) {
      fprintf(stderr, "Usage: %s <input_file>\n", argv[0]);
      MPI_Abort(MPI_COMM_WORLD, 1);
    }
    FILE *fileptr = fopen(argv[1], "rb");
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

    printf("File:         %s\n", argv[1]);
    printf("Uncompressed: %ld\n", srcSize);
    printf("Compressed:   %ld\n", compressed_size);
    printf("[rank 0] Compression:   %.6f s\n", t1 - t0);

    // Send sizes so receivers can bind their TCP sockets
    MPI_Send(&compressed_size, 1, MPI_UNSIGNED_LONG, C_NODE, 0, MPI_COMM_WORLD);
    MPI_Send(&srcSize,         1, MPI_UNSIGNED_LONG, U_NODE, 0, MPI_COMM_WORLD);

    // Receive TCP endpoint (hostname + port) from each receiver
    char c_host[256]; int c_port;
    char u_host[256]; int u_port;
    MPI_Recv(c_host,  sizeof(c_host), MPI_CHAR, C_NODE, 2, MPI_COMM_WORLD, &status);
    MPI_Recv(&c_port, 1,              MPI_INT,  C_NODE, 3, MPI_COMM_WORLD, &status);
    MPI_Recv(u_host,  sizeof(u_host), MPI_CHAR, U_NODE, 2, MPI_COMM_WORLD, &status);
    MPI_Recv(&u_port, 1,              MPI_INT,  U_NODE, 3, MPI_COMM_WORLD, &status);

    double start = MPI_Wtime();
    tcp_connect_send(c_host, c_port, tmp_compressed, compressed_size);
    printf("[rank 0] Send (C):      %.6f s\n", MPI_Wtime() - start);

    start = MPI_Wtime();
    tcp_connect_send(u_host, u_port, src, srcSize);
    printf("[rank 0] Send (U):      %.6f s\n", MPI_Wtime() - start);

    free(src);
    free(tmp_compressed);
  }

  if (rank == C_NODE) {
    // Receive expected size, bind TCP socket, advertise endpoint to rank 0
    size_t compressed_size;
    MPI_Recv(&compressed_size, 1, MPI_UNSIGNED_LONG, 0, 0, MPI_COMM_WORLD, &status);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); MPI_Abort(MPI_COMM_WORLD, 1); }
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in addr = {0};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = 0; // ephemeral
    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
      perror("bind"); MPI_Abort(MPI_COMM_WORLD, 1);
    }
    listen(server_fd, 1);
    socklen_t addrlen = sizeof(addr);
    getsockname(server_fd, (struct sockaddr *)&addr, &addrlen);
    int port = ntohs(addr.sin_port);

    char ipstr[256];
    get_local_ip(ipstr, sizeof(ipstr));
    MPI_Send(ipstr,  sizeof(ipstr), MPI_CHAR, 0, 2, MPI_COMM_WORLD);
    MPI_Send(&port,    1,               MPI_INT,  0, 3, MPI_COMM_WORLD);

    char *compressed = (char *)malloc(compressed_size);
    double start = MPI_Wtime();
    tcp_accept_recv(server_fd, compressed, compressed_size);
    printf("[rank 1] Receive:       %.6f s\n", MPI_Wtime() - start);
    close(server_fd);

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
    // Receive expected size, bind TCP socket, advertise endpoint to rank 0
    size_t recv_size;
    MPI_Recv(&recv_size, 1, MPI_UNSIGNED_LONG, 0, 0, MPI_COMM_WORLD, &status);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); MPI_Abort(MPI_COMM_WORLD, 1); }
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in addr = {0};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = 0;
    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
      perror("bind"); MPI_Abort(MPI_COMM_WORLD, 1);
    }
    listen(server_fd, 1);
    socklen_t addrlen = sizeof(addr);
    getsockname(server_fd, (struct sockaddr *)&addr, &addrlen);
    int port = ntohs(addr.sin_port);

    char ipstr[256];
    get_local_ip(ipstr, sizeof(ipstr));
    MPI_Send(ipstr,  sizeof(ipstr), MPI_CHAR, 0, 2, MPI_COMM_WORLD);
    MPI_Send(&port,    1,               MPI_INT,  0, 3, MPI_COMM_WORLD);

    char *data = (char *)malloc(recv_size);
    double recv_start = MPI_Wtime();
    tcp_accept_recv(server_fd, data, recv_size);
    printf("[rank 2] Receive:       %.6f s\n", MPI_Wtime() - recv_start);
    printf("[rank 2] Decompress:    %.6f s\n", 0.0);
    close(server_fd);
    free(data);
  }
  MPI_Finalize();
  return 0;
}
