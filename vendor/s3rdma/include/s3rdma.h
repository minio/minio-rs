// s3rdma.h — C FFI header for libs3rdma.so
//
// S3 PUT/GET over RDMA transport: DC or RC, over RoCE or native InfiniBand,
// into any memory `ibv_reg_mr` accepts (host RAM, hugepages, mmap, GPU device
// memory). Implements the 81-byte `x-amz-rdma-token` descriptor, so it
// interoperates with any peer that speaks that format.

#ifndef S3RDMA_H
#define S3RDMA_H

#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void *S3RdmaHandle;
typedef void *S3RdmaBufHandle;
typedef void *S3RdmaClientHandle;

typedef struct S3RdmaStatusC {
    ssize_t size;       // bytes transferred, or -1 on error
    int32_t ibv_status; // IBV_WC_SUCCESS (0) on success
} S3RdmaStatusC;

typedef struct S3RdmaAsyncEventC {
    void   *async_handle;
    ssize_t status;
    int32_t ibv_status;
} S3RdmaAsyncEventC;

// Configuration. Field layout is frozen; do not reorder.
typedef struct S3RdmaConfig {
    int      num_dcis;        // default: 256
    unsigned cq_depth;        // default: 256
    uint64_t dc_key;          // default: 0xffeeddcc (must match the peer)
    uint8_t  timeout;         // default: 14
    unsigned hop_limit;       // default: 64
    int      max_wr;          // default: 256
    int      max_sge;         // default: 1
    uint8_t  retry_cnt;       // default: 7
    int      service_level;   // default: 0
    unsigned traffic_class;   // default: 0
    int      pkey_index;      // default: 0
} S3RdmaConfig;

// --- Version / availability ---
// Returns library version (static null-terminated string).
const char *s3rdma_version(void);

// Returns 1 if RDMA is available, 0 otherwise. Writes an error/status
// message into `errbuf` (null-terminated, truncated to errbuf_len-1).
int s3rdma_check_rdma(char *errbuf, size_t errbuf_len);

// --- Server lifecycle ---
// `ip` may be an IPv4 address, a device name (e.g. "mlx5_0"), or empty (first
// available device).
//
// An address resolves through the GID table, and failing that through the
// sysfs device-to-netdev mapping. The second step is what makes an address
// work on native InfiniBand, where GIDs carry no IP address and the table
// lookup alone finds nothing. RoCE resolves at the first step as it always
// has, which matters: the GID entry matching the exact address is not
// necessarily the one a device name would select.
//
// `port` is informational; RDMA addressing uses GID/DCT, not TCP port.
S3RdmaHandle s3rdma_server_init(const char *ip, unsigned port);
S3RdmaHandle s3rdma_server_init_with_config(const char *ip, unsigned port,
                                             S3RdmaConfig *cfg);
// As above, but writes the failure reason into `err_buf` (null-terminated,
// truncated to err_buf_len-1) when it returns NULL. `err_buf` may be NULL.
//
// Prefer this over pairing an init with s3rdma_last_error: that error is
// thread-local, so a caller whose runtime moves work between OS threads --
// Go's does -- must pin itself to one thread across both calls or read an
// empty string. This reports during the call, so no pinning is needed.
S3RdmaHandle s3rdma_server_init_with_config_err(const char *ip, unsigned port,
                                                 S3RdmaConfig *cfg,
                                                 char *err_buf,
                                                 size_t err_buf_len);
void s3rdma_server_free(S3RdmaHandle h);

// Copy the calling thread's last error into `buf` (null-terminated, truncated
// to len-1); returns bytes written excluding the NUL. Thread-local -- see the
// note on s3rdma_server_init_with_config_err.
size_t s3rdma_last_error(char *buf, size_t len);

// Rails this server can originate transfers from, or 0 for an invalid handle.
// An empty `ip` opens every device with an ACTIVE port, so a dual-rail host
// serves both without being told to; a specific address or device name selects
// that one rail. A named device repeated in the list is opened once.
int s3rdma_server_nic_count(S3RdmaHandle h);

// --- Buffer registration ---
// The buffer is registered on every rail, so a transfer can be served from
// whichever rail can reach the client that asked for it.
S3RdmaBufHandle s3rdma_register_buffer(S3RdmaHandle h, void *buf, size_t size);
void s3rdma_deregister_buffer(S3RdmaHandle h, S3RdmaBufHandle reg);

// Allocate / free a host-side mmap buffer suitable for registration.
void *s3rdma_alloc_host_buffer(S3RdmaHandle h, size_t size);
void  s3rdma_free_host_buffer(S3RdmaHandle h, void *buf, size_t size);

// --- Synchronous operations ---
// PUT: server RDMA-READs `size` bytes from the client into `reg+offset`.
S3RdmaStatusC s3rdma_put(S3RdmaHandle h, S3RdmaBufHandle reg,
                          const char *token, uint64_t offset, size_t size,
                          uint16_t channel);
// GET: server RDMA-WRITEs `size` bytes from `reg+offset` to the client.
S3RdmaStatusC s3rdma_get(S3RdmaHandle h, S3RdmaBufHandle reg,
                          const char *token, uint64_t offset, size_t size,
                          uint16_t channel);

// --- RC transport (DC clients need no handshake; RC is connection-oriented) ---
// RC handshake: connect a server RC QP to the client (described by `client_desc`,
// the same x-amz-rdma-token descriptor) and key it by the client's qp_num.
// Writes the NUL-terminated server descriptor into `desc_buf` (<= desc_len) and
// returns 0, else -1 (reason via s3rdma_last_error). After this the client's
// normal s3rdma_put/s3rdma_get are served over RC automatically, routed off the
// token's qp_num — no extra wire header.
int s3rdma_rc_connect(S3RdmaHandle h, const char *client_desc,
                       char *desc_buf, size_t desc_len);
// Tear down a client's RC connection (by its qp_num). Idempotent.
void s3rdma_rc_disconnect(S3RdmaHandle h, uint32_t client_qp_num);

// --- Asynchronous operations ---
S3RdmaStatusC s3rdma_put_async(S3RdmaHandle h, S3RdmaBufHandle reg,
                                const char *token, uint64_t offset, size_t size,
                                uint16_t channel, void **async_handle);
S3RdmaStatusC s3rdma_get_async(S3RdmaHandle h, S3RdmaBufHandle reg,
                                const char *token, uint64_t offset, size_t size,
                                uint16_t channel, void **async_handle);
int s3rdma_poll(S3RdmaHandle h, S3RdmaAsyncEventC *events, int max_events,
                 uint16_t channel);

// Release an async handle returned via the `async_handle` out-param of
// s3rdma_put_async / s3rdma_get_async. Safe to call after the matching
// event is observed in s3rdma_poll. The server will also drain any
// leaked handles on s3rdma_server_free.
void s3rdma_async_handle_free(void *handle);

// --- Channel management ---
// Returns u16::MAX (0xFFFF) when no channel is available.
uint16_t s3rdma_allocate_channel(S3RdmaHandle h);
void     s3rdma_free_channel(S3RdmaHandle h, uint16_t channel);

// --- Client side (S3 SDKs) ---
//
// An S3 SDK is the RDMA *passive* side: it pins the caller's buffer, mints an
// `x-amz-rdma-token` describing it, and sends that token on the ordinary HTTP
// request; the server then performs the one-sided transfer. Supports any
// memory ibv_reg_mr accepts -- host RAM, hugepages, mmap, GPU device memory --
// over RoCE or native InfiniBand.
//
// The client mints DC tokens, so it needs a DC-capable HCA (mlx5,
// ConnectX-4 and later). s3rdma_client_init fails on a device without DC
// support -- SoftRoCE, or another vendor's RoCE NIC -- rather than minting a
// token no server can honour; the RC path above is server-side only.
//
// Note the direction of s3rdma_put / s3rdma_get above: those move data on
// behalf of a *remote* client. Everything below is `s3rdma_client_` prefixed
// and does the opposite job.

// Memory classification returned by s3rdma_client_memory_type.
#define S3RDMA_MEM_SYSTEM 0
#define S3RDMA_MEM_CUDA_MANAGED 1
#define S3RDMA_MEM_CUDA_DEVICE 2
#define S3RDMA_MEM_UNKNOWN 3

// Open a client-side RDMA context on `device`: a device name such as "mlx5_0",
// several separated by commas ("mlx5_0,mlx5_1"), or NULL/empty to take
// S3RDMA_DEVICE from the environment and, failing that, *every* device whose
// port is ACTIVE. Returns NULL on failure and writes the reason into `err_buf`
// (NUL-terminated, truncated to err_buf_len-1); `err_buf` may be NULL.
//
// Multi-rail is automatic. A buffer is registered on every rail, and each
// token names one of them, chosen round-robin -- so consecutive transfers for
// one buffer spread across NICs without the caller arranging anything. A rail
// whose port goes down is skipped, and a caller that retries a failed transfer
// gets a token on a different rail, which is what carries a transfer through a
// NIC failure. A single-NIC host behaves exactly as it always did.
//
// The returned handle is an opaque id, not an address: do not dereference it.
// Ids are never reissued, so a handle that has been freed matches nothing
// afterwards. Every entry point below refuses a handle this library did not
// issue or has already freed, rather than acting on it -- a double free, and a
// stale handle used after another client was opened, are both safe.
S3RdmaClientHandle s3rdma_client_init(const char *device, char *err_buf,
                                       size_t err_buf_len);
// Free a client. Freeing twice, or freeing a handle that was never issued, is
// a no-op. The client is released once any operation still in flight on
// another thread has finished with it.
void s3rdma_client_free(S3RdmaClientHandle h);

// Returns 1 when `h` is a live client that can mint tokens. This reports a
// usable local RDMA device, not a reachable peer: DC is connectionless, so
// there is no session to probe. A server declining RDMA says so per request,
// with `x-amz-rdma-reply: 501`, and the SDK falls back to HTTP there.
int s3rdma_client_ready(S3RdmaClientHandle h);

// Rails this client can mint on, and how many are currently usable. A healthy
// count below the total means the client is running degraded but still
// serving. Both return 0 for a handle this library did not issue or has
// already freed -- indistinguishable from a client with no rails, which
// cannot occur, since init fails rather than returning a railless client.
// The reason is available from s3rdma_last_error.
int s3rdma_client_nic_count(S3RdmaClientHandle h);
int s3rdma_client_healthy_nic_count(S3RdmaClientHandle h);

// Report that the transfer for `token` failed, so the rail that token named is
// skipped until it recovers. Returns 0 if the rail was found, -1 otherwise.
//
// Optional: the next mint moves to another rail regardless, because selection
// is round-robin. Calling it makes the move immediate rather than eventual,
// which matters when one rail is down and the others are healthy.
int s3rdma_client_report_token_failure(S3RdmaClientHandle h, const char *token);

// Pin `ptr..ptr+size` for RDMA; 0 on success, -1 on failure. Registration is
// keyed by `ptr` and reference counted, so pinning the same address twice
// registers once and it stays pinned until a matching number of deregisters.
// The buffer is registered on every rail, so any token may name any of them.
int s3rdma_client_register(S3RdmaClientHandle h, void *ptr, size_t size);
// Drop a reference taken by s3rdma_client_register. 0 on success, -1 if `ptr`
// was not pinned.
int s3rdma_client_deregister(S3RdmaClientHandle h, void *ptr);

// Mint an x-amz-rdma-token for `size` bytes at `offset` within the pinned
// region at `ptr`. On success writes a NUL-terminated token into *token_out
// and returns 0; release it with s3rdma_client_free_token. Returns -1 on
// failure, leaving *token_out NULL.
//
// The token carries no direction: the server takes that from the S3 request
// (READ for a PUT, WRITE for a GET), and the memory is registered for both.
int s3rdma_client_get_token(S3RdmaClientHandle h, void *ptr, size_t size,
                             size_t offset, char **token_out);
void s3rdma_client_free_token(char *token);

// Classify `ptr` as host or CUDA memory, so a caller can tell whether its own
// fallback paths may touch the buffer directly. Resolved through
// dlopen("libcuda.so.1"), so libs3rdma keeps its zero CUDA link dependency:
// a host without the driver reports every pointer as S3RDMA_MEM_SYSTEM.
int s3rdma_client_memory_type(const void *ptr);

#ifdef __cplusplus
}
#endif

#endif // S3RDMA_H
