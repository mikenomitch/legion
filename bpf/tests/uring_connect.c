/* Connect a TCP socket through io_uring (IORING_OP_CONNECT) and print the
 * result: "ok", or the errno name. No liburing: raw syscalls. */
#include <linux/io_uring.h>
#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>

int main(int argc, char **argv) {
  if (argc < 3) return 2;
  struct io_uring_params p; memset(&p, 0, sizeof p);
  int ring = syscall(__NR_io_uring_setup, 4, &p);
  if (ring < 0) { printf("setup:%s\n", strerror(errno)); return 1; }
  size_t sq_sz = p.sq_off.array + p.sq_entries * sizeof(unsigned);
  size_t cq_sz = p.cq_off.cqes + p.cq_entries * sizeof(struct io_uring_cqe);
  char *sq = mmap(0, sq_sz, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_POPULATE, ring, IORING_OFF_SQ_RING);
  char *cq = (p.features & IORING_FEAT_SINGLE_MMAP) ? sq : mmap(0, cq_sz, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_POPULATE, ring, IORING_OFF_CQ_RING);
  if (p.features & IORING_FEAT_SINGLE_MMAP && cq_sz > sq_sz) { munmap(sq, sq_sz); sq = cq = mmap(0, cq_sz, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_POPULATE, ring, IORING_OFF_SQ_RING); }
  struct io_uring_sqe *sqes = mmap(0, p.sq_entries * sizeof(struct io_uring_sqe), PROT_READ|PROT_WRITE, MAP_SHARED|MAP_POPULATE, ring, IORING_OFF_SQES);
  int s = socket(AF_INET, SOCK_STREAM, 0);
  struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = htons(atoi(argv[2])) };
  inet_pton(AF_INET, argv[1], &addr.sin_addr);
  struct io_uring_sqe *sqe = &sqes[0]; memset(sqe, 0, sizeof *sqe);
  sqe->opcode = IORING_OP_CONNECT; sqe->fd = s; sqe->addr = (unsigned long)&addr; sqe->off = sizeof addr;
  unsigned *tail = (unsigned *)(sq + p.sq_off.tail), *array = (unsigned *)(sq + p.sq_off.array);
  array[*tail & *(unsigned *)(sq + p.sq_off.ring_mask)] = 0;
  __atomic_store_n(tail, *tail + 1, __ATOMIC_RELEASE);
  if (syscall(__NR_io_uring_enter, ring, 1, 1, IORING_ENTER_GETEVENTS, 0, 0) < 0) { printf("enter:%s\n", strerror(errno)); return 1; }
  unsigned *head = (unsigned *)(cq + p.cq_off.head);
  struct io_uring_cqe *cqes = (struct io_uring_cqe *)(cq + p.cq_off.cqes);
  int res = cqes[*head & *(unsigned *)(cq + p.cq_off.ring_mask)].res;
  if (res == 0) printf("ok\n"); else if (res == -EPERM) printf("eperm\n"); else printf("errno:%d\n", -res);
  return 0;
}
