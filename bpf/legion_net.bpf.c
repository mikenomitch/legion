// SPDX-License-Identifier: GPL-2.0
/* The kernel classifier (docs/network-handlers.md, "The kernel classifier";
 * mesh interfaces plan §6.A).
 *
 * One fixed set of programs, attached once to the worker's cgroup parent, so
 * every allocation leaf under it inherits them. The policy is data in maps
 * that the worker fills from each heartbeat; nothing here is generated, and
 * no operator code is ever loaded.
 *
 * For each connect() (and unconnected UDP sendmsg) from a governed
 * allocation, in order:
 *
 *   1. its own datapath (its VIP block, its upstream loopback ports): pass;
 *   2. the host guard: hostGuard.allow, then cloud metadata, hostGuard.deny,
 *      a Legion control port on a local address, another allocation's
 *      datapath: EPERM, exactly as the nftables guard it replaces;
 *   3. DNS: port 53 goes to the allocation's own resolver;
 *   4. a gated allocation with capture: everything else is redirected to its
 *      in-namespace capture listener (the namespace is the boundary);
 *   5. the compiled egress chain: first match wins, as evaluate_connect does;
 *      a host rule matches an address its resolver learned for an allowed
 *      name. Allow passes (or redirects, when netd must see the flow), deny is
 *      EPERM, and what only netd can decide is redirected (or refused, when
 *      capture is off).
 *
 * Built with bpf/build.sh; the object is checked in next to this file.
 */
#include <linux/bpf.h>
#include <linux/types.h>

#include "legion_net.h"

#define SEC(name) __attribute__((section(name), used))
#define __uint(name, val) int (*name)[val]
#define __type(name, val) typeof(val) *name
#undef __always_inline
#define __always_inline inline __attribute__((always_inline))

static void *(*bpf_map_lookup_elem)(void *map, const void *key) = (void *)1;
static long (*bpf_map_update_elem)(void *map, const void *key, const void *value,
				   __u64 flags) = (void *)2;
static long (*bpf_map_delete_elem)(void *map, const void *key) = (void *)3;
static __u64 (*bpf_ktime_get_ns)(void) = (void *)5;
static __u64 (*bpf_get_current_pid_tgid)(void) = (void *)14;
static __u64 (*bpf_get_socket_cookie)(void *ctx) = (void *)46;
static __u64 (*bpf_skb_ancestor_cgroup_id)(void *skb, int level) = (void *)83;
static __u64 (*bpf_get_netns_cookie)(void *ctx) = (void *)122;
static __u64 (*bpf_get_current_ancestor_cgroup_id)(int level) = (void *)123;
static long (*bpf_ringbuf_output)(void *ringbuf, void *data, __u64 size, __u64 flags) =
	(void *)130;

#define AF_INET 2
#define AF_INET6 10
#define AF_PACKET 17
#define SOCK_RAW 3
#define IPPROTO_TCP 6
#define IPPROTO_UDP 17

#define bswap16(x) ((__u16)((((x) & 0xff) << 8) | (((x) >> 8) & 0xff)))
#define bswap32(x) ((((x) & 0xffu) << 24) | (((x) & 0xff00u) << 8) | \
		    (((x) >> 8) & 0xff00u) | (((x) >> 24) & 0xffu))
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define ln_htons(x) bswap16(x)
#define ln_htonl(x) bswap32(x)
#else
#define ln_htons(x) (x)
#define ln_htonl(x) (x)
#endif

/* ------------------------------------------------------------------ maps */

struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, 1);
	__type(key, __u32);
	__type(value, struct ln_config);
} ln_config SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 16384);
	__type(key, __u64);
	__type(value, struct ln_alloc);
} ln_allocs SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 16384);
	__type(key, __u64);
	__type(value, struct ln_rules);
} ln_rules SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_LRU_HASH);
	__uint(max_entries, 262144);
	__type(key, struct ln_learned_key);
	__type(value, struct ln_learned);
} ln_learned SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_LPM_TRIE);
	__uint(max_entries, 1024);
	__uint(map_flags, BPF_F_NO_PREALLOC);
	__type(key, struct ln_lpm4);
	__type(value, __u32);
} ln_guard4 SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_LPM_TRIE);
	__uint(max_entries, 1024);
	__uint(map_flags, BPF_F_NO_PREALLOC);
	__type(key, struct ln_lpm6);
	__type(value, __u32);
} ln_guard6 SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, LN_MAX_ALLOW);
	__type(key, __u32);
	__type(value, struct ln_allow);
} ln_allow SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 256);
	__type(key, struct ln_local_key);
	__type(value, __u32);
} ln_local SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 64);
	__type(key, __u32); /* port, network order */
	__type(value, __u32);
} ln_control_ports SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 65536);
	__type(key, __u32); /* port, network order */
	__type(value, __u32);
} ln_datapath_ports SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 65536);
	__type(key, struct ln_own_port);
	__type(value, __u32);
} ln_own_ports SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 262144);
	__type(key, struct ln_counter_key);
	__type(value, __u64);
} ln_counters SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_LRU_HASH);
	__uint(max_entries, 65536);
	__type(key, __u64); /* socket cookie */
	__type(value, struct ln_orig);
} ln_pending SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_LRU_HASH);
	__uint(max_entries, 65536);
	__type(key, struct ln_endpoint);
	__type(value, struct ln_orig);
} ln_orig SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_LRU_HASH);
	__uint(max_entries, 65536);
	__type(key, __u64); /* socket cookie */
	__type(value, struct ln_asked);
} ln_asked SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_PERCPU_HASH);
	__uint(max_entries, 16384);
	__type(key, __u64);
	__type(value, struct ln_bytes);
} ln_bytes SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_RINGBUF);
	__uint(max_entries, 1 << 18);
} ln_events SEC(".maps");

/* --------------------------------------------------------------- helpers */

/* What one flow is, as the programs see it. addr is network order; an IPv4
 * flow (including an IPv4-mapped IPv6 one) has family 4 and addr[0] set. */
struct flow {
	__u32 addr[4];
	__u32 port; /* network order */
	__u32 family;
	__u32 protocol;
};

static __always_inline __u64 leaf_of_current(void)
{
	__u32 zero = 0;
	struct ln_config *config = bpf_map_lookup_elem(&ln_config, &zero);
	if (!config || config->leaf_level == 0 || config->leaf_level > 32)
		return 0;
	return bpf_get_current_ancestor_cgroup_id(config->leaf_level);
}

static __always_inline struct ln_alloc *alloc_for(__u64 *leaf)
{
	struct ln_alloc *alloc = 0;
	if (*leaf)
		alloc = bpf_map_lookup_elem(&ln_allocs, leaf);
	if (!alloc) {
		/* A socket under the parent that no known leaf owns: the floor. */
		*leaf = 0;
		alloc = bpf_map_lookup_elem(&ln_allocs, leaf);
	}
	return alloc;
}

static __always_inline void count(__u64 leaf, __u32 slot, __u32 verdict)
{
	struct ln_counter_key key = { .leaf = leaf, .slot = slot, .verdict = verdict };
	__u64 *value = bpf_map_lookup_elem(&ln_counters, &key);
	if (value) {
		__sync_fetch_and_add(value, 1);
		return;
	}
	__u64 one = 1;
	bpf_map_update_elem(&ln_counters, &key, &one, BPF_NOEXIST);
}

static __always_inline void emit(struct ln_alloc *alloc, struct flow *flow, __u32 kind,
				 __u32 index)
{
	struct ln_event event = {};
	event.allocation_id = alloc->allocation_id;
	event.kind = kind;
	event.index = index;
	event.addr[0] = flow->addr[0];
	event.addr[1] = flow->addr[1];
	event.addr[2] = flow->addr[2];
	event.addr[3] = flow->addr[3];
	event.port = (__u16)flow->port;
	event.family = (__u8)flow->family;
	event.protocol = (__u8)flow->protocol;
	event.pid = bpf_get_current_pid_tgid() >> 32;
	event.generation = alloc->generation;
	bpf_ringbuf_output(&ln_events, &event, sizeof(event), 0);
}

static __always_inline int is_loopback(struct flow *flow)
{
	if (flow->family == 4)
		return (flow->addr[0] & ln_htonl(0xff000000)) == ln_htonl(0x7f000000);
	return flow->addr[0] == 0 && flow->addr[1] == 0 && flow->addr[2] == 0 &&
	       flow->addr[3] == ln_htonl(1);
}

static __always_inline int is_local(struct flow *flow)
{
	if (is_loopback(flow))
		return 1;
	if (flow->addr[0] == 0 && flow->addr[1] == 0 && flow->addr[2] == 0 &&
	    flow->addr[3] == 0)
		return 1; /* 0.0.0.0 / :: dial the node itself */
	struct ln_local_key key = {};
	key.addr[0] = flow->addr[0];
	key.addr[1] = flow->addr[1];
	key.addr[2] = flow->addr[2];
	key.addr[3] = flow->addr[3];
	key.family = flow->family;
	return bpf_map_lookup_elem(&ln_local, &key) != 0;
}

static __always_inline int prefix_match(__u32 *want, __u32 *have, __u32 prefix, __u32 words)
{
	/* `words` 32-bit words of network-order address, first `prefix` bits. */
	for (__u32 i = 0; i < 4; i++) {
		if (i >= words)
			break;
		if (prefix >= 32) {
			if (want[i] != have[i])
				return 0;
			prefix -= 32;
			continue;
		}
		if (prefix == 0)
			return 1;
		__u32 mask = ln_htonl(~((1u << (32 - prefix)) - 1));
		return (want[i] & mask) == (have[i] & mask);
	}
	return 1;
}

/* The allocation's own datapath: its VIP block and its loopback ports. */
static __always_inline int own_datapath(__u64 leaf, struct ln_alloc *alloc, struct flow *flow)
{
	if (flow->family != 4)
		return 0;
	if (alloc->vip_block &&
	    (flow->addr[0] & ln_htonl(0xffffff00)) == alloc->vip_block)
		return 1;
	if (is_loopback(flow) && flow->protocol == IPPROTO_TCP) {
		struct ln_own_port key = { .leaf = leaf, .port = flow->port };
		if (bpf_map_lookup_elem(&ln_own_ports, &key))
			return 1;
	}
	return 0;
}

/* The host guard. Returns a class (LN_EV_*) to refuse, 0 to go on. */
static __always_inline __u32 guard(struct flow *flow)
{
	for (__u32 i = 0; i < LN_MAX_ALLOW; i++) {
		__u32 slot = i;
		struct ln_allow *allow = bpf_map_lookup_elem(&ln_allow, &slot);
		if (!allow || allow->family == 0)
			break;
		if (allow->family == flow->family && allow->port == flow->port &&
		    flow->protocol == IPPROTO_TCP &&
		    prefix_match(allow->addr, flow->addr, allow->prefix,
				 flow->family == 4 ? 1 : 4))
			return 0;
	}
	__u32 *class = 0;
	if (flow->family == 4) {
		struct ln_lpm4 key = { .prefixlen = 32, .addr = flow->addr[0] };
		class = bpf_map_lookup_elem(&ln_guard4, &key);
	} else {
		struct ln_lpm6 key = { .prefixlen = 128 };
		key.addr[0] = flow->addr[0];
		key.addr[1] = flow->addr[1];
		key.addr[2] = flow->addr[2];
		key.addr[3] = flow->addr[3];
		class = bpf_map_lookup_elem(&ln_guard6, &key);
	}
	if (class)
		return *class;
	if (flow->protocol == IPPROTO_TCP && is_local(flow)) {
		__u32 port = flow->port;
		if (bpf_map_lookup_elem(&ln_control_ports, &port))
			return LN_EV_CONTROL;
	}
	if (flow->family == 4) {
		if ((flow->addr[0] & ln_htonl(0xffff0000)) == ln_htonl(0x7f2a0000))
			return LN_EV_DATAPATH;
		if (is_loopback(flow) && flow->protocol == IPPROTO_TCP) {
			__u32 port = flow->port;
			if (bpf_map_lookup_elem(&ln_datapath_ports, &port))
				return LN_EV_DATAPATH;
		}
	}
	return 0;
}

#define V_DENY 0
#define V_ALLOW 1
#define V_NEEDS_NETD 2

/* The compiled chain, first match wins. *index is the deciding rule (or a
 * slot for the fallthrough / an unlearned host). */
static __always_inline __u32 evaluate(__u64 leaf, struct ln_alloc *alloc, struct flow *flow,
				      __u32 *index)
{
	struct ln_rules *rules = bpf_map_lookup_elem(&ln_rules, &leaf);
	struct ln_learned_key lkey = { .leaf = leaf };
	lkey.addr[0] = flow->addr[0];
	lkey.addr[1] = flow->addr[1];
	lkey.addr[2] = flow->addr[2];
	lkey.addr[3] = flow->addr[3];
	struct ln_learned *learned = bpf_map_lookup_elem(&ln_learned, &lkey);
	__u64 bits = 0;
	int known = 0;
	if (learned && learned->expires_ns > bpf_ktime_get_ns()) {
		bits = learned->bits;
		known = 1;
	}
	if (rules) {
		for (__u32 i = 0; i < LN_MAX_RULES; i++) {
			if (i >= alloc->nrules)
				break;
			struct ln_rule *rule = &rules->rule[i];
			if ((rule->flags & LN_R_PORT) && rule->port != flow->port)
				continue;
			if (rule->flags & LN_R_CIDR) {
				if (rule->family != flow->family)
					continue;
				if (!prefix_match(rule->addr, flow->addr, rule->prefix,
						  flow->family == 4 ? 1 : 4))
					continue;
			}
			if (rule->flags & LN_R_HOST) {
				if (!known) {
					/* No name is known for this address: only netd can
					 * read one (SNI or Host) and judge it. */
					*index = 255;
					return V_NEEDS_NETD;
				}
				if (!(bits & (1ull << (rule->host_bit & 63))))
					continue;
			}
			*index = i;
			if (rule->flags & LN_R_DEFER)
				return V_NEEDS_NETD;
			return rule->action == LN_ALLOW ? V_ALLOW : V_DENY;
		}
	}
	*index = LN_SLOT_FALLTHROUGH;
	return (alloc->flags & LN_A_DEFAULT_ALLOW) ? V_ALLOW : V_DENY;
}

/* Remember where a redirected socket meant to go, until it is bound. */
static __always_inline void remember(void *ctx, struct ln_alloc *alloc, struct flow *flow)
{
	struct ln_orig orig = {};
	orig.addr[0] = flow->addr[0];
	orig.addr[1] = flow->addr[1];
	orig.addr[2] = flow->addr[2];
	orig.addr[3] = flow->addr[3];
	orig.port = (__u16)flow->port;
	orig.family = (__u8)flow->family;
	orig.allocation_id = alloc->allocation_id;
	orig.netns = bpf_get_netns_cookie(ctx);
	__u64 cookie = bpf_get_socket_cookie(ctx);
	bpf_map_update_elem(&ln_pending, &cookie, &orig, BPF_ANY);
}

/* What to do with one connect or sendmsg. 1 lets it go (possibly rewritten:
 * *redirect says to the capture listener, *dns to the resolver, both via
 * *out), 0 refuses it with EPERM. */
static __always_inline int decide(struct flow *flow, struct ln_alloc **out, int *redirect,
				  int *dns)
{
	__u64 leaf = leaf_of_current();
	struct ln_alloc *alloc = alloc_for(&leaf);
	*out = alloc;
	*redirect = 0;
	*dns = 0;
	if (!alloc)
		return 1;
	if (own_datapath(leaf, alloc, flow))
		return 1;
	if (alloc->flags & LN_A_GUARD) {
		__u32 class = guard(flow);
		if (class) {
			count(leaf, LN_SLOT_GUARD, LN_DENY);
			emit(alloc, flow, class, 0);
			return 0;
		}
	}
	if ((alloc->flags & LN_A_DNS) && alloc->resolver4 && flow->port == ln_htons(53)) {
		count(leaf, LN_SLOT_DNS, LN_ALLOW);
		*dns = 1;
		return 1;
	}
	/* Loopback is the node's own: the guard above covers Legion's part of
	 * it, and the chain judges what leaves the node. */
	if (is_loopback(flow))
		return 1;
	if ((alloc->flags & LN_A_GATED) && (alloc->flags & LN_A_CAPTURE)) {
		count(leaf, LN_SLOT_REDIRECT, LN_ALLOW);
		*redirect = 1;
		return 1;
	}
	if (!(alloc->flags & LN_A_ENFORCE))
		return 1;
	__u32 index = 0;
	__u32 verdict = evaluate(leaf, alloc, flow, &index);
	if (verdict == V_ALLOW && (alloc->flags & LN_A_INSPECT) && flow->protocol == IPPROTO_TCP)
		verdict = V_NEEDS_NETD;
	if (verdict == V_ALLOW) {
		count(leaf, index, LN_ALLOW);
		return 1;
	}
	if (verdict == V_DENY) {
		count(leaf, index, LN_DENY);
		emit(alloc, flow, index == LN_SLOT_FALLTHROUGH ? LN_EV_FALLTHROUGH : LN_EV_RULE,
		     index);
		return 0;
	}
	if ((alloc->flags & LN_A_CAPTURE) && flow->protocol == IPPROTO_TCP && alloc->capture4) {
		count(leaf, LN_SLOT_REDIRECT, LN_ALLOW);
		*redirect = 1;
		return 1;
	}
	count(leaf, LN_SLOT_NEEDS_NETD, LN_DENY);
	emit(alloc, flow, LN_EV_NEEDS_NETD, index);
	return 0;
}

static __always_inline void flow4(struct bpf_sock_addr *ctx, struct flow *flow)
{
	flow->addr[0] = ctx->user_ip4;
	flow->addr[1] = 0;
	flow->addr[2] = 0;
	flow->addr[3] = 0;
	flow->port = ctx->user_port & 0xffff;
	flow->family = 4;
	flow->protocol = ctx->protocol;
}

/* An IPv4-mapped IPv6 address (::ffff:a.b.c.d) is judged as IPv4. */
static __always_inline int flow6(struct bpf_sock_addr *ctx, struct flow *flow)
{
	__u32 a0 = ctx->user_ip6[0], a1 = ctx->user_ip6[1], a2 = ctx->user_ip6[2],
	      a3 = ctx->user_ip6[3];
	flow->port = ctx->user_port & 0xffff;
	flow->protocol = ctx->protocol;
	if (a0 == 0 && a1 == 0 && a2 == ln_htonl(0x0000ffff)) {
		flow->addr[0] = a3;
		flow->addr[1] = 0;
		flow->addr[2] = 0;
		flow->addr[3] = 0;
		flow->family = 4;
		return 1;
	}
	flow->addr[0] = a0;
	flow->addr[1] = a1;
	flow->addr[2] = a2;
	flow->addr[3] = a3;
	flow->family = 6;
	return 0;
}

static __always_inline void rewrite4(struct bpf_sock_addr *ctx, __u32 addr, __u16 port)
{
	ctx->user_ip4 = addr;
	ctx->user_port = port;
}

static __always_inline void rewrite6_mapped(struct bpf_sock_addr *ctx, __u32 addr, __u16 port)
{
	ctx->user_ip6[0] = 0;
	ctx->user_ip6[1] = 0;
	ctx->user_ip6[2] = ln_htonl(0x0000ffff);
	ctx->user_ip6[3] = addr;
	ctx->user_port = port;
}

/* -------------------------------------------------------------- programs */

SEC("cgroup/connect4")
int legion_connect4(struct bpf_sock_addr *ctx)
{
	struct flow flow;
	struct ln_alloc *alloc;
	int redirect, dns;
	flow4(ctx, &flow);
	if (!decide(&flow, &alloc, &redirect, &dns))
		return 0;
	if (!alloc)
		return 1;
	if (dns) {
		rewrite4(ctx, alloc->resolver4, ln_htons(53));
	} else if (redirect) {
		remember(ctx, alloc, &flow);
		rewrite4(ctx, alloc->capture4, alloc->capture_port);
	}
	return 1;
}

SEC("cgroup/connect6")
int legion_connect6(struct bpf_sock_addr *ctx)
{
	struct flow flow;
	struct ln_alloc *alloc;
	int redirect, dns;
	int mapped = flow6(ctx, &flow);
	if (!decide(&flow, &alloc, &redirect, &dns))
		return 0;
	if (!mapped || !alloc)
		return 1; /* the resolver and capture listeners are IPv4 */
	if (dns) {
		rewrite6_mapped(ctx, alloc->resolver4, ln_htons(53));
	} else if (redirect) {
		remember(ctx, alloc, &flow);
		rewrite6_mapped(ctx, alloc->capture4, alloc->capture_port);
	}
	return 1;
}

static __always_inline void remember_asked(void *ctx, struct flow *flow)
{
	struct ln_asked asked = {};
	asked.addr[0] = flow->addr[0];
	asked.addr[1] = flow->addr[1];
	asked.addr[2] = flow->addr[2];
	asked.addr[3] = flow->addr[3];
	asked.port = flow->port;
	asked.family = flow->family;
	__u64 cookie = bpf_get_socket_cookie(ctx);
	bpf_map_update_elem(&ln_asked, &cookie, &asked, BPF_ANY);
}

SEC("cgroup/sendmsg4")
int legion_sendmsg4(struct bpf_sock_addr *ctx)
{
	struct flow flow;
	struct ln_alloc *alloc;
	int redirect, dns;
	flow4(ctx, &flow);
	flow.protocol = IPPROTO_UDP;
	if (!decide(&flow, &alloc, &redirect, &dns))
		return 0;
	if (!dns || !alloc)
		return 1; /* UDP is never captured; netd could not read it */
	remember_asked(ctx, &flow);
	rewrite4(ctx, alloc->resolver4, ln_htons(53));
	return 1;
}

SEC("cgroup/sendmsg6")
int legion_sendmsg6(struct bpf_sock_addr *ctx)
{
	struct flow flow;
	struct ln_alloc *alloc;
	int redirect, dns;
	int mapped = flow6(ctx, &flow);
	flow.protocol = IPPROTO_UDP;
	if (!decide(&flow, &alloc, &redirect, &dns))
		return 0;
	if (!dns || !mapped || !alloc)
		return 1;
	remember_asked(ctx, &flow);
	rewrite6_mapped(ctx, alloc->resolver4, ln_htons(53));
	return 1;
}

/* A reply from the resolver a query was sent to reads as if it came from
 * the nameserver the task asked. */
SEC("cgroup/recvmsg4")
int legion_recvmsg4(struct bpf_sock_addr *ctx)
{
	__u64 cookie = bpf_get_socket_cookie(ctx);
	struct ln_asked *asked = bpf_map_lookup_elem(&ln_asked, &cookie);
	if (!asked || asked->family != 4)
		return 1;
	if ((ctx->user_port & 0xffff) != ln_htons(53))
		return 1;
	ctx->user_ip4 = asked->addr[0];
	ctx->user_port = asked->port;
	return 1;
}

SEC("cgroup/recvmsg6")
int legion_recvmsg6(struct bpf_sock_addr *ctx)
{
	__u64 cookie = bpf_get_socket_cookie(ctx);
	struct ln_asked *asked = bpf_map_lookup_elem(&ln_asked, &cookie);
	if (!asked || asked->family != 4)
		return 1;
	if ((ctx->user_port & 0xffff) != ln_htons(53))
		return 1;
	ctx->user_ip6[0] = 0;
	ctx->user_ip6[1] = 0;
	ctx->user_ip6[2] = ln_htonl(0x0000ffff);
	ctx->user_ip6[3] = asked->addr[0];
	ctx->user_port = asked->port;
	return 1;
}

/* Raw IP and packet sockets skip everything above, so a governed task
 * cannot open one. */
SEC("cgroup/sock_create")
int legion_sock_create(struct bpf_sock *sk)
{
	if (sk->type != SOCK_RAW && sk->family != AF_PACKET)
		return 1;
	__u64 leaf = leaf_of_current();
	struct ln_alloc *alloc = alloc_for(&leaf);
	if (!alloc || !(alloc->flags & (LN_A_GUARD | LN_A_ENFORCE)))
		return 1;
	struct flow flow = {};
	flow.family = sk->family == AF_INET6 ? 6 : 4;
	count(leaf, LN_SLOT_GUARD, LN_DENY);
	emit(alloc, &flow, LN_EV_RAW_SOCKET, 0);
	return 0;
}

/* A redirected socket now has its source port: key its original
 * destination by (netns, local address, local port), which is what the
 * capture listener sees as the peer. */
SEC("sockops")
int legion_sockops(struct bpf_sock_ops *skops)
{
	if (skops->op != BPF_SOCK_OPS_TCP_CONNECT_CB)
		return 1;
	__u64 cookie = bpf_get_socket_cookie(skops);
	struct ln_orig *orig = bpf_map_lookup_elem(&ln_pending, &cookie);
	if (!orig)
		return 1;
	struct ln_endpoint key = {};
	key.netns = orig->netns;
	if (skops->family == AF_INET) {
		key.addr[0] = skops->local_ip4;
	} else {
		key.addr[0] = skops->local_ip6[0];
		key.addr[1] = skops->local_ip6[1];
		key.addr[2] = skops->local_ip6[2];
		key.addr[3] = skops->local_ip6[3];
	}
	key.port = skops->local_port;
	bpf_map_update_elem(&ln_orig, &key, orig, BPF_ANY);
	bpf_map_delete_elem(&ln_pending, &cookie);
	return 1;
}

/* Bytes per allocation, for pass flows that never reach netd's ledgers. */
static __always_inline int account(struct __sk_buff *skb, int egress)
{
	__u32 zero = 0;
	struct ln_config *config = bpf_map_lookup_elem(&ln_config, &zero);
	if (!config || config->leaf_level == 0 || config->leaf_level > 32)
		return 1;
	__u64 leaf = bpf_skb_ancestor_cgroup_id(skb, config->leaf_level);
	if (!leaf || !bpf_map_lookup_elem(&ln_allocs, &leaf))
		return 1;
	struct ln_bytes *bytes = bpf_map_lookup_elem(&ln_bytes, &leaf);
	if (!bytes) {
		struct ln_bytes zeroed = {};
		bpf_map_update_elem(&ln_bytes, &leaf, &zeroed, BPF_NOEXIST);
		bytes = bpf_map_lookup_elem(&ln_bytes, &leaf);
		if (!bytes)
			return 1;
	}
	if (egress)
		bytes->tx += skb->len;
	else
		bytes->rx += skb->len;
	return 1;
}

SEC("cgroup_skb/egress")
int legion_skb_egress(struct __sk_buff *skb)
{
	return account(skb, 1);
}

SEC("cgroup_skb/ingress")
int legion_skb_ingress(struct __sk_buff *skb)
{
	return account(skb, 0);
}

char _license[] SEC("license") = "GPL";
