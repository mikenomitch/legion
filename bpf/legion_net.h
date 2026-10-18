/* The ABI between the eBPF programs in legion_net.bpf.c and the worker that
 * loads them (src/worker/netd/kernel/abi.rs mirrors every struct here, field
 * for field). Changing a layout means changing both, and rebuilding the
 * object with bpf/build.sh.
 *
 * Addresses are in network byte order. Ports are in network byte order in
 * the low 16 bits, as the kernel hands them to sock_addr programs, unless a
 * field says otherwise. An IPv4 address in a 16-byte field is in addr[0]
 * with addr[1..3] zero.
 */
#ifndef LEGION_NET_H
#define LEGION_NET_H

#define LN_MAX_RULES 64
#define LN_MAX_ALLOW 32

/* ln_alloc.flags */
#define LN_A_GUARD (1u << 0)    /* the host guard applies */
#define LN_A_ENFORCE (1u << 1)  /* the egress chain is enforced here */
#define LN_A_GATED (1u << 2)    /* a gated namespace: never pass */
#define LN_A_CAPTURE (1u << 3)  /* redirect what needs netd to capture4 */
#define LN_A_DNS (1u << 4)      /* send port-53 traffic to resolver4 */
#define LN_A_INSPECT (1u << 5)  /* allowed flows need netd (hops, access log) */
#define LN_A_DEFAULT_ALLOW (1u << 6) /* fallthrough is allow */

/* ln_rule.flags */
#define LN_R_PORT (1u << 0)
#define LN_R_CIDR (1u << 1)
#define LN_R_HOST (1u << 2)
#define LN_R_DEFER (1u << 3) /* matched, but the request head decides */

/* ln_rule.action */
#define LN_DENY 0
#define LN_ALLOW 1

/* ln_event.kind, and the guard classes (ln_guard_class values) */
#define LN_EV_METADATA 1
#define LN_EV_DENY_LIST 2
#define LN_EV_CONTROL 3
#define LN_EV_DATAPATH 4
#define LN_EV_RULE 16        /* index = the rule that denied */
#define LN_EV_FALLTHROUGH 17
#define LN_EV_NEEDS_NETD 18  /* index = the deferring rule, or 255: unlearned host */
#define LN_EV_RAW_SOCKET 19
#define LN_EV_REDIRECT 20    /* informational: a flow sent to the capture listener */

/* Counter slots (ln_counter_key.slot) beyond the rule indexes. */
#define LN_SLOT_FALLTHROUGH 64
#define LN_SLOT_GUARD 65
#define LN_SLOT_NEEDS_NETD 66
#define LN_SLOT_REDIRECT 67
#define LN_SLOT_DNS 68

/* Node-wide settings: ARRAY map, one entry. */
struct ln_config {
	__u32 leaf_level; /* cgroup v2 level of an allocation's leaf */
	__u32 flags;      /* reserved */
};

/* One governed allocation, keyed by its leaf's cgroup id (u64). Key 0 is
 * the unattributed entry: a socket under the cgroup parent that no known
 * leaf owns gets its flags (the guard floor) and no exemptions. */
struct ln_alloc {
	__s64 allocation_id;
	__u32 flags;
	__u32 vip_block;    /* 127.a.b.0, or 0 */
	__u32 resolver4;    /* the allocation's resolver, or 0 */
	__u32 capture4;     /* the capture listener's address */
	__u16 capture_port; /* the capture listener's port */
	__u16 nrules;
	__u32 generation;   /* bumped on every rewrite, for the event reader */
};

struct ln_rule {
	__u32 addr[4];
	__u16 port;    /* network order, with LN_R_PORT */
	__u8 flags;
	__u8 prefix;   /* with LN_R_CIDR */
	__u8 action;
	__u8 family;   /* 4 or 6, with LN_R_CIDR */
	__u8 host_bit; /* with LN_R_HOST: the bit in ln_learned.bits */
	__u8 pad;
};

/* An allocation's compiled egress chain, keyed by leaf cgroup id. */
struct ln_rules {
	struct ln_rule rule[LN_MAX_RULES];
};

/* An address the allocation's resolver returned for an allowed name. */
struct ln_learned_key {
	__u64 leaf;
	__u32 addr[4];
};
struct ln_learned {
	__u64 bits;       /* the host rules a name resolving here matched */
	__u64 expires_ns; /* CLOCK_MONOTONIC (bpf_ktime_get_ns) */
};

/* Guard LPM keys: metadata and hostGuard.deny ranges, value = class. */
struct ln_lpm4 {
	__u32 prefixlen;
	__u32 addr;
};
struct ln_lpm6 {
	__u32 prefixlen;
	__u32 addr[4];
};

/* hostGuard.allow: an address or CIDR and a port. ARRAY, LN_MAX_ALLOW. */
struct ln_allow {
	__u32 addr[4];
	__u16 port;   /* network order */
	__u8 prefix;
	__u8 family;  /* 0: unused slot */
};

/* An address of this node (control ports are refused on these). */
struct ln_local_key {
	__u32 addr[4];
	__u32 family;
};

/* An allocation's own upstream loopback port. */
struct ln_own_port {
	__u64 leaf;
	__u32 port; /* network order */
	__u32 pad;
};

struct ln_counter_key {
	__u64 leaf;
	__u32 slot;    /* rule index, or an LN_SLOT_* value */
	__u32 verdict; /* LN_DENY / LN_ALLOW */
};

/* A redirected flow's original destination, by socket cookie until the
 * socket is bound, then by (netns, local address, local port). */
struct ln_orig {
	__u32 addr[4];
	__u16 port;   /* network order */
	__u8 family;
	__u8 pad;
	__u32 pad2;
	__s64 allocation_id;
	__u64 netns;
};
struct ln_endpoint {
	__u64 netns;
	__u32 addr[4];
	__u32 port;   /* host order, as sock_ops reports it */
	__u32 pad;
};

/* The nameserver a task asked, by socket cookie, so a redirected reply
 * reads as if it came from there. */
struct ln_asked {
	__u32 addr[4];
	__u32 port; /* network order */
	__u32 family;
};

struct ln_bytes {
	__u64 tx;
	__u64 rx;
};

/* One denial (or redirect), for the worker's event reader. */
struct ln_event {
	__s64 allocation_id;
	__u32 kind;
	__u32 index;
	__u32 addr[4];
	__u16 port;    /* network order */
	__u8 family;
	__u8 protocol; /* IPPROTO_TCP / IPPROTO_UDP */
	__u32 pid;
	__u32 generation;
};

#endif
