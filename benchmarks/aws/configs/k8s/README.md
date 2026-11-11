# K8s campaign configs

The Kubernetes comparison cluster is kubeadm with 3 stacked-etcd control
planes (the same boxes Legion used) and a KWOK-managed fake-node fleet —
KWOK is what SIG-Scheduling itself benchmarks the scheduler with, and
the K8s counterpart of `legion start worker --simulate`.

Install flow (`scripts/install-k8s.sh`, phase 4 of the plan):

1. `kubeadm init --config kubeadm-config.yaml` on cp-0 (rendered from
   the `.tpl` here), then `kubeadm join --control-plane` on cp-1/cp-2
   with the printed certificate key. CNI: none — no real pods run, and
   KWOK fakes pod IPs; the control plane itself runs with hostNetwork.
2. Install kubectl on the loadgen box and copy the admin kubeconfig
   there. KWOK is **not** deployed in-cluster: without a CNI the
   control-plane nodes never go Ready, so a controller pod would never
   schedule.
3. Create fake nodes: `scripts/start-sim-fleet.sh k8s <total>` renders
   one Node object per fake node from `kwok-node.yaml.tpl`, shaped from
   the same `node_profiles` file every system uses, labeled `type=kwok`
   and `bench-slice=<i>`, and tainted `kwok.x-k8s.io/node=fake:NoSchedule`.
   It then runs one KWOK controller per worker instance
   (`kwok-slice.service`), each managing only its own slice, so the storm
   drill can kill a slice by stopping one unit. Slice `i` talks to
   apiserver `i % 3`, so lease renewals and pod status writes spread over
   all three control planes the way Nomad's simulated clients spread over
   its three servers (`K8S_SPREAD_APISERVERS=0` puts them all on cp-0). The controllers run the
   release's `stage-fast.yaml` (KWOK 0.8 has no built-in stages): pods go
   Running immediately and node leases are heartbeated.
   `kwok-stages.yaml` here is the older equivalent, kept for reference
   and not applied.
4. Scrape metrics: the install script creates a `bench-metrics`
   ServiceAccount with the `system:monitoring` clusterrole, extracts a
   token, and appends an HTTPS scrape job for the scheduler (:10259),
   apiserver (:6443), and etcd to the loadgen Prometheus.

The fairness record captures the kubeadm/K8s version, the KWOK version,
and the rendered kubeadm config. That config raises the scheduler's and
controller-manager's API client QPS and etcd's backend quota (see its
header for why) and leaves every scheduling and eviction knob at its
default. etcd's data dir, `/var/lib/etcd`, is bind-mounted onto the
control plane's local NVMe by cloud-init, the same disk as Legion's and
Nomad's stores.
