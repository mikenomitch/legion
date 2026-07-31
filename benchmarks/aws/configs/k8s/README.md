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
2. Deploy the KWOK controller (release manifest, version pinned in
   `scripts/versions.env`) onto the control plane (tolerating the
   control-plane taint), then apply `kwok-stages.yaml`.
3. Create fake nodes: `scripts/start-sim-fleet.sh k8s <total>` renders
   one Node object per fake node from `kwok-node.yaml.tpl`, shaped from
   the same `node_profiles` file every system uses, labeled
   `type=kwok` and tainted `kwok.x-k8s.io/node=fake:NoSchedule`. The
   translated Deployments carry the matching nodeSelector + toleration,
   so campaign pods land only on fake nodes.
4. Scrape metrics: the install script creates a `bench-metrics`
   ServiceAccount with the `system:monitoring` clusterrole, extracts a
   token, and appends an HTTPS scrape job for the scheduler (:10259),
   apiserver (:6443), and etcd to the loadgen Prometheus.

The fairness record captures: kubeadm/K8s version, KWOK version, the
rendered kubeadm config, and the explicit note that
`percentageOfNodesToScore` (and every other scheduler knob) is default.
