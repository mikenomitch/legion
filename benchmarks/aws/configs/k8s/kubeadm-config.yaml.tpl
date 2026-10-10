# kubeadm config for the K8s comparison campaign: 3 stacked-etcd control
# planes on the same boxes Legion used. Rendered by
# scripts/install-k8s.sh with envsubst: CP0_IP, K8S_VERSION.
#
# Fairness rule: stock kubeadm defaults plus the documented tuning below.
# percentageOfNodesToScore, the scoring plugins, and every eviction and
# toleration default stay stock.
#
# NON-DEFAULT (documented): API client rate limits on the scheduler and
# controller-manager. The scheduler binds each pod with one API call and
# its client defaults to 50 QPS / 100 burst, so a stock cluster binds at
# most ~50 pods/sec no matter how fast the scheduling cycle is; the
# controller-manager's 20 QPS caps how fast ReplicaSets can even create
# the pods. Left at defaults, the K8s column would measure two client-side
# token buckets rather than the scheduler. Raising them is what
# SIG-Scalability does for its own large-cluster tests, and the
# `kube-api-*` flags are the ones kubeadm's static-pod manifests accept
# (the scheduler ignores them only when started with --config, which
# kubeadm does not do). Concurrent sync workers are raised for the same
# reason: 5 ReplicaSet workers serialize 10,000 Deployments.
#
# Also: metrics bind addresses (the loadgen Prometheus must reach
# kube-scheduler's phase timings, which bind to 127.0.0.1 by default) and
# a stable control-plane endpoint.
apiVersion: kubeadm.k8s.io/v1beta4
kind: ClusterConfiguration
kubernetesVersion: "${K8S_VERSION}"
controlPlaneEndpoint: "${CP0_IP}:6443"
scheduler:
  extraArgs:
    - name: bind-address
      value: "0.0.0.0"
    - name: kube-api-qps
      value: "1000"
    - name: kube-api-burst
      value: "1000"
controllerManager:
  extraArgs:
    - name: bind-address
      value: "0.0.0.0"
    - name: kube-api-qps
      value: "1000"
    - name: kube-api-burst
      value: "1000"
    - name: concurrent-replicaset-syncs
      value: "50"
    - name: concurrent-deployment-syncs
      value: "50"
---
apiVersion: kubeadm.k8s.io/v1beta4
kind: InitConfiguration
nodeRegistration:
  # Control-plane boxes run only the control plane (no workload kubelets
  # in the measurement); the default NoSchedule taint stays.
  kubeletExtraArgs:
    - name: node-ip
      value: "${PRIVATE_IP}"
