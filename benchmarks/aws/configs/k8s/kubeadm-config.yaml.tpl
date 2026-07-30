# kubeadm config for the K8s comparison campaign: 3 stacked-etcd control
# planes on the same boxes Legion used. Rendered by
# scripts/install-k8s.sh with envsubst: CP0_IP, K8S_VERSION.
#
# Fairness rule: stock kubeadm defaults; the only additions are the
# metrics bind addresses (so the loadgen Prometheus can scrape scheduler
# phase timings — kube-scheduler binds metrics to 127.0.0.1 by default)
# and a stable control-plane endpoint. percentageOfNodesToScore stays at
# its default, per the plan's fairness record.
apiVersion: kubeadm.k8s.io/v1beta4
kind: ClusterConfiguration
kubernetesVersion: "${K8S_VERSION}"
controlPlaneEndpoint: "${CP0_IP}:6443"
scheduler:
  extraArgs:
    - name: bind-address
      value: "0.0.0.0"
controllerManager:
  extraArgs:
    - name: bind-address
      value: "0.0.0.0"
---
apiVersion: kubeadm.k8s.io/v1beta4
kind: InitConfiguration
nodeRegistration:
  # Control-plane boxes run only the control plane (no workload kubelets
  # in the measurement); the default NoSchedule taint stays.
  kubeletExtraArgs:
    - name: node-ip
      value: "${PRIVATE_IP}"
