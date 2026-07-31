# One KWOK fake node, rendered per node by scripts/start-sim-fleet.sh
# with envsubst: NODE_NAME, NODE_CLASS, NODE_REGION, CPUS, MEMORY_GI,
# MAX_PODS. Shaped from the same node_profiles file as the Legion sim
# fleet and nomad-nodesim templates, so all three systems see the same
# fleet classes.
apiVersion: v1
kind: Node
metadata:
  name: "${NODE_NAME}"
  labels:
    type: kwok
    kubernetes.io/role: agent
    class: "${NODE_CLASS}"
    region: "${NODE_REGION}"
  annotations:
    kwok.x-k8s.io/node: fake
spec:
  taints:
    - key: kwok.x-k8s.io/node
      value: fake
      effect: NoSchedule
status:
  allocatable:
    cpu: "${CPUS}"
    memory: "${MEMORY_GI}Gi"
    pods: "${MAX_PODS}"
  capacity:
    cpu: "${CPUS}"
    memory: "${MEMORY_GI}Gi"
    pods: "${MAX_PODS}"
  nodeInfo:
    kubeletVersion: fake
