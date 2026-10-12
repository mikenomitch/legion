# Basic sandbox: the same web image, booted in a cloud-hypervisor microVM with
# its own kernel, reachable over HTTP.
#
#   legion job run tests/spot-check/jobs/03-sandbox.ncl
#   legion job logs hello-vm -f           # the guest's serial console
#   curl http://<worker-public-ip>:8090/  # the worker it landed on
#
# Legion creates the guest's TAP on br0 and leases it 172.30.0.2, the only
# address in the worker config's sandbox.bridges.br0 pool
# (docs/task-runtime-sandbox.md, Addressing). The rig's worker.sh builds br0
# (host 172.30.0.1) and forwards host port 8090 to 172.30.0.2:8080, which Legion
# does not do. One address means one VM per worker, hence count = 1.
#
# Exec into a running microVM is not supported (no guest agent yet), so this
# job is checked through HTTP and logs only.
let images = (import "lib/images.ncl") vars in
{
  name = "hello-vm",
  task = {
    name = "web",
    runtime = "sandbox",
    lifecycle = "service",
    count = 1,
    config = {
      image = images.web,
      command = [
        "/bin/sh",
        "-c",
        m%"
          echo "guest kernel $(uname -r), host is a microVM: $(cat /sys/class/dmi/id/sys_vendor 2>/dev/null)"
          # The kernel brought eth0 up with the lease (ip= on its command
          # line). These repeat it for a guest kernel without CONFIG_IP_PNP.
          ip addr replace "$LEGION_GUEST_ADDR/24" dev eth0 2>/dev/null
          ip link set eth0 up
          ip route replace default via 172.30.0.1 2>/dev/null
          PORT=8080 GREETING="from a Legion microVM" exec python3 /app/app.py
        "%,
      ],
      vcpus = 1,
      memory_mib = 512,
      hypervisor = "cloud-hypervisor",
      network = { mode = "tap", bridge = "br0" },
    },
    resources = { cpu = 1000, memory = 512 },
    constraints = { os = "linux" },
  },
}
