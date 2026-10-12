# Basic sandbox: the same web image, booted in a cloud-hypervisor microVM with
# its own kernel, reachable over HTTP.
#
#   legion job run tests/spot-check/jobs/03-sandbox.ncl
#   legion job logs hello-vm -f           # the guest's serial console
#   curl http://<worker-public-ip>:8090/  # the worker it landed on
#
# Networking is the manual part. Legion attaches a TAP device but does not
# create one, address the guest, or forward ports into it
# (docs/task-runtime-sandbox.md, Networking). The rig's worker.sh pre-creates
# tap0 (host 172.30.0.1) and forwards host port 8090 to 172.30.0.2:8080, and
# the command below configures the guest's side. One TAP means one VM per
# worker, hence count = 1.
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
          ip addr add 172.30.0.2/24 dev eth0
          ip link set eth0 up
          ip route add default via 172.30.0.1
          PORT=8080 GREETING="from a Legion microVM" exec python3 /app/app.py
        "%,
      ],
      vcpus = 1,
      memory_mib = 512,
      hypervisor = "cloud-hypervisor",
      network = { mode = "tap", tap = "tap0", mac = "02:00:ac:1e:00:02" },
    },
    resources = { cpu = 1000, memory = 512 },
    constraints = { os = "linux" },
  },
}
