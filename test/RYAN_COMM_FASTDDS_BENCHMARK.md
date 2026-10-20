# Ryan communication container: Fast DDS scheduling check — 2026-10-20

## Environment and scope

Inspected `ryan-comm` on `ryan015` over SSH. Container ID `389c3116b343`,
image `dcd4186d77911bcd19b04366abfd31b79e2d86d15acf7b1440818de1372ff6fd`.
The container uses ROS Jazzy, rclpy 7.1.10 from its overlay, and
`rmw_fastrtps_cpp`. The required `_executor_event` flag is present.
The deployed Python package matches the local package before the scheduling
fix, based on SHA-256 checks of all installed top-level Python files.

The running endpoint was observed at 74.59% of one CPU core over ten seconds,
with 2,151,364 KiB RSS (about 2.05 GiB). This is a live baseline observation,
not a measured production before/after comparison. The machine has 20 logical
CPUs and the container has no CPU quota; the host was already busy.

Tests used a temporary copy of the **installed package inside the existing
container**, with only `subscriber.py` replaced and
`subscription_callback_group.py` added. Each benchmark starts a fresh endpoint
process and toggles the callback group to compare the original behavior with
the fix. The publisher and validating TCP reader are outside the measured
endpoint process. Normal ROS message serialization is used throughout.

The test processes used domain 183, localhost-only discovery, and temporary
loopback TCP ports. Production uses domain 0 with a Fast DDS super-client
profile. Discovery-server and profile environment variables were cleared only
for the test processes. This isolates the tests from the robot's ROS graph,
but means they do not reproduce production discovery/network traffic.

## Compatibility and regression validation

- All **48 behavior and scheduling tests pass** on the container's runtime.
- Disabling the new scheduling check reproduces the original regression:
  **576 middleware waits**, exceeding the limit of ten across three 50 ms spins.
  The regression passes with the fix, including delivery of the queued message
  and the real incompatible-QoS event after the workers resume.
- The expected failing baseline run emits teardown warnings after the assertion
  aborts; the passing suite does not fail on teardown.

## Measurement method

For each rate, three five-second samples per message type and configuration,
with one second of warmup. Configuration order alternates between repeats.
Payloads are a 1280×720 RGB image and a 1 MiB point cloud. Every received frame
is checked for destination, content, sequence, and ordering. Results below are
medians, not a statistical guarantee. The live robot workload continued during
testing, so host contention and sample variability affect the comparison.

| Rate | Workload | CPU ms/message before → after | CPU reduction | Median latency ms before → after | RSS MiB before → after |
| --- | --- | ---: | ---: | ---: | ---: |
| 2 Hz | image | 35.15 → 32.46 | 7.6% | 40.03 → 32.29 | 94.76 → 94.80 |
| 2 Hz | cloud | 26.10 → 25.05 | 4.0% | 19.36 → 21.62 | 77.82 → 77.70 |
| 30 Hz | image | 31.00 → 27.53 | 11.2% | 45.71 → 43.45 | 97.72 → 94.84 |
| 30 Hz | cloud | 18.62 → 16.66 | 10.5% | 23.47 → 22.63 | 77.93 → 77.79 |

All **2,304 messages** passed validation: 144 at 2 Hz and 2,160 at 30 Hz,
including warmup. At 2 Hz, CPU results overlap between configurations; one
image pair was slower with the fix. At 30 Hz, the improvement is more consistent.
Memory is essentially unchanged. Latency does not improve uniformly.

## Interpretation and deployment status

The workaround is compatible with this deployed runtime and fixes a reproduced
scheduling defect. It provides a modest CPU improvement in these isolated
workloads, much smaller than the earlier WSL benchmark. These measurements do
not demonstrate a large reduction in the live endpoint's CPU or its 2 GiB memory
footprint. Production topic mix, concurrency, discovery, network peers, and
client backpressure are not represented by this synthetic comparison.

During this isolated comparison, the production package was not modified and
the live endpoint was not restarted. The subsequent user-authorized live trial
is recorded below.

## Local evidence

The ignored `test-results/` directory contains:

- `ryan-comm-runtime.json` and `ryan-comm-installed-hashes.json`.
- `ryan-comm-fastdds-tests.log`: 48 passing tests.
- `ryan-comm-fastdds-regression-without-fix.log`: expected failing baseline.
- `ryan-comm-fastdds-comparison.json` and `ryan-comm-fastdds-high-rate.json`.
- Corresponding benchmark logs with individual sample results.

Reproduction uses `tools.benchmark_subscription_scheduling` with `--repeats 3
--duration 5 --cases image cloud --event-gating both`, once with `--rate-hz 2`
and once with `--rate-hz 30`, in an isolated copy of the installed package.

## Live deployment trial

The user subsequently authorized patching and restarting the live endpoint.
The two-file fix was installed in the existing container's package, and
`ryan-comm` was restarted at 22:01:18 UTC on 2026-10-20. Restarting the container
was used because its launch file does not enable endpoint respawning by default.
A fresh Python process verified the installed constructor uses the callback
group. The container returned to healthy status and the endpoint listened on
port 10000. Rosbridge also restarted and its Johnny client reconnected.

Before restart, six ten-second samples averaged **74.99% of one CPU core**;
median RSS was **2,101.83 MiB**. Both TCP clients remained connected, and the
endpoint sent approximately 25 bytes/s. This was light outgoing application
traffic, despite substantial endpoint CPU and memory usage.

In the first minute after restart, endpoint CPU averaged **4.08% of one core**
and median RSS was **89.89 MiB**. However, only the RyanFace client reconnected;
the other TCP client, which registered tablet/conversation publishers and
battery/proximity subscriptions, had not returned. This is **not a matched-load
comparison** and does not establish that the fix caused the observed reduction.
Restarting also resets accumulated process memory and DDS state.

At the user's direction, the tablet process (`DreamFace_Tablet.x86_64`, PID
1729053) received SIGTERM at 22:04:12 UTC. Its existing watchdog restarted it
as PID 2264183. The second TCP client reconnected, and the endpoint registered
its tablet/conversation publishers, battery/proximity subscriptions, services,
and actions. Both clients then stayed connected during observation.

The ROS graph showed 11 bridge subscriptions and 15 bridge publishers after
reconnection, including the tablet topics observed before restart. TCP outgoing
traffic was approximately 27 bytes/s, compared with 25 bytes/s before restart.
This was the current light application workload, not a camera-streaming or
interactive conversation stress test. No synthetic commands or robot actions
were sent by the monitoring scripts. The restarted tablet performed its normal
startup sequence, including its own behavior/action requests.

During tablet re-registration, the endpoint logged one recoverable invalid-node
handle warning while checking action-server availability and one cancellation
for an unknown/empty goal. The executor continued, registrations completed,
and both client connections remained up. These warnings should not be described
as a completely error-free startup.


### Results with both clients restored

Six ten-second baseline samples were compared with twelve ten-second samples
after both clients reconnected:

| Measurement | Before | After |
| --- | ---: | ---: |
| Mean endpoint CPU, % of one core | 74.99% | 71.62% |
| Median endpoint RSS | 2101.83 MiB | 131.57 MiB |
| TCP clients | 2 | 2 |
| Mean TCP output | 24.5 bytes/s | 25.6 bytes/s |

The observed CPU reduction is approximately 4.5%, a modest change.
The initial 4% CPU observation with only one client does **not** represent the
full current workload. RSS remained between 131.56 and
131.58 MiB across the two-minute measurement. The much larger
memory reduction is confounded by restarting a long-running process and must
not be claimed as a demonstrated RAM benefit of this scheduling fix. This
trial did not include a fresh unpatched restart control or a long memory soak.

The patch remains installed. Backups remain available, and temporary deployment
staging files were removed. Summary: `test-results/ryan-comm-live-summary.json`;
full two-client samples: `ryan-comm-live-both-clients.jsonl` and
`ryan-comm-live-both-clients-steady.jsonl` in the same results directory.

### Rollback and persistence

The original package and rollback script are retained on `ryan015` at:

```
/home/ryan-admin/ros-tcp-fastdds-backup-20261020.lWRVA9/
```

Running `rollback.sh` in that directory restores the original subscriber,
removes the callback-group module, and restarts `ryan-comm`. The patch currently
lives in the existing container's writable layer. It survives a restart of this
container but must be included in a rebuilt image to survive container replacement.

Local observation logs: `test-results/ryan-comm-live-before.jsonl`,
`ryan-comm-live-after-startup.jsonl`, `ryan-comm-live-after-warm.jsonl`, and
`ryan-comm-live-graph-after.json`. The monitor records process CPU/RSS, TCP
connections, counters, and socket queues without publishing ROS messages.

## Deeper profiling: idle action monitors dominate the remaining CPU

### Question and scope

What explains the endpoint's remaining roughly 70% core usage with two live
clients and very little outgoing TCP traffic, and which change offers the
largest savings without degrading action handling?

This follow-up did not alter or restart the production endpoint. The previously
deployed subscription scheduling workaround remained active. Live observation
used py-spy 0.4.2 in nonblocking mode and Linux perf; experiments used separate
processes in domain 184 with localhost discovery and no production XML profile.

### Ranked synthesis

| Rank | Finding | Confidence | Basis |
| --- | --- | --- | --- |
| 1 | Four idle action-health timers repeatedly trigger expensive executor work and graph-availability checks | High for the idle workload | Live Python/native profiles and an isolated reproduction with the same registration types |
| 2 | The executor's 100 ms timeout causes unnecessary rebuilds even after idle health timers are cancelled | High for idle behavior | Increasing the maximum wait to 1 s reduces the residual cost by about another order of magnitude |
| 3 | The previous 2 GiB memory footprint needs a separate lifetime/retention investigation | Unresolved | Current RSS is stable around 132 MiB; these CPU experiments do not materially reduce memory |

### Evidence

- `ros_tcp_endpoint/action_client.py:44` creates a 100 ms health timer for
  every action client, including clients with no pending or running goals.
- `ros_tcp_endpoint/action_client.py:303` queries server availability before
  examining the goal dictionaries. Native profiling shows this walking the
  Fast DDS graph cache through `rcl_action_server_is_available` and reader/writer
  count queries.
- `ros_tcp_endpoint/server.py:388` calls `spin_once(timeout_sec=0.1)`.
  The installed executor reconstructs its wait set on subsequent waits,
  repeatedly visiting the registered nodes and their QoS event handlers.
- Of 949 GIL-held Python stack samples, approximately 75% included
  `setup_executor`, and 16% ended in action-client `server_is_ready`.
  These are sampled stack shares, not precise CPU-time attribution.
  The nonblocking profiler reported 109 sampling errors; native perf supplied
  independent corroboration with 785 samples and no lost samples.
- Two action-status topics returned completed goals. The other two returned no
  status sample, which alone does not prove they had no active goals.
- The isolated reproduction creates 59 bridge/server nodes: 15 publishers,
  11 subscribers, 26 ROS service clients, two Unity services, four action
  clients, and the main endpoint. A separate mock-server node makes service
  and action availability checks real. No goals or data messages are sent in
  the CPU comparison. All action servers are confirmed discoverable.

### Isolated results

Three fresh-process, ten-second samples for each main configuration after a
warmup. Medians, on the same host while the live robot continued running:

| Configuration | CPU, % of one core | RSS MiB |
| --- | ---: | ---: |
| Current behavior, including the earlier subscription fix | 69.93% | 98.97 |
| Cancel health timers for the four idle action clients | 11.00% | 98.82 |
| Cancel idle health timers and increase maximum executor wait to 1 s | 1.37% | 98.80 |

That is approximately **84% less idle CPU** from cancelling the idle timers,
and **98% less** with both diagnostic changes. The no-message baseline
reproduces the live CPU level, which strongly supports housekeeping as the main
cause. A separate single-run control kept the timers running but made their
callbacks no-ops: CPU remained 53.10%, showing that the callbacks' direct graph
queries are only part of the cost. Waking and rebuilding the executor matters.

These are diagnostic experiments, not a tested production implementation.
Cancelling every health timer unconditionally would be incorrect when goals
are active. The experiment has no active goals, so it cannot establish that
pending goals, server loss, cancellation, and disconnect races remain correct.

### Recommended implementation direction

Arm each action-health timer only while that client has a goal awaiting
acceptance or completion; cancel it when both goal collections are empty.
Keep the existing immediate readiness check when sending a goal and retain
100 ms disappearance checks while work is outstanding. Verify transitions,
including goal acceptance/rejection, completion, cancellation, exceptions,
disconnect cleanup, and overlapping goals.

Then replace periodic idle executor polling with waiting for work, preserving
explicit executor wakeups for registration changes and shutdown. A longer
maximum wait is not a batching delay: ROS data/timers/guard conditions can wake
it earlier. The disposable wakeup check below tests that basic property;
production shutdown and registration races still need regression coverage.

The wakeup experiment added one ordinary String subscription and published
20 messages at 5 Hz in the isolated domain, with idle health timers cancelled
and a 1 s maximum executor wait. All 20 callbacks arrived, with 12.08 ms median
and 20.44 ms maximum publish-to-callback latency. This checks prompt wakeup,
not full TCP forwarding or action lifecycle correctness. Its log is
`test-results/ryan-topology-wakeup-probe.log`; the script is
`test-results/ryan_topology_wakeup_probe.py`.

### Memory limit and evidence files

During a further minute of live observation, RSS remained at 131.79 MiB.
The isolated CPU changes left RSS essentially unchanged. No claim of a RAM fix
is supported by these measurements, and a longer allocation/lifecycle study
would be needed to explain the earlier growth.

Evidence remains in the ignored `test-results/` directory:
`ryan-comm-live-gil-stacks.txt`, `ryan-comm-live-stacks.txt`,
`ryan-comm-live-perf.data`, `ryan-comm-live-perf.txt`,
`ryan-comm-live-perf-summary.txt`, `ryan-topology-comparison.json`,
`ryan_topology_probe.py`, `ryan-comm-topology.json`, and the corresponding logs.
Profiling method reference: [py-spy documentation](https://github.com/benfred/py-spy).
