# Idle executor and action monitor validation — 2026-10-20

The candidate stops the 100 ms action-server health timer while no goals are in
flight or awaiting results. The first goal rearms it; rejection, failure, and
terminal results disarm it after the last outstanding goal. Active goals retain
the 100 ms health cadence. The executor's idle wait increases from 100 ms to one
second; messages and other ROS work still wake it immediately.

These changes are **local only**. The production `ryan-comm` container still has
the earlier Fast DDS subscription callback-group fix. No new robot restart or
Unity application change was made during this validation.

## Regressions caught and fixed

1. **Registration delay with a longer wait.** `executor.wake()` can rebuild the
   wait set without returning from `spin_once`, leaving the executor mutation
   lock unavailable until the timeout. A queued no-op executor task makes the
   spin call yield, even if both worker threads are occupied. The regression
   performs 25 add/remove cycles with free workers and another 25 with both
   workers blocked. Each pair must complete within 400 ms; measured maxima were
   1.02 ms and 0.67 ms in one run. Replacing the task with the previous wake while
   keeping the one-second wait makes both cases fail.
2. **Service destruction before response transmission.** Faster cleanup exposed
   a race: a Unity service callback could return, then its node could be destroyed
   before rclpy sent the ROS response. A draining callback group prevents new calls
   and waits through rclpy's response transmission before removal. Teardown has a
   two-second bound and logs a warning on timeout. A deterministic regression holds
   response transmission open and verifies teardown waits. The previous code fails
   this test with an invalid handle; the candidate passes. The full integration
   suite failed at disconnect before this fix and passed twice afterward.

## Validation results

Primary environment: cached user image `d50557ee9deb`, ROS Jazzy/rclpy 7.1.10,
Fast DDS, isolated WSL container and local-only discovery domains. Separate domains
were used for concurrently running tests. No robot topics were published to.

| Check | Result |
| --- | --- |
| Focused lifecycle tests, existing behavior tests, Fast DDS scheduling regression | **60 passed** |
| Existing complete TCP integration stress suite after service-drain fix | **2 complete passes** |
| Action stress | 300 requests over 3 idle/active cycles: 240 accepted, 60 rejected; all 240 results and feedback delivered, correct payloads, no duplicate terminal results |
| Peak overlapping accepted goals | 80; timer remains armed, then stops after the last result |
| Registration under worker saturation | 50 add/remove pairs total, all below 400 ms |
| 1280×720 RGB images at 100 Hz | 2 fresh processes × 1,100 validated messages, including warmup |
| 1 MiB point clouds at 100 Hz | 2 fresh processes × 1,100 validated messages, including warmup |
| Combined high-rate messages | **4,400 validated**, no sequence, destination, or payload failures |
| Native RoboStack Jazzy/rclpy 7.1.12 with dynamic Fast DDS | **13 focused tests passed** |
| Native RoboStack Jazzy/rclpy 7.1.12 with Cyclone DDS | **12 focused tests passed**, Fast DDS-specific test skipped |

The full TCP integration script additionally exercises QoS, shared subscriptions,
client ownership, non-reading clients, service errors/timeouts, action rejection,
cancellation, action-server disappearance, fieldless messages, pending-request
cleanup, and reconnects. The focused tests cover synchronous send failures, future
failures, overlapping goals, server loss before/after acceptance, late completions,
and unregistering with an outstanding acceptance future.

High-rate image p99 latency was 35.3–36.2 ms; cloud p99 was 24.3–28.7 ms. These are
stress validation measurements on shared hardware, not a production capacity or
hard real-time guarantee. Other tests ran concurrently. No RAM-saving claim is
made from these short tests.

## TCP connector checks

Inspected the sibling `ros-tcp-connector` repository, including its action goal
queueing, registration replay, connection loss handling, and network reader.
Endpoint health timers are internal and do not introduce or remove protocol
heartbeat frames. Goal commands and serialized payloads remain queued together.

`tools/build_connector_transport_probe.py` extracts the actual
`ReadToByteArray` and `ReadMessageContents` methods from that checkout into a C#
harness. Compiled and executed using the installed Windows .NET Framework C#
compiler/runtime. It passed:

- A 1.5-second idle pause followed by a keepalive.
- Byte-fragmented topic lengths, action command frames, payload lengths, payloads,
  and a topic name longer than the reader's initial scratch buffer.
- 1,000 ordered frames, including ten 1 MiB payloads and 990 4 KiB payloads,
  validating every payload byte.
- Cancellation of a pending read.

This executes the real frame-reading methods, **not the full Unity application**.
No Unity Editor is installed here. Unity main-thread dispatch, scene behavior,
and full application reconnect behavior have not been runtime-tested.

## Existing limitations reproduced separately

### Connector half-close polling

After the peer shuts down its sending direction, the C# reader can remain in
`DataAvailable` polling instead of detecting EOF. It was still pending after
600 ms with `ReadTimeout` set to 150 ms; cancellation released it. Inspection
shows the loop never attempts a read while no data is available, and does not
handle a zero-byte read explicitly. `ReaderThread` exceptions are also handled
separately from the connection writer loop. This is an existing transport issue,
not introduced by the endpoint optimization. The connector checkout was not
modified. A transport fix needs its own Unity reconnect validation.

### Simultaneous action result burst

A separate diagnostic accepted 80 overlapping goals and then completed all of
them together. Both the previous endpoint and the candidate delivered only 10
results and 10 feedback messages, leaving 70 pending. That matches the default
ROS action history depth of 10 and is consistent with queue overflow; attribution
to a particular DDS queue needs further instrumentation. An initial unpaced
100-request burst also lost acceptance responses. Therefore the passing lifecycle
stress waits for each acceptance, retains all 80 accepted goals concurrently,
and releases completions individually. It does **not** establish lossless
behavior for arbitrary instantaneous bursts.

This is the most significant remaining load-related finding. The idle-monitor
change does not solve it. Changing action QoS depth or adding admission/backpressure
needs separate validation and a memory/capacity policy, rather than silently
changing the test's production defaults.

### Cyclone deserialization differences

The six existing malformed-payload rejection tests fail under the alternate
Cyclone/RoboStack environment because its deserializer accepts those inputs. All
six failures reproduce with the previous endpoint source; the new lifecycle tests
pass. This is not a claim that the entire repository suite passes on Cyclone.

The repository-wide style configuration also reports existing import, quote,
docstring, whitespace, and unused-variable issues. It is not a clean lint baseline.

## Reproduce

In the cached container, source `/opt/ros/jazzy/setup.bash`, unset
`FASTRTPS_DEFAULT_PROFILES_FILE`, `FASTDDS_DEFAULT_PROFILES_FILE`, and
`ROS_SUPER_CLIENT`, and set an unused `ROS_DOMAIN_ID` with
`ROS_AUTOMATIC_DISCOVERY_RANGE=LOCALHOST`.

```sh
python3 -m pytest -q -p no:cacheprovider \
  test/test_idle_action_monitor.py test/test_idle_executor_stress.py \
  test/test_behavior.py test/test_subscription_callback_group.py
python3 -c 'import runpy; runpy.run_path("test/manual_integration_stress.py", run_name="__main__")'
python3 -m tools.benchmark_subscription_scheduling \
  --event-gating enabled --cases image cloud --rate-hz 100 --duration 10 \
  --repeats 2 --output /tmp/extreme-100hz.json
```

Run `python3 tools/build_connector_transport_probe.py` from WSL, compile the printed
`.cs` path with `C:\Windows\Microsoft.NET\Framework64\v4.0.30319\csc.exe`, and run
the resulting executable. The probe deliberately verifies and reports the existing
half-close problem; it should be updated when that connector defect is fixed.

Raw local logs are in ignored `test-results/`: `idle-executor-final-pass.log`,
`idle-monitor-drained-stress*.log`, `extreme-100hz.log`,
`idle-{cyclone,dynamic}-focused.log`, `connector-extreme.log`,
`negative-wakeup-test.log`, `negative-service-drain.log`,
`{baseline,candidate}-action-burst.log`, and `cyclone-baseline-behavior.log`.
The frozen simultaneous-completion diagnostic is
`test-results/test_baseline_action_burst.py`; it is expected to fail on both
versions and is not part of the passing regression suite.

## Subsequent robot deployment

At the user's request, this candidate was subsequently deployed to `ryan-comm`
on `ryan015` on 2026-10-20. The earlier "local only" statement describes the
initial validation stage. Deployment, live measurements, rollback location, and
functional checks are recorded in `RYAN_COMM_IDLE_LIVE_TEST.md`.
