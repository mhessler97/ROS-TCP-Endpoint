# Fast DDS subscription scheduling — 2026-10-20

## Finding

The major CPU overhead in the subscription benchmark comes from
repeated empty Fast DDS waits, not from useful message processing. The library
now assigns a dedicated callback group to each Fast DDS bridge subscription to
avoid this condition. Subscriptions use the original ROS message conversion path.

An instrumented run forwarding six measured image messages recorded 6,872
middleware waits: 6,841 returned without any ready standard entity, and only six
subscription callbacks were scheduled. With the callback-group change, the same
probe recorded 40 waits, zero empty returns, and six subscription callbacks.
Cyclone DDS also recorded zero empty returns without the workaround.

This matches [rmw_fastrtps issue #929](https://github.com/ros2/rmw_fastrtps/issues/929).
Fast DDS shares a subscription's data-ready condition with its status events.
When rclpy queues the subscription for a worker, it excludes the subscription
from its next wait set, but its status events can remain eligible. An unread
message then wakes the wait even though no actual status event is ready. Repeated
wait-set rebuilding consumes CPU and competes with worker threads for execution.

The proposed middleware fix, [PR #930](https://github.com/ros2/rmw_fastrtps/pull/930),
was still open and targeted Rolling when checked. This repository change does
not require installing that unmerged patch or changing the user's container image.

## Library change

`SubscriptionCallbackGroup` extends normal mutual exclusion by making a
subscription's event handlers temporarily ineligible while that subscription is
already queued for execution. Once the worker takes the message and finishes its
callback, the existing rclpy wakeups make the event handlers eligible again.

- Applies only to bridge subscriptions using `rmw_fastrtps_cpp` or
  `rmw_fastrtps_dynamic_cpp`.
- Preserves registered QoS/type diagnostic handlers, reliability, history depth,
  message order, and the existing executor worker count.
- Introduces no sleeps, polling interval, message dropping, or rate throttling.
- Uses rclpy's `_executor_event` queued-work flag, the same flag checked by its
  executor. If a future version removes it, the group falls back to ordinary
  mutual exclusion; this would lose the optimization, not disable callbacks.
- Covers `RosSubscriber` bridge entities. It is not a general patch for other
  applications or internally created subscriptions such as `/clock`.

## Paired full-endpoint measurements

Environment: user-supplied image `d50557ee9deb`, WSL container with 512 MiB shared
memory, Jazzy/rclpy 7.1.10, Fast DDS, local discovery. The benchmark uses the actual
endpoint and TCP protocol. Publisher and validating receiver run outside the
measured endpoint process. No profiling or diagnostic counters were enabled for
these timing comparisons.

Three fresh-process samples per configuration, alternating order, 2 Hz, two
warmup and ten measured messages per sample. Both configurations use normal
object subscriptions; only the callback-group choice changes. Medians:

| Workload | CPU ms/message before | CPU ms/message after | Median latency before/after ms | End RSS before/after MiB |
| --- | ---: | ---: | ---: | ---: |
| 1280×720 RGB image | 94.67 | 7.34 | 88.11 / 25.73 | 97.80 / 97.53 |
| 1 MiB PointCloud2 | 94.37 | 6.73 | 92.05 / 15.21 | 80.32 / 80.59 |

This is approximately **92.2% and 92.9% less endpoint CPU per message** in these
workloads. Image p99 latency fell from a median 233.79 ms to 26.49 ms; point-cloud
p99 fell from 224.50 ms to 25.16 ms. With just ten measured frames in each run,
these p99 values represent the slowest samples, not a robust tail estimate.
All 144 messages including warmup passed field, sequence, and destination checks.

Resident memory is essentially unchanged. This is a CPU/scheduling improvement,
not evidence of a significant RAM reduction. Short benchmarks on a shared WSL
host do not establish production capacity or hard real-time guarantees.

## Higher-rate validation

With the scheduling change enabled, three samples per message type at **30 Hz**
each forwarded 30 warmup and 150 measured messages. All **1,080 messages** using
normal object subscriptions passed content, ordering, and destination checks.
Median results:

| Workload | CPU ms/message | CPU % of one core | End RSS MiB | Median latency ms |
| --- | ---: | ---: | ---: | ---: |
| Image | 4.675 | 14.07 | 97.80 | 16.12 |
| PointCloud2 | 4.037 | 12.15 | 80.55 | 14.88 |

These are the object-subscription samples from the original benchmark. The
experimental raw forwarding path has been removed. A real Unity application
and a longer production-like workload were not exercised.

## Regression coverage

The targeted regression occupies both executor workers, publishes a message,
then creates a publisher with incompatible reliability. While the data callback
is queued, three 50 ms spins must perform at most ten middleware waits. After
releasing the workers, both the original message and the incompatible-QoS event
must arrive.

Replacing the new eligibility check with ordinary mutual exclusion makes the
regression fail: the old behavior performs roughly 1,500 waits in that interval.
The existing live integration stress script also passed with normal object
subscriptions. It covers shared topics, independent client ownership,
reconnects, a non-reading peer, QoS history, services, actions, and disconnect
cleanup. The scheduling regression intentionally skips on Cyclone DDS.

The supplied image lacks `rmw_fastrtps_dynamic_cpp`. The object-subscription
scheduling regression previously passed with that backend in the separate
RoboStack Jazzy/rclpy 7.1.12 environment.

## Verification after removing raw forwarding

On the supplied Fast DDS image, the remaining behavior and scheduling suite
passes **48 tests**, and the complete live integration stress suite passes.
The renamed scheduling benchmark also passes a smoke run with both callback
group configurations for image and point-cloud messages: all 16 messages,
including warmup, pass content and ordering validation. This short smoke run
checks the benchmark refactor; the performance figures above remain from the
longer original comparison. Focused lint and `git diff --check` pass.

The added `std_msgs` test dependency supports the scheduling regression;
`sensor_msgs` supports the retained image/point-cloud scheduling benchmark.

## Reproduction

From the repository root in the supplied container:

```bash
source /opt/ros/jazzy/setup.bash
unset FASTRTPS_DEFAULT_PROFILES_FILE FASTDDS_DEFAULT_PROFILES_FILE ROS_SUPER_CLIENT
export ROS_DOMAIN_ID=183
export ROS_AUTOMATIC_DISCOVERY_RANGE=LOCALHOST
export RMW_IMPLEMENTATION=rmw_fastrtps_cpp
export PYTHONDONTWRITEBYTECODE=1

python3 -m pytest -q -p no:cacheprovider test/test_subscription_callback_group.py test/test_behavior.py
python3 -m tools.benchmark_subscription_scheduling --repeats 3 --duration 5 --rate-hz 2 --cases image cloud --event-gating both --output /tmp/executor-comparison.json
python3 -m tools.benchmark_subscription_scheduling --repeats 3 --duration 5 --rate-hz 30 --cases image cloud --event-gating enabled --output /tmp/executor-high-rate.json
```

`--event-gating disabled` selects the old callback group only inside the disposable
benchmark endpoint. `--event-gating both` alternates baseline and changed runs.

Local results are retained in the ignored `test-results/` directory:
`executor-comparison.json`, `executor-high-rate.json`,
and `executor-stress-object.log`. Historical JSON files include the removed raw
experiment; only object-subscription measurements are reported above.

Post-removal verification logs: `executor-fix-only-smoke.log` and
`executor-fix-only-stress.log` in the same ignored results directory.
