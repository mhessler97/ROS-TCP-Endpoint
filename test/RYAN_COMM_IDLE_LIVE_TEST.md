# Idle monitor candidate: live robot trial — 2026-10-20

## Deployment

User-authorized SSH test on `ryan-admin@130.253.185.161` (`ryan015`), container
`ryan-comm` (`389c3116b343`). Replaced only `action_client.py`, `server.py`, and
`unity_service.py` in the installed Python package. The previously deployed
Fast DDS subscription callback-group fix remains intact and its hashes were
verified. The three replaced files matched the expected pre-change source.

The container restarted at **22:36:43 UTC**. Endpoint host PID changed from
2248184 to **2424765**. Docker health returned to healthy. The face client
reconnected automatically. As in the previous trial, the tablet needed a process
restart: SIGTERM at 22:37:18 UTC let its watchdog replace PID 2264183 with
**2428749**, after which its registrations and startup action returned.

Before modifying production, the candidate passed all **60 focused/behavior
regression tests** in the robot's actual container (19.09 seconds), plus a full
TCP integration stress run. These tests used separate localhost-only ROS domains
184 and 185 and did not address production robot topics.

## Measured live result

Compared six ten-second baseline intervals with eighteen ten-second intervals
(181.7 seconds) after both clients were restored and the private test client had
disconnected. No synthetic probe ran during that steady sample.

| Measurement | Before | Candidate |
| --- | ---: | ---: |
| Mean endpoint CPU, % of one core | 73.54% | **3.20%** |
| CPU interval range | 73.15–73.97% | 2.77–3.47% |
| Median RSS | 131.83 MiB | 137.89 MiB |
| Connected TCP clients throughout | 2 | 2 |
| Mean TCP outgoing traffic | 24.51 B/s | 25.26 B/s |
| Mean TCP incoming traffic | 15.58 B/s | 15.67 B/s |

This is **95.64% less endpoint CPU** (roughly 0.70 core saved) for the current
light live application workload. The same bridge registrations and comparable
wire traffic were present on both sides. This is not a 95.64% reduction in total
robot CPU. RSS was constant throughout each measurement window; the candidate
measurement followed temporary functional-test registrations and was about
6 MiB higher. There is no demonstrated RAM saving.

The candidate remains installed, healthy, and running. These measurements are a
short live trial, not a long memory soak or proof across every conversation,
camera stream, and physical action. The earlier simultaneous-action burst limit
and connector half-close issue remain separate, unresolved findings.

## Live functional checks

The original graph was restored exactly: **15 bridge publishers, 11 subscribers,
and four action clients**, with the same node names and namespaces. A temporary
third TCP client exercised the actual production endpoint using unique
`/codex_endpoint_probe_1661cc0c/*` names and a mock action server. It did not send
movement or speech commands to hardware.

- 20 validated messages in each direction, with exact content checks.
- Combined TCP-to-ROS then ROS-to-TCP round trip: median **36.0 ms**, maximum
  **41.3 ms**, including the probe's polling interval.
- Three successful action goals with feedback/results across idle intervals,
  one deliberately rejected goal, and one successfully canceled goal.
- The temporary client disconnected and all its registrations were removed.
  Only the original two clients remained for the steady CPU comparison.
- `/tablet/available_scenes` returned actual scene metadata through the live
  Unity client in 2.34 seconds.

The initial `/avatar/get_face_hsv` query was invalid: the probe left
`avatar_material_part` empty. Unity's log showed `Unknown material part name`
in `AvatarController.HandleGetFaceHSVRequest`, explaining the endpoint's
five-second timeout and default response. The initial probe's overall `PASS`
line applies to its transport/action assertions, not successful handling of this
invalid service request. The probe was corrected to request material index 0,
part `lips`. Three corrected calls returned the actual nonzero HSV values
(hue 352.3256, saturation 53.3019, value 84.9057), with latencies 2,233 ms,
48 ms, and 82 ms. The first-call delay was not compared against the previous
version and should not be presented as a latency improvement. No further timeout
was observed for the corrected requests.

Startup emitted one recoverable InvalidHandle warning during duplicate
registration and an empty/unknown cancellation from the tablet. Similar startup
warnings occurred in the previous trial. The deliberately rejected test goal
and temporary TCP-client closure also logged expected errors. The invalid HSV
request logged an additional timeout; this was a probe-input error, not evidence
of an endpoint scheduling regression.

## Rollback and persistence

The full original installed package, the three deployed candidate files, tests,
and rollback script are retained at:

```
/home/ryan-admin/ros-tcp-idle-backup-20261020.LpUHog/
```

Execute `rollback.sh` there to restore the three original files and restart
`ryan-comm`, retaining the earlier Fast DDS subscription fix. The patch is in the
existing container's writable layer: it survives restarting this container but
must be incorporated into an image rebuild to survive container replacement.

## Evidence

Local ignored `test-results/` contains:

- `ryan-idle-isolated-tests.log`, `ryan-idle-isolated-integration.log`
- `ryan-idle-live-before.jsonl`, `ryan-idle-live-startup.jsonl`,
  `ryan-idle-live-steady.jsonl`
- `ryan-idle-graph-before.json`, `ryan-idle-graph-after.json`
- `ryan-idle-live-probe.log`, `ryan-idle-live-endpoint.log`
- `ryan-idle-face-query-error.log` and the corrected service-query results
- `ryan-idle-live-summary.json` and `ryan-idle-final-state.txt`
- Reusable monitoring, graph inspection, and live-probe scripts
