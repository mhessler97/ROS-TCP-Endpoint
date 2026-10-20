# Copyright 2026 Unity Technologies
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Compare Fast DDS subscription scheduling through an isolated TCP endpoint.

Run from the repository root in a sourced ROS environment:
    python3 -m tools.benchmark_subscription_scheduling --repeats 3

The publisher and validating TCP receiver run outside the measured endpoint
process. Each sample starts a fresh endpoint, warms up, then publishes at a fixed
rate. CPU includes all endpoint threads; peak RSS includes startup and warmup.
No Unity application is involved: the receiver validates the TCP wire messages
using ROS deserialization. Linux is required for resident-memory measurements.
"""

import argparse
import cProfile
import json
import multiprocessing
import os
from pathlib import Path
import queue
import resource
import socket
import statistics
import struct
import threading
import time
from unittest.mock import patch

import rclpy
from rclpy.node import Node
from rclpy.executors import ExternalShutdownException, ShutdownException
from rclpy.serialization import deserialize_message
from sensor_msgs.msg import Image, PointCloud2, PointField
from std_msgs.msg import String

from ros_tcp_endpoint import TcpServer
from ros_tcp_endpoint.subscriber import RosSubscriber


def rss_mib():
    """Read current Linux process resident memory, including native ROS memory."""
    pages = int(Path("/proc/self/statm").read_text().split()[1])
    return pages * os.sysconf("SC_PAGE_SIZE") / (1024 * 1024)


def endpoint_worker(control, port, event_gating=True):
    """Run the production endpoint with the requested subscription experiment."""
    original = RosSubscriber.create_subscription

    def create_subscription(self, *args, **kwargs):
        if not event_gating:
            kwargs["callback_group"] = self.default_callback_group
        return original(self, *args, **kwargs)

    with patch.object(RosSubscriber, "create_subscription", create_subscription):
        rclpy.init()
        rclpy.logging.set_logger_level(
            "scheduling_benchmark_endpoint", rclpy.logging.LoggingSeverity.WARN
        )
        server = TcpServer("scheduling_benchmark_endpoint", tcp_ip="127.0.0.1", tcp_port=port)
        server.start()

        def spin():
            profile_path = os.environ.get("ROS_TCP_BENCHMARK_PROFILE")
            profiler = cProfile.Profile() if profile_path else None
            try:
                if profiler is not None:
                    profiler.runcall(server.setup_executor)
                else:
                    server.setup_executor()
            except (ExternalShutdownException, ShutdownException):
                pass
            finally:
                if profiler is not None:
                    profiler.dump_stats("{}-{}.prof".format(
                        profile_path, "enabled" if event_gating else "disabled"
                    ))

        spinning = threading.Thread(target=spin, daemon=True)
        spinning.start()
        control.send("ready")
        while True:
            command = control.recv()
            if command == "start":
                baseline_rss = rss_mib()
                baseline_cpu = time.process_time()
                control.send("started")
            elif command == "stop":
                cpu = time.process_time() - baseline_cpu
                control.send({
                    "cpu_seconds": cpu,
                    "rss_after_warmup_mib": baseline_rss,
                    "rss_end_mib": rss_mib(),
                    "peak_rss_mib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
                })
            elif command == "quit":
                executor = server.executor
                server.destroy_nodes()
                rclpy.shutdown()
                spinning.join(timeout=5)
                if executor is not None:
                    executor.shutdown(timeout_sec=5)
                return


def make_message(case):
    """Return a representative message, its protocol type name, and test rate."""
    if case == "image":
        return Image(
            height=720, width=1280, encoding="rgb8", step=1280 * 3,
            data=bytes(range(256)) * 10800,
        ), "sensor_msgs/Image", 10
    if case == "cloud":
        return PointCloud2(
            height=1, width=65536,
            fields=[
                PointField(name=name, offset=offset, datatype=PointField.FLOAT32, count=1)
                for name, offset in (("x", 0), ("y", 4), ("z", 8), ("intensity", 12))
            ],
            point_step=16, row_step=65536 * 16,
            data=struct.pack("<ffff", 1.0, 2.0, 3.0, 0.5) * 65536,
            is_dense=True,
        ), "sensor_msgs/PointCloud2", 30
    return String(data="x" * 256), "std_msgs/String", 200


def set_sequence(message, sequence):
    """Embed a sequence number so losses, duplicates, and reordering fail validation."""
    if isinstance(message, String):
        message.data = "{:08d}:{}".format(sequence, "x" * 256)
    else:
        message.header.stamp.sec = sequence


def receive_frame(stream):
    """Read one complete frame from a buffered TCP stream."""
    def exact(size):
        data = stream.read(size)
        if len(data) != size:
            raise EOFError("TCP endpoint closed before a complete frame arrived")
        return data

    name = exact(struct.unpack("<I", exact(4))[0]).decode("utf-8")
    payload = exact(struct.unpack("<I", exact(4))[0])
    return name, payload


def run_sample(case, duration, rate_override=None, event_gating=True):
    """Measure matched traffic in a fresh process and validate every received message."""
    message, type_name, rate = make_message(case)
    rate = rate_override or rate
    expected, _, _ = make_message(case)
    warmup = rate
    measured = round(duration * rate)
    total = warmup + measured
    context = multiprocessing.get_context("spawn")
    control, child_control = context.Pipe()
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    process = context.Process(
        target=endpoint_worker, args=(child_control, port, event_gating)
    )
    process.start()
    child_control.close()
    connection = None
    stream = None
    node = None
    reader = None
    try:
        if not control.poll(20) or control.recv() != "ready":
            raise RuntimeError("Endpoint failed to initialize")
        deadline = time.monotonic() + 10
        while connection is None:
            try:
                connection = socket.create_connection(("127.0.0.1", port), timeout=10)
            except ConnectionRefusedError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)
        stream = connection.makefile("rb")
        assert receive_frame(stream)[0] == "__handshake"
        topic = "/scheduling_benchmark"
        command = b"__subscribe"
        params = json.dumps({"topic": topic, "message_name": type_name}).encode()
        connection.sendall(
            struct.pack("<I", len(command)) + command
            + struct.pack("<I", len(params)) + params
        )
        rclpy.init()
        node = Node("scheduling_benchmark_publisher")
        publisher = node.create_publisher(type(message), topic, 10)
        deadline = time.monotonic() + 10
        while publisher.get_subscription_count() != 1:
            if time.monotonic() >= deadline:
                raise TimeoutError("ROS publisher and bridge did not discover each other")
            time.sleep(0.01)
        time.sleep(0.25)

        sent_at = {}
        latencies = []
        received = queue.Queue()
        errors = queue.Queue()

        def consume():
            try:
                for sequence in range(total):
                    destination, payload = receive_frame(stream)
                    arrived = time.perf_counter()
                    assert destination == topic, destination
                    decoded = deserialize_message(payload, type(message))
                    set_sequence(expected, sequence)
                    if decoded != expected:
                        changed = [
                            field for field in expected.get_fields_and_field_types()
                            if getattr(decoded, field) != getattr(expected, field)
                        ]
                        actual_sequence = (
                            decoded.data[:8] if isinstance(decoded, String)
                            else decoded.header.stamp.sec
                        )
                        raise AssertionError(
                            "Expected sequence {}, received {}; changed fields: {}".format(
                                sequence, actual_sequence, changed
                            )
                        )
                    if sequence >= warmup:
                        latencies.append((arrived - sent_at[sequence]) * 1000)
                    received.put(sequence)
            except Exception as exc:
                errors.put(exc)

        reader = threading.Thread(target=consume, daemon=True)
        reader.start()

        def publish_range(start, stop):
            started = time.perf_counter()
            for sequence in range(start, stop):
                wait = started + (sequence - start) / rate - time.perf_counter()
                if wait > 0:
                    time.sleep(wait)
                if not errors.empty():
                    raise errors.get()
                set_sequence(message, sequence)
                sent_at[sequence] = time.perf_counter()
                publisher.publish(message)
            for sequence in range(start, stop):
                try:
                    actual = received.get(timeout=10)
                except queue.Empty:
                    if not errors.empty():
                        raise errors.get()
                    raise TimeoutError("Missing forwarded message {}".format(sequence)) from None
                assert actual == sequence

        publish_range(0, warmup)
        control.send("start")
        assert control.poll(10) and control.recv() == "started"
        started = time.perf_counter()
        publish_range(warmup, total)
        elapsed = time.perf_counter() - started
        control.send("stop")
        if not control.poll(10):
            raise TimeoutError("Endpoint did not return measurement results")
        stats = control.recv()
        ordered = sorted(latencies)
        return {
            "case": case, "rate_hz": rate,
            "subscription_event_gating": event_gating,
            "measured_messages": measured, "validated_messages": total,
            "elapsed_seconds": elapsed,
            "cpu_ms_per_message": stats["cpu_seconds"] * 1000 / measured,
            "cpu_percent_one_core": stats["cpu_seconds"] * 100 / elapsed,
            "latency_p50_ms": statistics.median(latencies),
            "latency_p99_ms": ordered[min(len(ordered) - 1, int(len(ordered) * 0.99))],
            **stats,
        }
    finally:
        if connection is not None:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()
        if reader is not None:
            reader.join(timeout=2)
        if stream is not None:
            stream.close()
        if node is not None:
            node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()
        if process.is_alive():
            control.send("quit")
            process.join(timeout=10)
        if process.is_alive():
            process.terminate()
            process.join(timeout=3)
        control.close()


def main():
    """Run paired samples with alternating order and emit reproducible JSON results."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--duration", type=float, default=5.0)
    parser.add_argument("--rate-hz", type=int, help="Override the per-message-type publish rate")
    parser.add_argument("--event-gating", choices=("enabled", "disabled", "both"),
                        default="enabled", help="Compare Fast DDS event scheduling workaround")
    parser.add_argument("--cases", nargs="+", choices=("image", "cloud", "string"),
                        default=["image", "cloud", "string"])
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.repeats < 1 or args.duration < 1:
        parser.error("repeats must be positive and duration must be at least one second")
    if args.rate_hz is not None and args.rate_hz < 1:
        parser.error("rate-hz must be positive")
    results = []
    for case in args.cases:
        for repeat in range(args.repeats):
            gating = [args.event_gating != "disabled"]
            if args.event_gating == "both":
                gating = [False, True] if repeat % 2 == 0 else [True, False]
            for event_gating in gating:
                result = run_sample(case, args.duration, args.rate_hz, event_gating)
                result["repeat"] = repeat + 1
                results.append(result)
                print(json.dumps(result), flush=True)
                if args.output is not None:
                    args.output.write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
