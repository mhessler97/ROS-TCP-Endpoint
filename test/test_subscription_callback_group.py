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

"""Exercise queued subscriptions and real QoS events with a saturated executor."""

import threading
import time
from types import SimpleNamespace

import pytest
import rclpy
from rclpy.executors import MultiThreadedExecutor
from rclpy.event_handler import QoSSubscriptionEventType
from rclpy.node import Node
from rclpy.qos import QoSPolicyKind, QoSProfile, QoSReliabilityPolicy
from rclpy.utilities import get_rmw_implementation_identifier
from std_msgs.msg import String

from ros_tcp_endpoint.subscriber import RosSubscriber
from ros_tcp_endpoint.subscription_callback_group import SubscriptionCallbackGroup


def test_queued_subscription_waits_without_spinning_and_preserves_qos_events(monkeypatch):
    """Avoid empty Fast DDS wakeups while preserving data and incompatible-QoS events."""
    if get_rmw_implementation_identifier() not in (
        "rmw_fastrtps_cpp", "rmw_fastrtps_dynamic_cpp"
    ):
        pytest.skip("Regression specifically exercises Fast DDS subscription event conditions")

    rclpy.init()
    publisher_node = Node("queued_subscription_publisher")
    executor = MultiThreadedExecutor(num_threads=2)
    release_workers = threading.Event()
    entered = [threading.Event(), threading.Event()]
    forwarded = []
    qos_events = []
    subscriber = None
    try:
        server = SimpleNamespace(
            send_unity_message=lambda topic, message, client_id: forwarded.append(message)
        )
        subscriber = RosSubscriber(
            "/queued_subscription_test", String, server, client_id=1
        )
        subscription = subscriber.subscription
        assert isinstance(subscription.callback_group, SubscriptionCallbackGroup)
        for handler in subscription.event_handlers:
            if handler.event_type == (
                QoSSubscriptionEventType.RCL_SUBSCRIPTION_REQUESTED_INCOMPATIBLE_QOS
            ):
                handler.callback = qos_events.append
                break
        else:
            pytest.fail("The incompatible-QoS diagnostic handler must remain registered")
        executor.add_node(subscriber)
        publisher = publisher_node.create_publisher(String, "/queued_subscription_test", 10)
        deadline = time.monotonic() + 10
        while publisher.get_subscription_count() != 1 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert publisher.get_subscription_count() == 1

        def occupy_worker(started):
            started.set()
            release_workers.wait(timeout=10)

        for started in entered:
            executor.create_task(occupy_worker, started)
        deadline = time.monotonic() + 5
        while not all(started.is_set() for started in entered) and time.monotonic() < deadline:
            executor.spin_once(timeout_sec=0.01)
        assert all(started.is_set() for started in entered)

        publisher.publish(String(data="queued-data"))
        deadline = time.monotonic() + 5
        while not subscription._executor_event and time.monotonic() < deadline:
            executor.spin_once(timeout_sec=0.01)
        assert subscription._executor_event
        assert forwarded == []

        # Add an incompatible peer while the data callback is queued. Its warning
        # must be deferred, not disabled or lost when the workers resume.
        publisher_node.create_publisher(
            String, "/queued_subscription_test",
            QoSProfile(depth=10, reliability=QoSReliabilityPolicy.BEST_EFFORT),
        )
        import rclpy.executors as executor_module
        original_wait_set = executor_module._rclpy.WaitSet
        waits = []

        class CountingWaitSet(original_wait_set):
            def wait(self, timeout):
                waits.append(timeout)
                return super().wait(timeout)

        with monkeypatch.context() as scoped_patch:
            scoped_patch.setattr(executor_module._rclpy, "WaitSet", CountingWaitSet)
            for _ in range(3):
                executor.spin_once(timeout_sec=0.05)
        # Unpatched Fast DDS executes hundreds/thousands of empty waits here.
        assert len(waits) <= 10, "Queued data caused repeated empty middleware wakeups"
        assert qos_events == []

        release_workers.set()
        deadline = time.monotonic() + 5
        while (not forwarded or not qos_events) and time.monotonic() < deadline:
            executor.spin_once(timeout_sec=0.05)
        assert len(forwarded) == 1
        assert forwarded[0].data == "queued-data"
        assert qos_events
        assert qos_events[0].last_policy_kind == QoSPolicyKind.RELIABILITY
    finally:
        release_workers.set()
        executor.shutdown(timeout_sec=5)
        if subscriber is not None:
            subscriber.unregister()
        publisher_node.destroy_node()
        rclpy.shutdown()
