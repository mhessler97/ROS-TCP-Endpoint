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

"""Real-executor lifecycle stress for idle monitoring and prompt registration."""

import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import rclpy
from example_interfaces.action import Fibonacci
from rclpy.action import ActionServer, GoalResponse
from rclpy.callback_groups import ReentrantCallbackGroup
from rclpy.executors import ExternalShutdownException, MultiThreadedExecutor
from rclpy.node import Node
from rclpy.serialization import serialize_message
from rclpy.task import Future
from std_srvs.srv import Empty

from ros_tcp_endpoint.action_client import RosActionClient
from ros_tcp_endpoint.server import TcpServer
from ros_tcp_endpoint.unity_service import UnityService


def wait_for(predicate, timeout=10):
    """Wait for an asynchronous effect with a bounded deadline."""
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, 'Timed out waiting for test condition'
        time.sleep(.002)


@pytest.fixture
def running_endpoint():
    """Run the real endpoint executor and verify clean shutdown."""
    rclpy.init()
    endpoint = TcpServer('idle_executor_stress')
    errors = []

    def spin():
        try:
            endpoint.setup_executor()
        except ExternalShutdownException:
            pass
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=spin, daemon=True)
    thread.start()
    wait_for(lambda: endpoint.executor is not None)
    executor = endpoint.executor
    yield endpoint
    rclpy.shutdown()
    thread.join(3)
    executor.shutdown(timeout_sec=3)
    endpoint.destroy_node()
    assert not thread.is_alive(), 'Idle executor failed to shut down'
    assert not errors, errors


@pytest.mark.parametrize('busy_workers', [False, True])
def test_registration_wakes_idle_executor_with_busy_or_free_workers(
    running_endpoint, busy_workers
):
    """Adding and removing nodes must not wait for the one-second timeout."""
    endpoint = running_endpoint
    release = threading.Event()
    started = [threading.Event(), threading.Event()]
    nodes = []
    latencies = []

    def block(event):
        event.set()
        release.wait(15)

    try:
        if busy_workers:
            for event in started:
                endpoint.executor.create_task(block, event)
            wait_for(lambda: all(event.is_set() for event in started))
        for i in range(25):
            node = Node('mutation_stress_' + str(i))
            nodes.append(node)
            # Let the executor settle into its long wait before each mutation.
            time.sleep(.02)
            began = time.monotonic()
            endpoint.register_node(node)
            assert node in endpoint.executor.get_nodes()
            endpoint._mutate_executor(lambda executor: executor.remove_node(node))
            latencies.append(time.monotonic() - began)
            assert latencies[-1] < .4, latencies
        print('registration busy={} max_ms={:.2f}'.format(busy_workers, max(latencies)*1000))
    finally:
        release.set()
        for node in nodes:
            node.destroy_node()


def test_three_bursts_of_one_hundred_overlapping_goals(running_endpoint):
    """Preserve every result across idle periods and 80 simultaneous goals."""
    endpoint = running_endpoint
    helper = Node('action_burst_server')
    executor = MultiThreadedExecutor(num_threads=2)
    executor.add_node(helper)
    gates = {}
    accepted = []
    results = []
    feedback = []
    sender = SimpleNamespace(
        send_action_goal_response=lambda *args, **kwargs: accepted.append((args, kwargs)),
        send_action_result=lambda *args, **kwargs: results.append((args, kwargs)),
        send_action_feedback=lambda *args, **kwargs: feedback.append((args, kwargs)),
        send_unity_error=Mock(),
    )
    endpoint.unity_tcp_sender = sender
    endpoint.send_unity_error = Mock()

    async def execute(handle):
        await gates[handle.request.order]
        handle.publish_feedback(Fibonacci.Feedback(sequence=[handle.request.order]))
        handle.succeed()
        return Fibonacci.Result(sequence=[handle.request.order])

    action = ActionServer(
        helper, Fibonacci, '/action_burst', execute,
        goal_callback=lambda request: (
            GoalResponse.REJECT if request.order < 0 else GoalResponse.ACCEPT
        ),
        callback_group=ReentrantCallbackGroup(),
    )
    stopping = threading.Event()

    def spin():
        while not stopping.is_set():
            executor.spin_once(timeout_sec=.05)

    thread = threading.Thread(target=spin, daemon=True)
    thread.start()
    bridge = RosActionClient('/action_burst', Fibonacci, endpoint)
    endpoint.register_node(bridge)
    try:
        for cycle in range(3):
            gates.clear()
            gates.update((i, Future()) for i in range(100) if i % 5)
            assert bridge._server_health_timer.is_canceled()
            time.sleep(.2)
            for i in range(100):
                bridge.send_goal('goal-{}-{}'.format(cycle, i), serialize_message(
                    Fibonacci.Goal(order=-1 if i % 5 == 0 else i)), client_id=1)
                # Keep accepted goals overlapping without overflowing the default
                # ROS action service history (depth 10) with unaccepted requests.
                wait_for(lambda: len(accepted) == 100*cycle+i+1)
            wait_for(lambda: len(accepted) == 100*(cycle+1))
            assert len(bridge._pending_goals) == 80
            assert not bridge._inflight_goals
            assert not bridge._server_health_timer.is_canceled()
            # Multiple health checks while all 80 accepted goals remain pending.
            time.sleep(.25)
            assert len(results) == 80*cycle
            for completed, i in enumerate(gates, 1):
                gates[i].set_result(None)
                wait_for(lambda: len(results) == 80*cycle+completed)
            wait_for(lambda: bridge._server_health_timer.is_canceled())
            assert not bridge._pending_goals and not bridge._ros_goal_lookup
        assert len({args[1] for args, _ in accepted}) == 300
        assert len({args[1] for args, _ in results}) == 240
        assert sum(kwargs.get('message') == 'rejected' for _, kwargs in accepted) == 60
        assert all(args[2] == 4 for args, _ in results)
        for args, _ in results:
            assert list(args[3].sequence) == [int(args[1].rsplit('-', 1)[1])]
        wait_for(lambda: len(feedback) == 240)
    finally:
        print('action totals: accepted={}, results={}, feedback={}, pending={}'.format(
            len(accepted), len(results), len(feedback), len(bridge._pending_goals)
        ))
        for gate in gates.values():
            if not gate.done():
                gate.set_result(None)
        endpoint.unregister_node(bridge)
        stopping.set()
        thread.join(3)
        executor.shutdown(timeout_sec=3)
        action.destroy()
        helper.destroy_node()


def test_service_unregister_waits_for_response_transmission(running_endpoint):
    """Block ROS response transmission to reproduce the teardown race reliably."""
    endpoint = running_endpoint
    helper = Node('service_drain_client')
    executor = MultiThreadedExecutor(num_threads=2)
    executor.add_node(helper)
    release = threading.Event()
    entered = threading.Event()
    response_started = threading.Event()
    unregister_done = threading.Event()

    def service_reply(*args):
        entered.set()
        return Empty.Response()

    endpoint.send_unity_service = service_reply
    bridge = UnityService('/service_drain', Empty, endpoint)
    original_send_response = bridge.service.send_response

    def delayed_response(response, header):
        # Reproduce the race after our callback returns but before rclpy sends.
        response_started.set()
        assert release.wait(5)
        original_send_response(response, header)

    bridge.service.send_response = delayed_response
    endpoint.register_node(bridge)
    client = helper.create_client(Empty, '/service_drain')
    try:
        assert client.wait_for_service(timeout_sec=5)
        future = client.call_async(Empty.Request())
        assert entered.wait(5) and response_started.wait(5)

        def unregister():
            endpoint.unregister_node(bridge)
            unregister_done.set()

        unregister_thread = threading.Thread(target=unregister, daemon=True)
        unregister_thread.start()
        assert not unregister_done.wait(.2), 'Service destroyed before response was transmitted'
        release.set()
        deadline = time.monotonic() + 5
        while not future.done() and time.monotonic() < deadline:
            executor.spin_once(timeout_sec=.05)
        assert isinstance(future.result(), Empty.Response)
        assert unregister_done.wait(3)
        unregister_thread.join(3)
    finally:
        release.set()
        executor.shutdown(timeout_sec=3)
        helper.destroy_node()
