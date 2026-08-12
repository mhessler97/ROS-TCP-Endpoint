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

from collections import deque
import queue
import threading
import time
from types import SimpleNamespace

import pytest
import rclpy
from action_msgs.msg import GoalStatus
from example_interfaces.action import Fibonacci
from rclpy.executors import MultiThreadedExecutor, SingleThreadedExecutor
from rclpy.node import Node
from rclpy.qos import (
    QoSDurabilityPolicy,
    QoSHistoryPolicy,
    QoSReliabilityPolicy,
)
from rclpy.serialization import serialize_message
from rclpy.task import Future
from std_msgs.msg import Empty as EmptyMessage
from std_srvs.srv import Empty, SetBool

from ros_tcp_endpoint.communication import (
    RosReceiver,
    RosSender,
    deserialize_ros_message,
    deserialize_service_message,
    payload_is_effectively_empty,
)
from ros_tcp_endpoint.client import ClientThread
from ros_tcp_endpoint.qos import make_qos_profile
from ros_tcp_endpoint.service import RosService
from ros_tcp_endpoint.server import SysCommands
from ros_tcp_endpoint.tcp_sender import UnityTcpSender
from ros_tcp_endpoint.unity_action import UnityActionServer


class RecordingLogger:
    def __init__(self):
        self.errors = []
        self.warnings = []

    def error(self, message):
        self.errors.append(message)

    def warning(self, message):
        self.warnings.append(message)

    def info(self, message):
        pass


def test_dynamic_bridge_nodes_do_not_create_auxiliary_ros_services():
    rclpy.init()
    sender = RosSender("lean_sender_test")
    receiver = RosReceiver("lean_receiver_test")

    try:
        assert list(sender.services) == []
        assert list(receiver.services) == []
        assert sender.get_parameter("start_type_description_service").value is False
        assert receiver.get_parameter("start_type_description_service").value is False
    finally:
        sender.destroy_node()
        receiver.destroy_node()
        rclpy.shutdown()


class FakeGoalHandle:
    def __init__(self):
        self.goal_id = SimpleNamespace(uuid=bytes(range(16)))
        self.request = Fibonacci.Goal(order=5)
        self.succeeded = False
        self.aborted = False
        self.canceled_state = False

    def succeed(self):
        self.succeeded = True

    def abort(self):
        self.aborted = True

    def canceled(self):
        self.canceled_state = True

    def publish_feedback(self, feedback):
        pass


def make_uninitialized_action_server(result_timeout_sec=0.5):
    server = object.__new__(UnityActionServer)
    server.action_name = "/unity_fibonacci"
    server.action_type = Fibonacci
    server._goal_handles = {}
    server._result_futures = {}
    server._result_timers = {}
    server._lock = threading.Lock()
    server._condition = threading.Condition(server._lock)
    server._accepted_goals_awaiting_execution = 0
    server._closing = False
    server._shutdown_prepared = False
    server.result_timeout_sec = result_timeout_sec
    logger = RecordingLogger()
    server.get_logger = lambda: logger
    return server, logger


def run_rclpy_coroutine(coroutine, timeout_sec=2.0):
    rclpy.init()
    executor = SingleThreadedExecutor()
    try:
        task = executor.create_task(coroutine)
        executor.spin_until_future_complete(task, timeout_sec=timeout_sec)
        assert task.done()
        return task.result()
    finally:
        executor.shutdown()
        rclpy.shutdown()


def test_empty_service_request_and_response_types_accept_empty_payloads():
    for payload in (b"", b"\x00\x01\x00\x00"):
        assert isinstance(
            deserialize_service_message(payload, Empty.Request), Empty.Request
        )
        assert isinstance(
            deserialize_service_message(payload, Empty.Response), Empty.Response
        )

    request = Empty.Request()
    response = Empty.Response()
    assert isinstance(
        deserialize_service_message(serialize_message(request), Empty.Request), Empty.Request
    )
    assert isinstance(
        deserialize_service_message(serialize_message(response), Empty.Response), Empty.Response
    )


@pytest.mark.parametrize("pending_is_request", [True, False])
def test_syscommand_between_service_header_and_payload_preserves_pending_state(
    pending_is_request,
):
    handled_commands = []
    service_requests = []
    service_responses = []
    published_messages = []
    ros_service = object()
    unity_service = object()
    publisher = SimpleNamespace(send=lambda data: published_messages.append(data))
    server = SimpleNamespace(
        handle_syscommand=lambda destination, data, client_thread: handled_commands.append(
            (destination, data, client_thread)
        ),
        send_unity_service_response=lambda srv_id, data, client_id: service_responses.append(
            (srv_id, data, client_id)
        ),
        ros_services_table={"/empty_ros": ros_service},
        ros_service_clients={"/empty_ros": 7},
        unity_services_table={"/empty_ros": unity_service},
        unity_service_clients={"/empty_ros": 7},
        publishers_table={"/interleaved_topic": publisher},
        publisher_clients={"/interleaved_topic": 7},
        get_owned_registration=lambda table, owners, destination, client_id: (
            table.get(destination) if owners.get(destination) == client_id else None
        ),
    )
    client = object.__new__(ClientThread)
    client.tcp_server = server
    client.client_id = 7
    client.pending_srv_id = 42
    client.pending_srv_is_request = pending_is_request
    client.pending_action = None
    client.pending_payload_deadline = 123.0
    client.deferred_payload_headers = deque()
    client.send_ros_service_request = (
        lambda srv_id, destination, data: service_requests.append(
            (srv_id, destination, data)
        )
    )

    client.process_frame("__topic_list", b"{}")

    assert handled_commands == [("__topic_list", b"{}", client)]
    assert client.pending_srv_id == 42
    assert client.pending_payload_deadline == 123.0

    client.process_frame("/interleaved_topic", b"topic-data")

    assert published_messages == [b"topic-data"]
    assert client.pending_srv_id == 42
    assert client.pending_payload_deadline == 123.0

    client.process_frame("/empty_ros", b"")

    assert client.pending_srv_id is None
    assert client.pending_payload_deadline is None
    if pending_is_request:
        assert service_requests == [(42, "/empty_ros", b"")]
        assert service_responses == []
    else:
        assert service_requests == []
        assert service_responses == [(42, b"", 7)]


def test_keepalive_and_syscommand_do_not_consume_pending_action_payload():
    handled_commands = []
    server = SimpleNamespace(
        handle_syscommand=lambda destination, data, client_thread: handled_commands.append(
            (destination, data, client_thread)
        )
    )
    client = object.__new__(ClientThread)
    client.tcp_server = server
    client.client_id = 8
    client.pending_srv_id = None
    client.pending_srv_is_request = False
    client.pending_action = {"action_name": "/example", "phase": "goal_to_ros"}
    client.pending_payload_deadline = 456.0
    client.deferred_payload_headers = deque()

    client.process_frame("", b"")
    client.process_frame("__topic_list", b"{}")

    assert handled_commands == [("__topic_list", b"{}", client)]
    assert client.pending_action == {
        "action_name": "/example",
        "phase": "goal_to_ros",
    }
    assert client.pending_payload_deadline == 456.0


def test_second_payload_header_is_deferred_until_first_payload_arrives():
    activated_headers = []
    service_requests = []
    server = SimpleNamespace()
    client = object.__new__(ClientThread)
    client.tcp_server = server
    client.client_id = 9
    client.pending_srv_id = 41
    client.pending_srv_is_request = True
    client.pending_action = None
    client.pending_payload_deadline = 100.0
    client.pending_payload_timeout_sec = 5.0
    client.deferred_payload_headers = deque()
    client.send_ros_service_request = (
        lambda srv_id, destination, data: service_requests.append(
            (srv_id, destination, data)
        )
    )

    def handle_syscommand(destination, data, client_thread):
        activated_headers.append((destination, data))
        client_thread.set_pending_service(42, is_request=True)

    server.handle_syscommand = handle_syscommand
    server.ros_services_table = {"/empty_ros": object()}
    server.ros_service_clients = {"/empty_ros": 9}
    server.unity_services_table = {}
    server.unity_service_clients = {}
    server.publishers_table = {}
    server.publisher_clients = {}
    server.get_owned_registration = lambda table, owners, destination, client_id: (
        table.get(destination) if owners.get(destination) == client_id else None
    )

    client.process_frame("__request", b'{"srv_id": 42}')

    assert activated_headers == []
    assert list(client.deferred_payload_headers) == [
        ("__request", b'{"srv_id": 42}')
    ]
    assert client.pending_srv_id == 41

    client.process_frame("/empty_ros", b"first")

    assert service_requests == [(41, "/empty_ros", b"first")]
    assert activated_headers == [("__request", b'{"srv_id": 42}')]
    assert client.pending_srv_id == 42

    client.process_frame("/empty_ros", b"second")

    assert service_requests == [
        (41, "/empty_ros", b"first"),
        (42, "/empty_ros", b"second"),
    ]
    assert client.pending_srv_id is None


def test_legacy_topic_qos_defaults_are_preserved():
    profile = make_qos_profile(queue_size=17)

    assert profile.depth == 17
    assert profile.history == QoSHistoryPolicy.KEEP_LAST
    assert profile.reliability == QoSReliabilityPolicy.RELIABLE
    assert profile.durability == QoSDurabilityPolicy.VOLATILE


def test_legacy_latch_enables_transient_local_durability():
    profile = make_qos_profile(queue_size=4, latch=True)

    assert profile.depth == 4
    assert profile.durability == QoSDurabilityPolicy.TRANSIENT_LOCAL


def test_topic_qos_presets_and_overrides_are_supported():
    sensor_profile = make_qos_profile(qos="sensor-data")
    assert sensor_profile.reliability == QoSReliabilityPolicy.BEST_EFFORT
    assert sensor_profile.durability == QoSDurabilityPolicy.VOLATILE

    custom_profile = make_qos_profile(
        queue_size=10,
        qos={
            "preset": "transient_local",
            "history": "keep_all",
            "depth": 23,
        },
    )
    assert custom_profile.depth == 23
    assert custom_profile.history == QoSHistoryPolicy.KEEP_ALL
    assert custom_profile.reliability == QoSReliabilityPolicy.RELIABLE
    assert custom_profile.durability == QoSDurabilityPolicy.TRANSIENT_LOCAL


def test_latch_wins_over_conflicting_explicit_durability():
    profile = make_qos_profile(
        latch=True,
        qos={"durability": "volatile"},
    )
    assert profile.durability == QoSDurabilityPolicy.TRANSIENT_LOCAL


@pytest.mark.parametrize(
    "qos",
    [
        "unknown",
        {"reliability": "sometimes"},
        {"durability": "forever"},
        {"history": "everything_recent"},
        {"depth": 0},
        {"unexpected": True},
    ],
)
def test_invalid_topic_qos_is_rejected(qos):
    with pytest.raises(ValueError):
        make_qos_profile(qos=qos)


@pytest.mark.parametrize(
    "representation_identifier",
    [0x0000, 0x0001, 0x0002, 0x0003, 0x0006, 0x0007, 0x0008, 0x0009, 0x000A, 0x000B],
)
def test_all_header_only_cdr_encodings_are_accepted_for_fieldless_messages(
    representation_identifier,
):
    payload = representation_identifier.to_bytes(2, byteorder="big") + b"\x00\x00"
    assert payload_is_effectively_empty(payload)
    assert isinstance(deserialize_ros_message(payload, EmptyMessage), EmptyMessage)


@pytest.mark.parametrize(
    "payload",
    [b"\x00", b"\x00\x01", b"\x00\x01\x00", b"random"],
)
def test_truncated_or_invalid_empty_message_encodings_are_rejected(payload):
    assert not payload_is_effectively_empty(payload)
    with pytest.raises(Exception):
        deserialize_ros_message(payload, EmptyMessage)


def test_header_only_cdr_is_rejected_for_nonempty_message_types():
    with pytest.raises(Exception):
        deserialize_ros_message(b"\x00\x01\x00\x00", SetBool.Request)


def test_installed_fieldless_action_sections_accept_header_only_cdr():
    twist_mux_actions = pytest.importorskip("twist_mux_msgs.action")
    action_type = twist_mux_actions.JoyPriority
    payload = b"\x00\x01\x00\x00"

    assert isinstance(deserialize_ros_message(payload, action_type.Goal), action_type.Goal)
    assert isinstance(
        deserialize_ros_message(payload, action_type.Result), action_type.Result
    )
    assert isinstance(
        deserialize_ros_message(payload, action_type.Feedback), action_type.Feedback
    )


def test_nonempty_service_type_rejects_an_empty_payload():
    with pytest.raises(Exception):
        deserialize_service_message(b"", SetBool.Request)


def test_empty_service_type_rejects_malformed_nonempty_payload():
    with pytest.raises(Exception):
        deserialize_service_message(b"not-cdr", Empty.Request)


def test_ros_service_bridge_calls_empty_request_service():
    rclpy.init()
    service_node = Node("empty_service_test_server")
    service_node.create_service(Empty, "/empty_bridge_test", lambda request, response: response)
    bridge = RosService("/empty_bridge_test", Empty)
    executor = MultiThreadedExecutor(num_threads=2)
    executor.add_node(service_node)
    executor.add_node(bridge)
    spin_thread = threading.Thread(target=executor.spin, daemon=True)
    spin_thread.start()

    try:
        valid_response = bridge.send(serialize_message(Empty.Request()))
        zero_length_response = bridge.send(b"")
        assert isinstance(valid_response, Empty.Response)
        assert isinstance(zero_length_response, Empty.Response)
    finally:
        executor.shutdown()
        spin_thread.join(timeout=1.0)
        bridge.destroy_node()
        service_node.destroy_node()
        rclpy.shutdown()


@pytest.mark.parametrize("response_payload", [b"", serialize_message(Empty.Response())])
def test_unity_service_accepts_empty_response_type(response_payload):
    tcp_server = SimpleNamespace(
        _state_lock=threading.RLock(),
        unity_service_clients={"/empty": 7},
        logerr=lambda message: None,
        logwarn=lambda message: None,
    )
    sender = UnityTcpSender(tcp_server)

    def respond_immediately(payload, client_id=None):
        srv_id = next(iter(sender.services_waiting))
        sender.send_unity_service_response(srv_id, response_payload, client_id=client_id)
        return True

    sender._enqueue = respond_immediately
    result = sender.send_unity_service_request(
        "/empty", Empty, Empty.Request(), client_id=7
    )

    assert isinstance(result, Empty.Response)


def test_unity_action_result_resumes_rclpy_future_from_tcp_thread():
    server, logger = make_uninitialized_action_server()
    goal_handle = FakeGoalHandle()

    class Sender:
        def send_unity_action_goal_request(self, action_name, goal_id, goal_request):
            def complete_goal():
                time.sleep(0.02)
                result = Fibonacci.Result(sequence=[0, 1, 1, 2, 3])
                server.handle_unity_result(
                    goal_id,
                    GoalStatus.STATUS_SUCCEEDED,
                    serialize_message(result),
                )

            threading.Thread(target=complete_goal, daemon=True).start()

    server.tcp_server = SimpleNamespace(unity_tcp_sender=Sender())
    result = run_rclpy_coroutine(server._execute_callback(goal_handle))

    assert list(result.sequence) == [0, 1, 1, 2, 3]
    assert goal_handle.succeeded
    assert not goal_handle.aborted
    assert logger.errors == []


def test_unity_action_timeout_aborts_and_returns_default_result():
    server, logger = make_uninitialized_action_server(result_timeout_sec=0.02)
    goal_handle = FakeGoalHandle()
    server.tcp_server = SimpleNamespace(
        unity_tcp_sender=SimpleNamespace(
            send_unity_action_goal_request=lambda action_name, goal_id, request: None
        )
    )

    result = run_rclpy_coroutine(server._execute_callback(goal_handle))

    assert isinstance(result, Fibonacci.Result)
    assert goal_handle.aborted
    assert logger.errors


def test_unity_action_prepare_unregister_drains_pending_result():
    server, logger = make_uninitialized_action_server()
    goal_handle = FakeGoalHandle()
    goal_id = server._ros_uuid_to_str(goal_handle.goal_id)
    result_future = Future()
    internal_result_future = Future()
    result_future.add_done_callback(
        lambda future: internal_result_future.set_result(future.result())
    )
    server._goal_handles[goal_id] = goal_handle
    server._result_futures[goal_id] = result_future
    server.action_server = SimpleNamespace(
        _result_futures={bytes(goal_handle.goal_id.uuid): internal_result_future}
    )

    server.prepare_unregister(timeout_sec=0.5)

    assert goal_handle.aborted
    assert isinstance(result_future.result(), Fibonacci.Result)
    assert internal_result_future.done()
    assert server._closing
    assert server._shutdown_prepared
    assert logger.warnings == []


class FakeTcpServer:
    def __init__(self, client_id):
        self._state_lock = threading.RLock()
        self._executor_lock = threading.RLock()
        self._registration_lock = threading.RLock()
        self.executor = None
        self._client_context = threading.local()
        self.set_active_client(client_id)
        self.subscribers_table = {}
        self.subscriber_clients = {}
        self.errors = []
        self.unregistered = []

    def get_active_client(self):
        return getattr(self._client_context, "client", None)

    def set_active_client(self, client_id):
        self._client_context.client = SimpleNamespace(client_id=client_id)

    def send_unity_error(self, message, client_id=None):
        self.errors.append((client_id, message))

    def unregister_node(self, node):
        self.unregistered.append(node)

    def register_node(self, node):
        pass

    def client_owns(self, owner_table, key, client_id):
        with self._state_lock:
            return owner_table.get(key) == client_id

    def loginfo(self, message):
        pass

    def logwarn(self, message):
        pass

    def logerr(self, message):
        pass


def test_subscribe_command_forwards_optional_qos_without_changing_legacy_api(
    monkeypatch,
):
    tcp_server = FakeTcpServer(client_id=1)
    commands = SysCommands(tcp_server)
    commands.resolve_message_name = lambda name, extension_hint=None: object
    captured = {}

    def make_subscriber(topic, message_class, server, **kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr("ros_tcp_endpoint.server.RosSubscriber", make_subscriber)
    commands.subscribe(
        "/qos",
        "std_msgs/String",
        queue_size=7,
        qos={"reliability": "best_effort", "durability": "transient_local"},
    )

    assert captured == {
        "queue_size": 7,
        "latch": False,
        "qos": {
            "reliability": "best_effort",
            "durability": "transient_local",
        },
        "client_id": 1,
    }


def test_subscribe_command_forwards_legacy_latch_flag(monkeypatch):
    tcp_server = FakeTcpServer(client_id=1)
    commands = SysCommands(tcp_server)
    commands.resolve_message_name = lambda name, extension_hint=None: object
    captured = {}

    def make_subscriber(topic, message_class, server, **kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr("ros_tcp_endpoint.server.RosSubscriber", make_subscriber)
    commands.subscribe("/latched", "std_msgs/String", latch=True)

    assert captured == {
        "queue_size": 10,
        "latch": True,
        "qos": None,
        "client_id": 1,
    }


def test_topic_registration_is_independent_for_multiple_clients(monkeypatch):
    tcp_server = FakeTcpServer(client_id=1)
    commands = SysCommands(tcp_server)
    commands.resolve_message_name = lambda name, extension_hint=None: object
    monkeypatch.setattr(
        "ros_tcp_endpoint.server.RosSubscriber",
        lambda topic, message_class, server, **kwargs: SimpleNamespace(
            created_by=kwargs["client_id"]
        ),
    )

    commands.subscribe("/shared", "std_msgs/String")
    tcp_server.set_active_client(2)
    commands.subscribe("/shared", "std_msgs/String")

    assert tcp_server.subscriber_clients["/shared"] == {1, 2}
    assert set(tcp_server.subscribers_table["/shared"]) == {1, 2}
    assert tcp_server.subscribers_table["/shared"][1].created_by == 1
    assert tcp_server.subscribers_table["/shared"][2].created_by == 2
    assert tcp_server.errors == []

    commands.remove_subscriber("/shared")

    assert tcp_server.subscriber_clients["/shared"] == {1}
    assert set(tcp_server.subscribers_table["/shared"]) == {1}
    assert tcp_server.unregistered[0].created_by == 2
    assert tcp_server.errors == []


def test_owner_can_replace_and_remove_its_node(monkeypatch):
    tcp_server = FakeTcpServer(client_id=1)
    original_node = object()
    replacement_node = object()
    tcp_server.subscribers_table["/shared"] = {1: original_node}
    tcp_server.subscriber_clients["/shared"] = {1}
    commands = SysCommands(tcp_server)
    commands.resolve_message_name = lambda name, extension_hint=None: object
    monkeypatch.setattr(
        "ros_tcp_endpoint.server.RosSubscriber",
        lambda topic, message_class, server, **kwargs: replacement_node,
    )

    commands.subscribe("/shared", "std_msgs/String")
    assert tcp_server.subscribers_table["/shared"] == {1: replacement_node}
    assert tcp_server.unregistered == [original_node]

    commands.remove_subscriber("/shared")
    assert "/shared" not in tcp_server.subscribers_table
    assert "/shared" not in tcp_server.subscriber_clients
    assert tcp_server.unregistered == [original_node, replacement_node]


def test_shared_topic_registration_upgrades_legacy_single_owner(monkeypatch):
    tcp_server = FakeTcpServer(client_id=2)
    original_node = SimpleNamespace(created_by=1)
    replacement_node = SimpleNamespace(created_by=2)
    tcp_server.subscribers_table["/shared"] = original_node
    tcp_server.subscriber_clients["/shared"] = 1
    commands = SysCommands(tcp_server)
    commands.resolve_message_name = lambda name, extension_hint=None: object
    monkeypatch.setattr(
        "ros_tcp_endpoint.server.RosSubscriber",
        lambda topic, message_class, server, **kwargs: replacement_node,
    )

    commands.subscribe("/shared", "std_msgs/String")

    assert tcp_server.subscriber_clients["/shared"] == {1, 2}
    assert tcp_server.subscribers_table["/shared"] == {
        1: original_node,
        2: replacement_node,
    }
    assert tcp_server.unregistered == []
    assert tcp_server.errors == []


def test_concurrent_clients_can_both_claim_the_same_topic(monkeypatch):
    tcp_server = FakeTcpServer(client_id=1)
    start_barrier = threading.Barrier(2)
    commands = SysCommands(tcp_server)
    commands.resolve_message_name = lambda name, extension_hint=None: object

    def make_subscriber(topic, message_class, server, **kwargs):
        return SimpleNamespace(created_by=server.get_active_client().client_id)

    monkeypatch.setattr("ros_tcp_endpoint.server.RosSubscriber", make_subscriber)

    def register(client_id):
        tcp_server.set_active_client(client_id)
        start_barrier.wait(timeout=1.0)
        commands.subscribe("/contended", "std_msgs/String")

    threads = [threading.Thread(target=register, args=(client_id,)) for client_id in (1, 2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2.0)

    assert tcp_server.subscriber_clients["/contended"] == {1, 2}
    registrations = tcp_server.subscribers_table["/contended"]
    assert set(registrations) == {1, 2}
    assert {node.created_by for node in registrations.values()} == {1, 2}
    assert tcp_server.unregistered == []
    assert tcp_server.errors == []


def test_shared_subscription_callback_targets_only_its_unity_client():
    tcp_server = SimpleNamespace(
        _state_lock=threading.RLock(),
        subscriber_clients={"/shared": {1, 2}},
    )
    sender = UnityTcpSender(tcp_server)
    sender.client_queues = {1: queue.Queue(), 2: queue.Queue()}

    sender.send_unity_message("/shared", EmptyMessage(), client_id=1)

    assert sender.client_queues[1].get_nowait()
    with pytest.raises(queue.Empty):
        sender.client_queues[2].get_nowait()
