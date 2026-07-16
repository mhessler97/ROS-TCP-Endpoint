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

import json
import queue
import socket
import struct
import threading
import time

import rclpy
from action_msgs.msg import GoalStatus
from example_interfaces.action import Fibonacci
from rclpy.action import ActionClient, ActionServer, CancelResponse, GoalResponse
from rclpy.callback_groups import ReentrantCallbackGroup
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node
from rclpy.qos import QoSDurabilityPolicy, QoSProfile, QoSReliabilityPolicy
from rclpy.serialization import deserialize_message, serialize_message
from std_msgs.msg import Empty as EmptyMessage
from std_msgs.msg import String
from std_srvs.srv import Empty, SetBool
from twist_mux_msgs.action import JoyPriority, JoyTurbo

from ros_tcp_endpoint import TcpServer


HOST = "127.0.0.1"
PORT = 12000
CDR_REPRESENTATION_IDENTIFIERS = (
    0x0000,
    0x0001,
    0x0002,
    0x0003,
    0x0006,
    0x0007,
    0x0008,
    0x0009,
    0x000A,
    0x000B,
)


def empty_wire_encodings(message):
    return (
        b"",
        serialize_message(message),
        *(
            identifier.to_bytes(2, byteorder="big") + b"\x00\x00"
            for identifier in CDR_REPRESENTATION_IDENTIFIERS
        ),
    )


def frame(destination, payload=b""):
    destination_bytes = destination.encode("utf-8")
    return (
        struct.pack("<I", len(destination_bytes))
        + destination_bytes
        + struct.pack("<I", len(payload))
        + payload
    )


def command(name, params):
    return frame(name, json.dumps(params).encode("utf-8"))


def recv_exact(sock, size):
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError("socket closed")
        data.extend(chunk)
    return bytes(data)


class RawClient:
    def __init__(self, name):
        self.name = name
        self.sock = socket.create_connection((HOST, PORT), timeout=10)
        self.sock.settimeout(0.5)
        self.frames = queue.Queue()
        self.backlog = []
        self.closed = threading.Event()
        self.reader = threading.Thread(target=self._read_loop, daemon=True)
        self.reader.start()
        handshake = self.receive("__handshake", timeout=10)
        handshake_data = json.loads(handshake.decode("utf-8"))
        assert handshake_data["version"] == "v0.8.0"
        handshake_metadata = json.loads(handshake_data["metadata"])
        assert "topic-qos" in handshake_metadata["features"]

    def _read_loop(self):
        try:
            while not self.closed.is_set():
                try:
                    destination_size = struct.unpack("<I", recv_exact(self.sock, 4))[0]
                except socket.timeout:
                    continue
                destination = recv_exact(self.sock, destination_size).decode("utf-8")
                payload_size = struct.unpack("<I", recv_exact(self.sock, 4))[0]
                payload = recv_exact(self.sock, payload_size)
                self.frames.put((destination, payload))
        except (ConnectionError, OSError):
            pass
        finally:
            self.closed.set()

    def send_command(self, name, params):
        self.sock.sendall(command(name, params))

    def send_message(self, destination, payload):
        self.sock.sendall(frame(destination, payload))

    def receive(self, destination=None, timeout=5):
        deadline = time.monotonic() + timeout
        while True:
            for index, item in enumerate(self.backlog):
                if destination is None or item[0] == destination:
                    _, payload = self.backlog.pop(index)
                    return payload
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    "{} timed out waiting for {}; backlog={}".format(
                        self.name, destination, [item[0] for item in self.backlog]
                    )
                )
            item = self.frames.get(timeout=remaining)
            if destination is None or item[0] == destination:
                return item[1]
            self.backlog.append(item)

    def receive_frame(self, timeout=5):
        if self.backlog:
            return self.backlog.pop(0)
        try:
            return self.frames.get(timeout=timeout)
        except queue.Empty as exc:
            raise TimeoutError("{} timed out waiting for a frame".format(self.name)) from exc

    def expect_error(self, text=None, timeout=5):
        payload = json.loads(self.receive("__error", timeout=timeout).decode("utf-8"))
        if text is not None:
            assert text in payload["text"], payload
        return payload["text"]

    def close(self):
        if self.closed.is_set():
            return
        self.closed.set()
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()
        self.reader.join(timeout=1)


def wait_until(predicate, timeout=10, message="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise TimeoutError("Timed out waiting for {}".format(message))


def wait_future(future, timeout=10):
    event = threading.Event()
    future.add_done_callback(lambda _: event.set())
    assert event.wait(timeout), "future timed out"
    return future.result()


def publish_until_received(publisher, client, destination, timeout=5):
    deadline = time.monotonic() + timeout
    sequence = 0
    while time.monotonic() < deadline:
        sequence += 1
        publisher.publish(String(data="ros-message-{}".format(sequence)))
        try:
            payload = client.receive(destination, timeout=0.2)
            return deserialize_message(payload, String)
        except (TimeoutError, queue.Empty):
            pass
    raise TimeoutError("subscription did not deliver")


def main():
    print("[stress] initializing ROS and endpoint")
    rclpy.init()
    endpoint = TcpServer("stress_endpoint", tcp_ip=HOST, tcp_port=PORT)
    endpoint.start()
    endpoint_thread = threading.Thread(target=endpoint.setup_executor, daemon=True)
    endpoint_thread.start()

    ros_node = Node("stress_ros_node")
    callback_group = ReentrantCallbackGroup()
    ros_executor = MultiThreadedExecutor(num_threads=8)
    ros_executor.add_node(ros_node)
    ros_thread = threading.Thread(target=ros_executor.spin, daemon=True)
    ros_thread.start()

    empty_calls = []
    set_bool_calls = []
    slow_service_started = threading.Event()
    release_slow_service = threading.Event()
    shutdown_action_started = threading.Event()
    release_shutdown_action = threading.Event()
    ros_node.create_service(
        Empty,
        "/empty_ros",
        lambda request, response: (empty_calls.append(True), response)[1],
        callback_group=callback_group,
    )

    def handle_set_bool(request, response):
        set_bool_calls.append(request.data)
        response.success = request.data
        response.message = "accepted" if request.data else "rejected"
        return response

    ros_node.create_service(
        SetBool,
        "/set_bool_ros",
        handle_set_bool,
        callback_group=callback_group,
    )

    def handle_slow_service(request, response):
        slow_service_started.set()
        release_slow_service.wait(timeout=2)
        return response

    slow_service = ros_node.create_service(
        Empty,
        "/slow_ros",
        handle_slow_service,
        callback_group=callback_group,
    )

    def execute_fibonacci(goal_handle):
        sequence = [0, 1]
        feedback = Fibonacci.Feedback()
        for _ in range(2, max(2, goal_handle.request.order)):
            if goal_handle.is_cancel_requested:
                goal_handle.canceled()
                result = Fibonacci.Result()
                result.sequence = sequence
                return result
            sequence.append(sequence[-1] + sequence[-2])
            feedback.sequence = sequence
            goal_handle.publish_feedback(feedback)
            time.sleep(0.03)
        goal_handle.succeed()
        result = Fibonacci.Result()
        result.sequence = sequence
        return result

    ros_action_server = ActionServer(
        ros_node,
        Fibonacci,
        "/fib_ros",
        execute_callback=execute_fibonacci,
        goal_callback=lambda goal: (
            GoalResponse.REJECT if goal.order < 0 else GoalResponse.ACCEPT
        ),
        cancel_callback=lambda goal_handle: CancelResponse.ACCEPT,
        callback_group=callback_group,
    )

    def execute_shutdown_action(goal_handle):
        shutdown_action_started.set()
        release_shutdown_action.wait(timeout=10)
        try:
            goal_handle.abort()
        except Exception:
            pass
        return Fibonacci.Result()

    shutdown_ros_action_server = ActionServer(
        ros_node,
        Fibonacci,
        "/shutdown_ros_action",
        execute_callback=execute_shutdown_action,
        callback_group=callback_group,
    )

    def execute_empty_action(goal_handle):
        goal_handle.publish_feedback(JoyPriority.Feedback())
        goal_handle.succeed()
        return JoyPriority.Result()

    empty_ros_action_server = ActionServer(
        ros_node,
        JoyPriority,
        "/empty_ros_action",
        execute_callback=execute_empty_action,
        callback_group=callback_group,
    )

    wait_until(
        lambda: endpoint_thread.is_alive(), timeout=5, message="endpoint executor thread"
    )
    wait_until(
        lambda: _can_connect(HOST, PORT), timeout=20, message="endpoint TCP listener"
    )

    client1 = RawClient("client1")
    client2 = RawClient("client2")
    try:
        print("[stress] testing two-client publishing and ownership isolation")
        from_unity_messages = []
        from_unity_event = threading.Event()

        def on_from_unity(message):
            from_unity_messages.append(message.data)
            from_unity_event.set()

        ros_node.create_subscription(
            String,
            "/from_unity",
            on_from_unity,
            10,
            callback_group=callback_group,
        )
        client1.send_command(
            "__publish", {"topic": "/from_unity", "message_name": "std_msgs/String"}
        )
        wait_until(
            lambda: endpoint.client_owns(endpoint.publisher_clients, "/from_unity", 2),
            message="client1 publisher registration",
        )
        client1.send_message(
            "/from_unity", serialize_message(String(data="owned-by-client1"))
        )
        assert from_unity_event.wait(5)
        assert from_unity_messages[-1] == "owned-by-client1"

        client2.send_command(
            "__publish", {"topic": "/from_unity", "message_name": "std_msgs/String"}
        )
        client2.expect_error("owned by another client")
        from_unity_event.clear()
        client2.send_message(
            "/from_unity", serialize_message(String(data="unauthorized"))
        )
        client2.expect_error("Not registered to publish")
        assert not from_unity_event.wait(0.3)

        print("[stress] testing fieldless Unity-to-ROS topic encodings")
        empty_topic_messages = []
        empty_topic_event = threading.Event()

        def on_empty_topic(message):
            empty_topic_messages.append(message)
            empty_topic_event.set()

        ros_node.create_subscription(
            EmptyMessage,
            "/empty_from_unity",
            on_empty_topic,
            10,
            callback_group=callback_group,
        )
        client1.send_command(
            "__publish",
            {"topic": "/empty_from_unity", "message_name": "std_msgs/Empty"},
        )
        wait_until(
            lambda: endpoint.client_owns(
                endpoint.publisher_clients, "/empty_from_unity", 2
            ),
            message="fieldless topic publisher registration",
        )
        empty_topic_encodings = empty_wire_encodings(EmptyMessage())
        for empty_payload in empty_topic_encodings:
            empty_topic_event.clear()
            client1.send_message("/empty_from_unity", empty_payload)
            assert empty_topic_event.wait(5)
        assert len(empty_topic_messages) == len(empty_topic_encodings)
        empty_topic_event.clear()
        client1.send_message("/empty_from_unity", b"malformed-empty-topic")
        assert not empty_topic_event.wait(0.3)
        client1.send_message("/empty_from_unity", b"\x00\x01\x00\x00")
        assert empty_topic_event.wait(5)
        assert len(empty_topic_messages) == len(empty_topic_encodings) + 1

        print("[stress] testing ROS-to-Unity subscription and collision rejection")
        to_unity_publisher = ros_node.create_publisher(String, "/to_unity", 10)
        client1.send_command(
            "__subscribe", {"topic": "/to_unity", "message_name": "std_msgs/String"}
        )
        wait_until(
            lambda: endpoint.client_owns(endpoint.subscriber_clients, "/to_unity", 2),
            message="client1 subscriber registration",
        )
        delivered = publish_until_received(to_unity_publisher, client1, "/to_unity")
        assert delivered.data.startswith("ros-message-")

        client2.send_command(
            "__subscribe", {"topic": "/to_unity", "message_name": "std_msgs/String"}
        )
        client2.expect_error("owned by another client")
        delivered = publish_until_received(to_unity_publisher, client1, "/to_unity")
        assert delivered.data.startswith("ros-message-")

        print("[stress] testing backward-compatible topic QoS and late joiners")
        latch_probe_event = threading.Event()
        latch_probe_messages = []
        latch_probe = ros_node.create_subscription(
            String,
            "/latched_from_unity",
            lambda message: (
                latch_probe_messages.append(message.data),
                latch_probe_event.set(),
            ),
            10,
            callback_group=callback_group,
        )
        client1.send_command(
            "__publish",
            {
                "topic": "/latched_from_unity",
                "message_name": "std_msgs/String",
                "queue_size": 3,
                "latch": True,
            },
        )
        wait_until(
            lambda: endpoint.client_owns(
                endpoint.publisher_clients, "/latched_from_unity", 2
            ),
            message="latched publisher registration",
        )
        latched_bridge = endpoint.get_registration(
            endpoint.publishers_table, "/latched_from_unity"
        )
        assert latched_bridge.qos_profile.depth == 3
        assert (
            latched_bridge.qos_profile.durability
            == QoSDurabilityPolicy.TRANSIENT_LOCAL
        )
        retained_values = ["retained-{}".format(index) for index in range(5)]
        for retained_value in retained_values:
            latch_probe_event.clear()
            client1.send_message(
                "/latched_from_unity",
                serialize_message(String(data=retained_value)),
            )
            assert latch_probe_event.wait(5)
        assert latch_probe_messages == retained_values
        ros_node.destroy_subscription(latch_probe)

        late_latch_event = threading.Event()
        late_latch_messages = []
        ros_node.create_subscription(
            String,
            "/latched_from_unity",
            lambda message: (
                late_latch_messages.append(message.data),
                late_latch_event.set(),
            ),
            QoSProfile(
                depth=3,
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
            ),
            callback_group=callback_group,
        )
        assert late_latch_event.wait(5)
        wait_until(
            lambda: len(late_latch_messages) == 3,
            timeout=5,
            message="all retained transient-local samples",
        )
        assert late_latch_messages == retained_values[-3:]

        transient_ros_publisher = ros_node.create_publisher(
            String,
            "/transient_to_unity",
            QoSProfile(
                depth=2,
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
            ),
        )
        transient_ros_publisher.publish(String(data="retained-from-ros"))
        time.sleep(0.1)
        client1.send_command(
            "__subscribe",
            {
                "topic": "/transient_to_unity",
                "message_name": "std_msgs/String",
                "latch": True,
            },
        )
        wait_until(
            lambda: endpoint.client_owns(
                endpoint.subscriber_clients, "/transient_to_unity", 2
            ),
            message="transient-local subscriber registration",
        )
        retained_from_ros = deserialize_message(
            client1.receive("/transient_to_unity", timeout=10), String
        )
        assert retained_from_ros.data == "retained-from-ros"

        best_effort_publisher = ros_node.create_publisher(
            String,
            "/best_effort_to_unity",
            QoSProfile(
                depth=5,
                reliability=QoSReliabilityPolicy.BEST_EFFORT,
                durability=QoSDurabilityPolicy.VOLATILE,
            ),
        )
        client1.send_command(
            "__subscribe",
            {
                "topic": "/best_effort_to_unity",
                "message_name": "std_msgs/String",
                "qos": "sensor_data",
            },
        )
        wait_until(
            lambda: endpoint.client_owns(
                endpoint.subscriber_clients, "/best_effort_to_unity", 2
            ),
            message="best-effort subscriber registration",
        )
        best_effort_bridge = endpoint.get_registration(
            endpoint.subscribers_table, "/best_effort_to_unity"
        )
        assert (
            best_effort_bridge.qos_profile.reliability
            == QoSReliabilityPolicy.BEST_EFFORT
        )
        best_effort_message = publish_until_received(
            best_effort_publisher, client1, "/best_effort_to_unity"
        )
        assert best_effort_message.data.startswith("ros-message-")

        client1.send_command(
            "__subscribe",
            {
                "topic": "/invalid_qos",
                "message_name": "std_msgs/String",
                "qos": {"durability": "permanent"},
            },
        )
        client1.expect_error("Failed to register subscriber", timeout=5)
        assert "/invalid_qos" not in endpoint.subscribers_table

        print("[stress] testing empty and non-empty Unity-to-ROS service calls")
        client1.send_command(
            "__ros_service", {"topic": "/empty_ros", "message_name": "std_srvs/Empty"}
        )
        wait_until(
            lambda: endpoint.client_owns(endpoint.ros_service_clients, "/empty_ros", 2),
            message="empty ROS service registration",
        )
        empty_request_encodings = empty_wire_encodings(Empty.Request())
        for srv_id, request_payload in enumerate(
            empty_request_encodings, start=5000
        ):
            client1.send_command("__request", {"srv_id": srv_id})
            client1.send_message("/empty_ros", request_payload)
            response_header = json.loads(
                client1.receive("__response").decode("utf-8")
            )
            assert response_header["srv_id"] == srv_id
            empty_response = client1.receive("/empty_ros")
            assert isinstance(
                deserialize_message(empty_response, Empty.Response), Empty.Response
            )
        assert len(empty_calls) == len(empty_request_encodings)

        client1.send_command(
            "__ros_service",
            {"topic": "/set_bool_ros", "message_name": "std_srvs/SetBool"},
        )
        wait_until(
            lambda: endpoint.client_owns(endpoint.ros_service_clients, "/set_bool_ros", 2),
            message="SetBool ROS service registration",
        )
        client1.send_command("__request", {"srv_id": 502})
        client1.send_message(
            "/set_bool_ros", serialize_message(SetBool.Request(data=True))
        )
        assert json.loads(client1.receive("__response").decode("utf-8"))["srv_id"] == 502
        set_bool_response = deserialize_message(client1.receive("/set_bool_ros"), SetBool.Response)
        assert set_bool_response.success and set_bool_response.message == "accepted"
        assert set_bool_calls == [True]
        client1.send_command("__request", {"srv_id": 508})
        client1.send_message("/set_bool_ros", b"\x00\x01\x00\x00")
        client1.expect_error("No response data from service", timeout=5)
        assert set_bool_calls == [True]

        print("[stress] testing ROS service timeout during server shutdown")
        client1.send_command(
            "__ros_service", {"topic": "/slow_ros", "message_name": "std_srvs/Empty"}
        )
        wait_until(
            lambda: endpoint.client_owns(endpoint.ros_service_clients, "/slow_ros", 2),
            message="slow ROS service registration",
        )
        endpoint.get_registration(
            endpoint.ros_services_table, "/slow_ros"
        ).service_wait_timeout_sec = 0.2
        client1.send_command("__request", {"srv_id": 505})
        client1.send_message("/slow_ros", b"")
        assert slow_service_started.wait(5)
        client1.expect_error("No response data from service", timeout=5)
        release_slow_service.set()
        time.sleep(0.2)
        ros_node.destroy_service(slow_service)
        slow_bridge = endpoint.get_registration(
            endpoint.ros_services_table, "/slow_ros"
        )
        wait_until(
            lambda: not slow_bridge.cli.service_is_ready(),
            timeout=5,
            message="slow ROS service shutdown discovery",
        )
        client1.send_command("__request", {"srv_id": 506})
        client1.send_message("/slow_ros", b"")
        client1.expect_error("No response data from service", timeout=5)

        client2.send_command("__request", {"srv_id": 503})
        client2.send_message("/empty_ros", b"")
        client2.expect_error("Service destination")

        print("[stress] testing empty Unity-hosted service responses")
        client1.send_command(
            "__unity_service", {"topic": "/empty_unity", "message_name": "std_srvs/Empty"}
        )
        wait_until(
            lambda: endpoint.client_owns(endpoint.unity_service_clients, "/empty_unity", 2),
            message="Unity service registration",
        )
        unity_service_client = ros_node.create_client(
            Empty, "/empty_unity", callback_group=callback_group
        )
        assert unity_service_client.wait_for_service(timeout_sec=5)
        for response_payload in empty_wire_encodings(Empty.Response()):
            service_future = unity_service_client.call_async(Empty.Request())
            request_header = json.loads(client1.receive("__request").decode("utf-8"))
            client1.receive("/empty_unity")
            client1.send_command("__response", {"srv_id": request_header["srv_id"]})
            client1.send_message("/empty_unity", response_payload)
            assert isinstance(wait_future(service_future), Empty.Response)

        original_unity_service_timeout = (
            endpoint.unity_tcp_sender.unity_service_timeout_sec
        )
        endpoint.unity_tcp_sender.unity_service_timeout_sec = 0.2
        timed_out_service = unity_service_client.call_async(Empty.Request())
        timed_out_header = json.loads(
            client1.receive("__request", timeout=10).decode("utf-8")
        )
        client1.receive("/empty_unity", timeout=10)
        assert isinstance(wait_future(timed_out_service, timeout=5), Empty.Response)
        client1.send_command(
            "__response", {"srv_id": timed_out_header["srv_id"]}
        )
        client1.send_message("/empty_unity", b"")
        endpoint.unity_tcp_sender.unity_service_timeout_sec = (
            original_unity_service_timeout
        )

        recovery_service = unity_service_client.call_async(Empty.Request())
        recovery_header = json.loads(
            client1.receive("__request", timeout=10).decode("utf-8")
        )
        client1.receive("/empty_unity", timeout=10)
        client1.send_command(
            "__response", {"srv_id": recovery_header["srv_id"]}
        )
        client1.send_message("/empty_unity", b"")
        assert isinstance(wait_future(recovery_service), Empty.Response)

        print("[stress] testing Unity-to-ROS action goal, feedback, result, and cancel")
        client1.send_command(
            "__ros_action",
            {"action_name": "/fib_ros", "action_type": "example_interfaces/Fibonacci"},
        )
        wait_until(
            lambda: endpoint.client_owns(endpoint.ros_action_clients_owner, "/fib_ros", 2),
            timeout=15,
            message="ROS action registration",
        )
        client2.send_command(
            "__ros_action",
            {"action_name": "/fib_ros", "action_type": "example_interfaces/Fibonacci"},
        )
        client2.expect_error("owned by another client")

        client1.send_command(
            "__action_goal", {"action_name": "/fib_ros", "goal_id": "goal-rejected"}
        )
        client1.send_message("/fib_ros", serialize_message(Fibonacci.Goal(order=-1)))
        rejected_response = json.loads(
            client1.receive("__action_goal_response", timeout=10).decode("utf-8")
        )
        assert not rejected_response["accepted"]
        client1.expect_error("rejected")

        client1.send_command(
            "__action_goal", {"action_name": "/fib_ros", "goal_id": "goal-normal"}
        )
        client1.send_message("/fib_ros", serialize_message(Fibonacci.Goal(order=7)))
        goal_response = json.loads(
            client1.receive("__action_goal_response", timeout=10).decode("utf-8")
        )
        assert goal_response["accepted"]
        for _ in range(5):
            assert client1.receive("__action_feedback", timeout=10) is not None
            feedback = deserialize_message(
                client1.receive("/fib_ros"), Fibonacci.Feedback
            )
            assert feedback.sequence
        result_header = json.loads(
            client1.receive("__action_result", timeout=10).decode("utf-8")
        )
        result = deserialize_message(client1.receive("/fib_ros"), Fibonacci.Result)
        assert result_header["status"] == GoalStatus.STATUS_SUCCEEDED
        assert list(result.sequence)[-1] == 8

        client1.send_command(
            "__action_goal", {"action_name": "/fib_ros", "goal_id": "goal-cancel"}
        )
        client1.send_message("/fib_ros", serialize_message(Fibonacci.Goal(order=40)))
        cancel_response = json.loads(
            client1.receive("__action_goal_response", timeout=10).decode("utf-8")
        )
        assert cancel_response["accepted"]
        client1.send_command(
            "__action_cancel", {"action_name": "/fib_ros", "goal_id": "goal-cancel"}
        )
        while True:
            destination, payload = client1.receive_frame(timeout=10)
            if destination == "__action_feedback":
                while True:
                    feedback_destination, feedback_payload = client1.receive_frame(
                        timeout=10
                    )
                    if feedback_destination == "/fib_ros":
                        break
                deserialize_message(feedback_payload, Fibonacci.Feedback)
                continue
            if destination != "__action_result":
                # Topic traffic can legitimately interleave with action frames.
                continue
            canceled_header = json.loads(payload.decode("utf-8"))
            while True:
                result_destination, result_payload = client1.receive_frame(timeout=10)
                if result_destination == "/fib_ros":
                    break
            deserialize_message(result_payload, Fibonacci.Result)
            break
        assert canceled_header["status"] == GoalStatus.STATUS_CANCELED

        print("[stress] testing ROS action server shutdown during an accepted goal")
        client1.send_command(
            "__ros_action",
            {
                "action_name": "/shutdown_ros_action",
                "action_type": "example_interfaces/Fibonacci",
            },
        )
        wait_until(
            lambda: endpoint.client_owns(
                endpoint.ros_action_clients_owner, "/shutdown_ros_action", 2
            ),
            timeout=15,
            message="shutdown ROS action registration",
        )
        shutdown_bridge = endpoint.get_registration(
            endpoint.ros_action_clients, "/shutdown_ros_action"
        )
        client1.send_command(
            "__action_goal",
            {
                "action_name": "/shutdown_ros_action",
                "goal_id": "goal-server-shutdown",
            },
        )
        client1.send_message(
            "/shutdown_ros_action", serialize_message(Fibonacci.Goal(order=10))
        )
        shutdown_goal_response = json.loads(
            client1.receive("__action_goal_response", timeout=10).decode("utf-8")
        )
        assert shutdown_goal_response["accepted"]
        assert shutdown_action_started.wait(5)
        shutdown_ros_action_server.destroy()
        shutdown_ros_action_server = None
        wait_until(
            lambda: not shutdown_bridge._client.server_is_ready(),
            timeout=10,
            message="ROS action server shutdown discovery",
        )
        shutdown_result_header = json.loads(
            client1.receive("__action_result", timeout=10).decode("utf-8")
        )
        shutdown_result = deserialize_message(
            client1.receive("/shutdown_ros_action", timeout=10), Fibonacci.Result
        )
        assert shutdown_result_header["status"] == GoalStatus.STATUS_ABORTED
        assert isinstance(shutdown_result, Fibonacci.Result)
        client1.expect_error("shut down while waiting", timeout=10)
        release_shutdown_action.set()

        client1.send_command(
            "__action_goal",
            {
                "action_name": "/shutdown_ros_action",
                "goal_id": "goal-after-server-shutdown",
            },
        )
        client1.send_message(
            "/shutdown_ros_action", serialize_message(Fibonacci.Goal(order=3))
        )
        unavailable_goal_response = json.loads(
            client1.receive("__action_goal_response", timeout=10).decode("utf-8")
        )
        assert not unavailable_goal_response["accepted"]
        client1.expect_error("is not ready", timeout=10)

        print("[stress] testing fieldless Unity-to-ROS action goals")
        client1.send_command(
            "__ros_action",
            {
                "action_name": "/empty_ros_action",
                "action_type": "twist_mux_msgs/JoyPriority",
            },
        )
        wait_until(
            lambda: endpoint.client_owns(
                endpoint.ros_action_clients_owner, "/empty_ros_action", 2
            ),
            timeout=15,
            message="fieldless ROS action registration",
        )
        client1.send_command(
            "__action_goal",
            {"action_name": "/empty_ros_action", "goal_id": "malformed-empty-goal"},
        )
        client1.send_message("/empty_ros_action", b"malformed-empty-goal")
        malformed_goal_response = json.loads(
            client1.receive("__action_goal_response", timeout=10).decode("utf-8")
        )
        assert not malformed_goal_response["accepted"]
        client1.expect_error("Failed to deserialize goal")
        for index, empty_payload in enumerate(
            empty_wire_encodings(JoyPriority.Goal())
        ):
            goal_id = "empty-goal-{}".format(index)
            client1.send_command(
                "__action_goal",
                {"action_name": "/empty_ros_action", "goal_id": goal_id},
            )
            client1.send_message("/empty_ros_action", empty_payload)
            empty_goal_response = json.loads(
                client1.receive("__action_goal_response", timeout=10).decode("utf-8")
            )
            assert empty_goal_response["accepted"]
            client1.receive("__action_feedback", timeout=10)
            assert isinstance(
                deserialize_message(
                    client1.receive("/empty_ros_action"), JoyPriority.Feedback
                ),
                JoyPriority.Feedback,
            )
            empty_result_header = json.loads(
                client1.receive("__action_result", timeout=10).decode("utf-8")
            )
            assert empty_result_header["status"] == GoalStatus.STATUS_SUCCEEDED
            assert isinstance(
                deserialize_message(
                    client1.receive("/empty_ros_action"), JoyPriority.Result
                ),
                JoyPriority.Result,
            )

        print("[stress] testing ROS-to-Unity action execution over TCP")
        client1.send_command(
            "__unity_action",
            {"action_name": "/fib_unity", "action_type": "example_interfaces/Fibonacci"},
        )
        wait_until(
            lambda: endpoint.client_owns(
                endpoint.unity_action_servers_owner, "/fib_unity", 2
            ),
            message="Unity action registration",
        )
        client2.send_command(
            "__action_feedback",
            {"action_name": "/fib_unity", "goal_id": "not-client2s-goal"},
        )
        client2.expect_error("unowned Unity action")
        unity_action_client = ActionClient(
            ros_node,
            Fibonacci,
            "/fib_unity",
            callback_group=callback_group,
        )
        assert unity_action_client.wait_for_server(timeout_sec=5)
        ros_goal_handle = wait_future(
            unity_action_client.send_goal_async(Fibonacci.Goal(order=6)), timeout=10
        )
        assert ros_goal_handle.accepted
        unity_goal_header = json.loads(
            client1.receive("__action_goal_request", timeout=10).decode("utf-8")
        )
        unity_goal = deserialize_message(client1.receive("/fib_unity"), Fibonacci.Goal)
        assert unity_goal.order == 6
        unity_goal_id = unity_goal_header["goal_id"]
        client1.send_command(
            "__action_feedback",
            {"action_name": "/fib_unity", "goal_id": unity_goal_id},
        )
        client1.send_message(
            "/fib_unity", serialize_message(Fibonacci.Feedback(sequence=[0, 1, 1]))
        )
        client1.send_command(
            "__action_result",
            {
                "action_name": "/fib_unity",
                "goal_id": unity_goal_id,
                "status": GoalStatus.STATUS_SUCCEEDED,
            },
        )
        client1.send_message(
            "/fib_unity",
            serialize_message(Fibonacci.Result(sequence=[0, 1, 1, 2, 3, 5])),
        )
        ros_result = wait_future(ros_goal_handle.get_result_async(), timeout=10)
        assert ros_result.status == GoalStatus.STATUS_SUCCEEDED
        assert list(ros_result.result.sequence)[-1] == 5

        cancel_from_ros_goal = wait_future(
            unity_action_client.send_goal_async(Fibonacci.Goal(order=12)), timeout=10
        )
        assert cancel_from_ros_goal.accepted
        cancel_goal_header = json.loads(
            client1.receive("__action_goal_request", timeout=10).decode("utf-8")
        )
        client1.receive("/fib_unity", timeout=10)
        cancel_from_ros_future = cancel_from_ros_goal.cancel_goal_async()
        cancel_request_header = json.loads(
            client1.receive("__action_cancel_request", timeout=10).decode("utf-8")
        )
        assert cancel_request_header["goal_id"] == cancel_goal_header["goal_id"]
        cancel_from_ros_response = wait_future(cancel_from_ros_future, timeout=10)
        assert len(cancel_from_ros_response.goals_canceling) == 1
        client1.send_command(
            "__action_result",
            {
                "action_name": "/fib_unity",
                "goal_id": cancel_goal_header["goal_id"],
                "status": GoalStatus.STATUS_CANCELED,
            },
        )
        client1.send_message(
            "/fib_unity", serialize_message(Fibonacci.Result(sequence=[0, 1]))
        )
        canceled_from_ros_result = wait_future(
            cancel_from_ros_goal.get_result_async(), timeout=10
        )
        assert canceled_from_ros_result.status == GoalStatus.STATUS_CANCELED

        print("[stress] testing fieldless Unity action feedback and results")
        client1.send_command(
            "__unity_action",
            {
                "action_name": "/empty_unity_action",
                "action_type": "twist_mux_msgs/JoyTurbo",
            },
        )
        wait_until(
            lambda: endpoint.client_owns(
                endpoint.unity_action_servers_owner, "/empty_unity_action", 2
            ),
            message="fieldless Unity action registration",
        )
        empty_unity_action_client = ActionClient(
            ros_node,
            JoyTurbo,
            "/empty_unity_action",
            callback_group=callback_group,
        )
        assert empty_unity_action_client.wait_for_server(timeout_sec=5)
        empty_feedback_messages = []
        for index, empty_payload in enumerate(
            empty_wire_encodings(JoyTurbo.Result())
        ):
            feedback_count = len(empty_feedback_messages)
            empty_goal_handle = wait_future(
                empty_unity_action_client.send_goal_async(
                    JoyTurbo.Goal(),
                    feedback_callback=lambda message: empty_feedback_messages.append(
                        message.feedback
                    ),
                ),
                timeout=10,
            )
            assert empty_goal_handle.accepted
            empty_goal_header = json.loads(
                client1.receive("__action_goal_request", timeout=10).decode("utf-8")
            )
            assert isinstance(
                deserialize_message(
                    client1.receive("/empty_unity_action"), JoyTurbo.Goal
                ),
                JoyTurbo.Goal,
            )
            empty_goal_id = empty_goal_header["goal_id"]
            client1.send_command(
                "__action_feedback",
                {
                    "action_name": "/empty_unity_action",
                    "goal_id": empty_goal_id,
                },
            )
            client1.send_message("/empty_unity_action", empty_payload)
            wait_until(
                lambda: len(empty_feedback_messages) > feedback_count,
                message="fieldless action feedback delivery",
            )
            client1.send_command(
                "__action_result",
                {
                    "action_name": "/empty_unity_action",
                    "goal_id": empty_goal_id,
                    "status": GoalStatus.STATUS_SUCCEEDED,
                },
            )
            client1.send_message("/empty_unity_action", empty_payload)
            empty_action_result = wait_future(
                empty_goal_handle.get_result_async(), timeout=10
            )
            assert empty_action_result.status == GoalStatus.STATUS_SUCCEEDED
            assert isinstance(empty_action_result.result, JoyTurbo.Result)

        malformed_result_goal = wait_future(
            empty_unity_action_client.send_goal_async(JoyTurbo.Goal()), timeout=10
        )
        assert malformed_result_goal.accepted
        malformed_result_header = json.loads(
            client1.receive("__action_goal_request", timeout=10).decode("utf-8")
        )
        client1.receive("/empty_unity_action", timeout=10)
        client1.send_command(
            "__action_result",
            {
                "action_name": "/empty_unity_action",
                "goal_id": malformed_result_header["goal_id"],
                "status": GoalStatus.STATUS_SUCCEEDED,
            },
        )
        client1.send_message("/empty_unity_action", b"malformed-empty-result")
        malformed_action_result = wait_future(
            malformed_result_goal.get_result_async(), timeout=10
        )
        assert malformed_action_result.status == GoalStatus.STATUS_ABORTED

        print(
            "[stress] testing disconnect cleanup, pending service/action release, "
            "and reconnect"
        )
        pending_action_goal = wait_future(
            unity_action_client.send_goal_async(Fibonacci.Goal(order=20)), timeout=10
        )
        assert pending_action_goal.accepted
        client1.receive("__action_goal_request", timeout=10)
        client1.receive("/fib_unity", timeout=10)
        pending_action_result = pending_action_goal.get_result_async()

        pending_service = unity_service_client.call_async(Empty.Request())
        client1.receive("__request", timeout=10)
        client1.receive("/empty_unity", timeout=10)
        client1.close()
        assert isinstance(wait_future(pending_service, timeout=10), Empty.Response)
        disconnected_action_result = wait_future(pending_action_result, timeout=10)
        assert disconnected_action_result.status == GoalStatus.STATUS_ABORTED
        assert isinstance(disconnected_action_result.result, Fibonacci.Result)
        wait_until(
            lambda: "/empty_unity" not in endpoint.unity_service_clients,
            message="disconnect registration cleanup",
        )
        wait_until(
            lambda: not any(
                (
                    endpoint.get_registration(endpoint.subscriber_clients, "/to_unity"),
                    endpoint.get_registration(endpoint.ros_service_clients, "/empty_ros"),
                    endpoint.get_registration(endpoint.ros_action_clients_owner, "/fib_ros"),
                    endpoint.get_registration(
                        endpoint.unity_action_servers_owner, "/fib_unity"
                    ),
                )
            ),
            message="all disconnected client registrations to be removed",
        )

        client1 = RawClient("client1-returned")
        client1.send_command(
            "__publish", {"topic": "/from_unity", "message_name": "std_msgs/String"}
        )
        wait_until(
            lambda: endpoint.client_owns(endpoint.publisher_clients, "/from_unity", 4),
            message="returning client reclaims publisher",
        )
        client1.send_command(
            "__subscribe", {"topic": "/to_unity", "message_name": "std_msgs/String"}
        )
        wait_until(
            lambda: endpoint.client_owns(endpoint.subscriber_clients, "/to_unity", 4),
            message="returning client reclaims subscriber",
        )
        delivered = publish_until_received(to_unity_publisher, client1, "/to_unity")
        assert delivered.data.startswith("ros-message-")
        from_unity_event.clear()
        client1.send_message(
            "/from_unity", serialize_message(String(data="returned-client"))
        )
        assert from_unity_event.wait(5)
        assert from_unity_messages[-1] == "returned-client"
        client1.close()
        wait_until(
            lambda: "/from_unity" not in endpoint.publisher_clients,
            message="returning client disconnect cleanup",
        )
        wait_until(
            lambda: "/to_unity" not in endpoint.subscriber_clients,
            message="returning subscriber disconnect cleanup",
        )

        print("[stress] repeatedly connecting, claiming, publishing, and dropping")
        for cycle in range(5):
            cycling = RawClient("cycle-{}".format(cycle))
            cycling.send_command(
                "__publish", {"topic": "/from_unity", "message_name": "std_msgs/String"}
            )
            wait_until(
                lambda: "/from_unity" in endpoint.publisher_clients,
                message="cycle publisher registration",
            )
            from_unity_event.clear()
            cycling.send_message(
                "/from_unity", serialize_message(String(data="cycle-{}".format(cycle)))
            )
            assert from_unity_event.wait(5)
            assert from_unity_messages[-1] == "cycle-{}".format(cycle)
            cycling.close()
            wait_until(
                lambda: "/from_unity" not in endpoint.publisher_clients,
                message="cycle disconnect cleanup",
            )

        print("[stress] PASS: all live integration scenarios completed")
    finally:
        client1.close()
        client2.close()
        ros_action_server.destroy()
        if shutdown_ros_action_server is not None:
            shutdown_ros_action_server.destroy()
        release_shutdown_action.set()
        empty_ros_action_server.destroy()
        rclpy.shutdown()
        endpoint_thread.join(timeout=3)
        ros_thread.join(timeout=3)
        ros_executor.shutdown(timeout_sec=1)


def _can_connect(host, port):
    try:
        sock = socket.create_connection((host, port), timeout=0.2)
        sock.close()
        return True
    except OSError:
        return False


if __name__ == "__main__":
    main()
