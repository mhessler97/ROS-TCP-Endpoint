#  Copyright 2020 Unity Technologies
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import rclpy
import socket
import json
import threading
import time
import os
from concurrent.futures import ThreadPoolExecutor
from json import JSONDecodeError

from rclpy.node import Node
from rclpy.executors import MultiThreadedExecutor

from .tcp_sender import UnityTcpSender
from .client import ClientThread
from .subscriber import RosSubscriber
from .publisher import RosPublisher
from .service import RosService
from .unity_service import UnityService
from .action_client import RosActionClient
from .unity_action import UnityActionServer


class TcpServer(Node):
    """
    Initializes ROS node and TCP server.
    """

    def __init__(self, node_name, buffer_size=1024, connections=10, tcp_ip=None, tcp_port=None):
        """
        Initializes ROS node and class variables.

        Args:
            node_name:               ROS node name for executing code
            buffer_size:             The read buffer size used when reading from a socket
            connections:             Max number of queued connections. See Python Socket documentation
        """
        super().__init__(node_name)

        self.declare_parameter("ROS_IP", "0.0.0.0")
        self.declare_parameter("ROS_TCP_PORT", 10000)

        if tcp_ip:
            self.loginfo("Using ROS_IP override from constructor: {}".format(tcp_ip))
            self.tcp_ip = tcp_ip
        else:
            self.tcp_ip = self.get_parameter("ROS_IP").get_parameter_value().string_value

        if tcp_port:
            self.loginfo("Using ROS_TCP_PORT override from constructor: {}".format(tcp_port))
            self.tcp_port = tcp_port
        else:
            self.tcp_port = self.get_parameter("ROS_TCP_PORT").get_parameter_value().integer_value

        self.unity_tcp_sender = UnityTcpSender(self)

        self.node_name = node_name
        self.publishers_table = {}
        self.subscribers_table = {}
        self.ros_services_table = {}
        self.unity_services_table = {}
        self.ros_action_clients = {}
        self.unity_action_servers = {}
        self.subscriber_clients = {}
        self.publisher_clients = {}
        self.ros_service_clients = {}
        self.unity_service_clients = {}
        self.ros_action_clients_owner = {}
        self.unity_action_servers_owner = {}
        self._state_lock = threading.RLock()
        self.buffer_size = buffer_size
        self.connections = connections
        self.syscommands = SysCommands(self)
        self.executor = None
        self._executor_lock = threading.RLock()
        self._executor_mutation_lock = threading.Lock()
        self._executor_mutation_requested = threading.Event()
        self._registration_lock = threading.RLock()
        self._last_invalid_handle_log_time = 0.0
        self._last_rcl_error_log_time = 0.0
        self._next_client_id = 1
        self._client_id_lock = threading.Lock()
        self._client_context = threading.local()
        self._service_call_executor = ThreadPoolExecutor(
            max_workers=max(4, min(8, os.cpu_count() or 4)),
            thread_name_prefix="ros_tcp_service",
        )

    def start(self, publishers=None, subscribers=None):
        if publishers is not None:
            self.publishers_table = publishers
        if subscribers is not None:
            self.subscribers_table = subscribers
        server_thread = threading.Thread(target=self.listen_loop)
        # Exit the server thread when the main thread terminates
        server_thread.daemon = True
        server_thread.start()

    def listen_loop(self):
        """
            Creates and binds sockets using TCP variables then listens for incoming connections.
            For each new connection a client thread will be created to handle communication.
        """
        self.loginfo("Starting server on {}:{}".format(self.tcp_ip, self.tcp_port))
        tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_server.bind((self.tcp_ip, self.tcp_port))

        while True:
            tcp_server.listen(self.connections)

            try:
                (conn, (ip, port)) = tcp_server.accept()
                client_id = self.allocate_client_id()
                ClientThread(conn, self, ip, port, client_id=client_id).start()
            except socket.timeout:
                self.logerr("ros_tcp_endpoint.TcpServer: socket timeout")

    def allocate_client_id(self):
        with self._client_id_lock:
            client_id = self._next_client_id
            self._next_client_id += 1
            return client_id

    def get_active_client(self):
        return getattr(self._client_context, "client", None)

    def get_registration(self, table, key):
        with self._state_lock:
            return table.get(key)

    def get_registration_keys(self, table):
        with self._state_lock:
            return list(table.keys())

    def get_owned_registration(self, table, owner_table, key, client_id):
        with self._state_lock:
            if owner_table.get(key) != client_id:
                return None
            return table.get(key)

    def client_owns(self, owner_table, key, client_id):
        with self._state_lock:
            return owner_table.get(key) == client_id

    def send_unity_error(self, error, client_id=None):
        if client_id is None:
            active_client = self.get_active_client()
            if active_client is not None:
                client_id = active_client.client_id
        self.unity_tcp_sender.send_unity_error(error, client_id=client_id)

    def send_unity_message(self, topic, message, client_id=None):
        self.unity_tcp_sender.send_unity_message(topic, message, client_id=client_id)

    def send_unity_service(self, topic, service_class, request, client_id=None):
        return self.unity_tcp_sender.send_unity_service_request(
            topic, service_class, request, client_id=client_id
        )

    def send_unity_service_response(self, srv_id, data, client_id=None):
        self.unity_tcp_sender.send_unity_service_response(srv_id, data, client_id=client_id)

    def submit_service_call(self, callback, *args):
        try:
            future = self._service_call_executor.submit(callback, *args)
        except RuntimeError as exc:
            self.logerr("Unable to schedule service call task: {}".format(exc))
            return False

        future.add_done_callback(self._handle_service_call_result)
        return True

    def _handle_service_call_result(self, future):
        try:
            future.result()
        except Exception as exc:  # noqa: pylint: disable=broad-except
            self.logerr("Unhandled exception in background service task: {}".format(exc))

    def cancel_ros_action_goal(self, action_name, goal_id, client_id=None):
        action_client = self.get_owned_registration(
            self.ros_action_clients,
            self.ros_action_clients_owner,
            action_name,
            client_id,
        )
        if action_client is None:
            self.send_unity_error(
                "Cannot cancel goal {} because action '{}' is not registered.".format(
                    goal_id, action_name
                )
            )
            return
        self.loginfo("Forwarding cancel for action {} goal {}".format(action_name, goal_id))
        action_client.cancel_goal(goal_id, client_id=client_id)

    def handle_syscommand(self, topic, data, client_thread=None):
        function = getattr(self.syscommands, topic[2:], None)
        if function is None:
            self.send_unity_error(
                "Don't understand SysCommand.'{}'".format(topic),
                client_id=getattr(client_thread, "client_id", None),
            )
            return

        # Syscommand payloads are sent as a length-prefixed UTF-8 JSON blob.
        # Some commands (e.g. `__topic_list`) may send an empty payload, and some
        # send a null-terminated string. Be permissive and avoid crashing a client
        # thread on malformed/empty JSON.
        message_json = data.decode("utf-8", errors="replace").strip("\x00\r\n\t ")
        if message_json == "":
            params = {}
        else:
            try:
                params = json.loads(message_json)
            except JSONDecodeError as exc:
                self.logwarn(
                    "Dropping syscommand '{}' due to invalid JSON payload (len={}): {}. Error: {}".format(
                        topic, len(data), repr(message_json[:200]), exc
                    )
                )
                self.send_unity_error(
                    "Invalid JSON payload for {}".format(topic),
                    client_id=getattr(client_thread, "client_id", None),
                )
                return

        if not isinstance(params, dict):
            self.logwarn(
                "Dropping syscommand '{}' because JSON payload is not an object: {}".format(
                    topic, type(params).__name__
                )
            )
            self.send_unity_error(
                "Invalid syscommand payload type for {}".format(topic),
                client_id=getattr(client_thread, "client_id", None),
            )
            return

        previous_client = getattr(self._client_context, "client", None)
        self._client_context.client = client_thread
        try:
            function(**params)
        except TypeError as exc:
            self.logwarn(
                "Dropping syscommand '{}' due to invalid parameters: {}".format(topic, exc)
            )
            self.send_unity_error(
                "Invalid parameters for {}".format(topic),
                client_id=getattr(client_thread, "client_id", None),
            )
        finally:
            self._client_context.client = previous_client

    def _unregister_owned_nodes(self, client_id, table, owner_table):
        with self._registration_lock:
            with self._state_lock:
                owned_keys = [key for key, owner in owner_table.items() if owner == client_id]
                nodes = []
                for key in owned_keys:
                    node = table.pop(key, None)
                    owner_table.pop(key, None)
                    if node is not None:
                        nodes.append(node)

            for node in nodes:
                self.unregister_node(node)

    def on_client_disconnect(self, client_id):
        self.unity_tcp_sender.remove_client(client_id)
        self._unregister_owned_nodes(client_id, self.subscribers_table, self.subscriber_clients)
        self._unregister_owned_nodes(client_id, self.publishers_table, self.publisher_clients)
        self._unregister_owned_nodes(client_id, self.ros_services_table, self.ros_service_clients)
        self._unregister_owned_nodes(client_id, self.unity_services_table, self.unity_service_clients)
        self._unregister_owned_nodes(
            client_id, self.ros_action_clients, self.ros_action_clients_owner
        )
        self._unregister_owned_nodes(
            client_id, self.unity_action_servers, self.unity_action_servers_owner
        )

    def loginfo(self, text):
        self.get_logger().info(text)

    def logwarn(self, text):
        self.get_logger().warning(text)

    def logerr(self, text):
        self.get_logger().error(text)

    def setup_executor(self):
        """
            Since rclpy.spin() is a blocking call the server needed a way
            to spin all of the relevant nodes at the same time.

            MultiThreadedExecutor allows us to set the number of threads
            needed as well as the nodes that need to be spun.
        """
        with self._state_lock:
            registered_nodes = (
                list(self.publishers_table.values())
                + list(self.subscribers_table.values())
                + list(self.ros_services_table.values())
                + list(self.unity_services_table.values())
                + list(self.ros_action_clients.values())
                + list(self.unity_action_servers.values())
            )
        num_threads = max(2, len(registered_nodes) + 1)
        executor = MultiThreadedExecutor(num_threads)
        self.executor = executor

        with self._executor_lock:
            executor.add_node(self)

            for ros_node in registered_nodes:
                executor.add_node(ros_node)

        # rclpy's executor can raise InvalidHandle if a waitable/node is destroyed
        # concurrently with wait set construction (e.g., when Unity unregisters and
        # re-registers pubs/subs/services while spinning). Treat this as a transient
        # race: log and continue rather than crashing the whole process.
        try:
            from rclpy._rclpy_pybind11 import InvalidHandle  # type: ignore
        except Exception:  # noqa: pylint: disable=broad-except
            InvalidHandle = ()  # type: ignore
        try:
            from rclpy._rclpy_pybind11 import RCLError  # type: ignore
        except Exception:  # noqa: pylint: disable=broad-except
            RCLError = ()  # type: ignore

        while rclpy.ok():
            try:
                with self._executor_lock:
                    mutation_requested = self._executor_mutation_requested.is_set()
                    if not mutation_requested:
                        executor.spin_once(timeout_sec=0.1)
                if mutation_requested:
                    # Let the thread which requested an add/remove operation acquire
                    # the executor lock before starting another spin cycle.
                    time.sleep(0.001)
            except InvalidHandle as exc:  # type: ignore[misc]
                now = time.time()
                if now - self._last_invalid_handle_log_time > 5.0:
                    self._last_invalid_handle_log_time = now
                    self.logwarn(
                        "Executor saw InvalidHandle (likely concurrent node/entity destruction); "
                        "continuing. Error: {}".format(exc)
                    )
                continue
            except RCLError as exc:  # type: ignore[misc]
                # This can occur when an ActionClient/ActionServer handle is destroyed but the
                # executor still tries to add it to the wait set (often due to concurrent
                # unregister/re-register flows from Unity clients).
                now = time.time()
                if now - self._last_rcl_error_log_time > 5.0:
                    self._last_rcl_error_log_time = now
                    self.logwarn(
                        "Executor saw RCLError (likely invalid ROS handle during wait set build); "
                        "continuing. Error: {}".format(exc)
                    )
                continue

    def _mutate_executor(self, operation):
        executor = self.executor
        if executor is None:
            return None

        # rclpy supports wake() from another thread.  Signal before waiting on
        # _executor_lock so spin_once exits promptly instead of repeatedly
        # reacquiring the lock and starving registrations/disconnections.
        with self._executor_mutation_lock:
            self._executor_mutation_requested.set()
            try:
                executor.wake()
                with self._executor_lock:
                    return operation(executor)
            finally:
                self._executor_mutation_requested.clear()

    def register_node(self, new_node):
        if new_node is not None:
            self._mutate_executor(lambda executor: executor.add_node(new_node))

    def unregister_node(self, old_node):
        if old_node is not None:
            prepare_unregister = getattr(old_node, "prepare_unregister", None)
            if prepare_unregister is not None:
                try:
                    prepare_unregister()
                except Exception as exc:  # noqa: pylint: disable=broad-except
                    self.logwarn(
                        "Failed to prepare node for unregister; continuing. Error: {}".format(
                            exc
                        )
                    )
            # Remove from the executor *before* destroying the node/entities to avoid
            # races where the executor is still building a wait set using that handle.
            if self.executor is not None:
                try:
                    self._mutate_executor(
                        lambda executor: executor.remove_node(old_node)
                    )
                except Exception as exc:  # noqa: pylint: disable=broad-except
                    self.logwarn(
                        "Failed to remove node from executor during unregister; "
                        "continuing. Error: {}".format(exc)
                    )
            try:
                old_node.unregister()
            except Exception as exc:  # noqa: pylint: disable=broad-except
                self.logwarn(
                    "Failed to unregister/destroy node; continuing. Error: {}".format(exc)
                )

    def destroy_nodes(self):
        """
            Clean up all of the nodes
        """
        with self._registration_lock:
            with self._state_lock:
                registered_nodes = (
                    list(self.publishers_table.values())
                    + list(self.subscribers_table.values())
                    + list(self.ros_services_table.values())
                    + list(self.unity_services_table.values())
                    + list(self.ros_action_clients.values())
                    + list(self.unity_action_servers.values())
                )
                self.publishers_table.clear()
                self.subscribers_table.clear()
                self.ros_services_table.clear()
                self.unity_services_table.clear()
                self.ros_action_clients.clear()
                self.unity_action_servers.clear()
                self.subscriber_clients.clear()
                self.publisher_clients.clear()
                self.ros_service_clients.clear()
                self.unity_service_clients.clear()
                self.ros_action_clients_owner.clear()
                self.unity_action_servers_owner.clear()

            for ros_node in registered_nodes:
                self.unregister_node(ros_node)
        self._service_call_executor.shutdown(wait=False)

        self.destroy_node()


class SysCommands:
    def __init__(self, tcp_server):
        self.tcp_server = tcp_server

    def _get_active_client(self):
        return self.tcp_server.get_active_client()

    def _get_active_client_id(self):
        client = self._get_active_client()
        if client is None:
            return None
        return client.client_id

    def _replace_owned_node(self, table, owner_table, key, node_factory, description):
        client_id = self._get_active_client_id()
        if client_id is None:
            self.tcp_server.logwarn(
                "Ignoring {} registration without an active client".format(description)
            )
            return False

        # Serialize lifecycle mutations separately from state reads.  In
        # particular, never hold _state_lock while waiting for the executor:
        # callbacks running in that executor may themselves need registration
        # state and would otherwise deadlock.
        with self.tcp_server._registration_lock:
            with self.tcp_server._state_lock:
                owner = owner_table.get(key)
                if owner is not None and owner != client_id:
                    self.tcp_server.send_unity_error(
                        "Cannot register {} '{}'; it is owned by another client.".format(
                            description, key
                        ),
                        client_id=client_id,
                    )
                    return False

            try:
                new_node = node_factory()
                self.tcp_server.register_node(new_node)
            except Exception as exc:  # noqa: pylint: disable=broad-except
                self.tcp_server.logerr(
                    "Failed to create {} '{}': {}".format(description, key, exc)
                )
                self.tcp_server.send_unity_error(
                    "Failed to register {} '{}': {}".format(description, key, exc),
                    client_id=client_id,
                )
                if "new_node" in locals():
                    self.tcp_server.unregister_node(new_node)
                return False

            with self.tcp_server._state_lock:
                old_node = table.get(key)
                table[key] = new_node
                owner_table[key] = client_id

            if old_node is not None:
                self.tcp_server.unregister_node(old_node)
        return True

    def _remove_owned_node(self, table, owner_table, key, description):
        client_id = self._get_active_client_id()
        with self.tcp_server._registration_lock:
            with self.tcp_server._state_lock:
                old_node = table.get(key)
                owner = owner_table.get(key)
                if old_node is None:
                    error = "{} '{}' is not registered".format(description, key)
                elif owner != client_id:
                    error = "Cannot remove {} '{}'; it is owned by another client.".format(
                        description, key
                    )
                else:
                    error = None
                    table.pop(key, None)
                    owner_table.pop(key, None)

            if error is not None:
                self.tcp_server.send_unity_error(error, client_id=client_id)
                return False

            self.tcp_server.unregister_node(old_node)
        return True

    def _active_client_owns(self, owner_table, key):
        client_id = self._get_active_client_id()
        return self.tcp_server.client_owns(owner_table, key, client_id)

    def _normalize_action_name(self, name: str) -> str:
        """Return a canonical action/topic name: single leading '/', no trailing '/', collapse repeats."""
        if name is None:
            return name
        # Strip whitespace and collapse multiple slashes
        parts = [p for p in str(name).strip().split('/') if p]
        normalized = '/' + '/'.join(parts) if parts else ''
        return normalized

    def subscribe(
        self, topic, message_name, queue_size=10, latch=False, qos=None
    ):
        if topic == "":
            self.tcp_server.send_unity_error(
                "Can't subscribe to a blank topic name! SysCommand.subscribe({}, {})".format(
                    topic, message_name
                )
            )
            return

        message_class = self.resolve_message_name(message_name)
        if message_class is None:
            self.tcp_server.send_unity_error(
                "SysCommand.subscribe - Unknown message class '{}'".format(message_name)
            )
            return

        if self._replace_owned_node(
            self.tcp_server.subscribers_table,
            self.tcp_server.subscriber_clients,
            topic,
            lambda: RosSubscriber(
                topic,
                message_class,
                self.tcp_server,
                queue_size=queue_size,
                latch=latch,
                qos=qos,
            ),
            "subscriber",
        ):
            self.tcp_server.loginfo("RegisterSubscriber({}, {}) OK".format(topic, message_class))

    def remove_subscriber(self, topic):
        if topic == "":
            self.tcp_server.send_unity_error(
                "Can't unsubscribe from a blank topic name! SysCommand.remove_subscriber({})".format(topic)
            )
            return

        if self._remove_owned_node(
            self.tcp_server.subscribers_table,
            self.tcp_server.subscriber_clients,
            topic,
            "subscriber",
        ):
            self.tcp_server.loginfo("UnregisterSubscriber({}) OK".format(topic))

    def publish(self, topic, message_name, queue_size=10, latch=False, qos=None):
        if topic == "":
            self.tcp_server.send_unity_error(
                "Can't publish to a blank topic name! SysCommand.publish({}, {})".format(
                    topic, message_name
                )
            )
            return

        message_class = self.resolve_message_name(message_name)
        if message_class is None:
            self.tcp_server.send_unity_error(
                "SysCommand.publish - Unknown message class '{}'".format(message_name)
            )
            return

        if self._replace_owned_node(
            self.tcp_server.publishers_table,
            self.tcp_server.publisher_clients,
            topic,
            lambda: RosPublisher(
                topic,
                message_class,
                queue_size=queue_size,
                latch=latch,
                qos=qos,
            ),
            "publisher",
        ):
            self.tcp_server.loginfo("RegisterPublisher({}, {}) OK".format(topic, message_class))

    def ros_service(self, topic, message_name):
        if topic == "":
            self.tcp_server.send_unity_error(
                "RegisterRosService({}, {}) - Can't register a blank topic name!".format(
                    topic, message_name
                )
            )
            return
        message_class = self.resolve_message_name(message_name, "srv")
        if message_class is None:
            self.tcp_server.send_unity_error(
                "RegisterRosService({}, {}) - Unknown service class '{}'".format(
                    topic, message_name, message_name
                )
            )
            return

        if self._replace_owned_node(
            self.tcp_server.ros_services_table,
            self.tcp_server.ros_service_clients,
            topic,
            lambda: RosService(topic, message_class),
            "ROS service client",
        ):
            self.tcp_server.loginfo("RegisterRosService({}, {}) OK".format(topic, message_class))

    def unity_service(self, topic, message_name):
        if topic == "":
            self.tcp_server.send_unity_error(
                "RegisterUnityService({}, {}) - Can't register a blank topic name!".format(
                    topic, message_name
                )
            )
            return

        message_class = self.resolve_message_name(message_name, "srv")
        if message_class is None:
            self.tcp_server.send_unity_error(
                "RegisterUnityService({}, {}) - Unknown service class '{}'".format(
                    topic, message_name, message_name
                )
            )
            return

        if self._replace_owned_node(
            self.tcp_server.unity_services_table,
            self.tcp_server.unity_service_clients,
            topic,
            lambda: UnityService(str(topic), message_class, self.tcp_server),
            "Unity service",
        ):
            self.tcp_server.loginfo("RegisterUnityService({}, {}) OK".format(topic, message_class))

    def ros_action(self, action_name, action_type):
        if action_name == "":
            self.tcp_server.send_unity_error(
                "RegisterRosAction({}, {}) - Can't register a blank action name!".format(
                    action_name, action_type
                )
            )
            return
        original_name = action_name
        action_name = self._normalize_action_name(action_name)
        if original_name != action_name:
            self.tcp_server.loginfo(
                "RegisterRosAction: normalized action name '{}' -> '{}'".format(original_name, action_name)
            )
        action_class = self.resolve_message_name(action_type, "action")
        if action_class is None:
            self.tcp_server.send_unity_error(
                "RegisterRosAction({}, {}) - Unknown action class '{}'".format(
                    action_name, action_type, action_type
                )
            )
            return

        if self._replace_owned_node(
            self.tcp_server.ros_action_clients,
            self.tcp_server.ros_action_clients_owner,
            action_name,
            lambda: RosActionClient(action_name, action_class, self.tcp_server),
            "ROS action client",
        ):
            self.tcp_server.loginfo("RegisterRosAction({}, {}) OK".format(action_name, action_class))

    def unity_action(self, action_name, action_type):
        if action_name == "":
            self.tcp_server.send_unity_error(
                "RegisterUnityAction({}, {}) - Can't register a blank action name!".format(
                    action_name, action_type
                )
            )
            return
        original_name = action_name
        action_name = self._normalize_action_name(action_name)
        if original_name != action_name:
            self.tcp_server.loginfo(
                "RegisterUnityAction: normalized action name '{}' -> '{}'".format(original_name, action_name)
            )
        action_class = self.resolve_message_name(action_type, "action")
        if action_class is None:
            self.tcp_server.send_unity_error(
                "RegisterUnityAction({}, {}) - Unknown action class '{}'".format(
                    action_name, action_type, action_type
                )
            )
            return

        if self._replace_owned_node(
            self.tcp_server.unity_action_servers,
            self.tcp_server.unity_action_servers_owner,
            action_name,
            lambda: UnityActionServer(action_name, action_class, self.tcp_server),
            "Unity action server",
        ):
            self.tcp_server.loginfo("RegisterUnityAction({}, {}) OK".format(action_name, action_class))

    def action_goal(self, action_name, goal_id):
        # Normalize provided action name for consistent lookup
        original_name = action_name
        action_name = self._normalize_action_name(action_name)
        if original_name != action_name:
            self.tcp_server.loginfo(
                "action_goal: normalized action name '{}' -> '{}'".format(original_name, action_name)
            )
        if not self._active_client_owns(
            self.tcp_server.ros_action_clients_owner, action_name
        ):
            self.tcp_server.send_unity_error(
                "Action goal received for unowned ROS action '{}'".format(action_name),
                client_id=self._get_active_client_id(),
            )
            return
        self._set_pending_action(action_name, goal_id, "goal_to_ros")

    def action_feedback(self, action_name, goal_id):
        original_name = action_name
        action_name = self._normalize_action_name(action_name)
        if original_name != action_name:
            self.tcp_server.loginfo(
                "action_feedback: normalized action name '{}' -> '{}'".format(original_name, action_name)
            )
        if not self._active_client_owns(
            self.tcp_server.unity_action_servers_owner, action_name
        ):
            self.tcp_server.send_unity_error(
                "Action feedback received for unowned Unity action '{}'".format(action_name),
                client_id=self._get_active_client_id(),
            )
            return
        self._set_pending_action(action_name, goal_id, "feedback_to_ros")

    def action_result(self, action_name, goal_id, status=0):
        original_name = action_name
        action_name = self._normalize_action_name(action_name)
        if original_name != action_name:
            self.tcp_server.loginfo(
                "action_result: normalized action name '{}' -> '{}'".format(original_name, action_name)
            )
        if not self._active_client_owns(
            self.tcp_server.unity_action_servers_owner, action_name
        ):
            self.tcp_server.send_unity_error(
                "Action result received for unowned Unity action '{}'".format(action_name),
                client_id=self._get_active_client_id(),
            )
            return
        self._set_pending_action(action_name, goal_id, "result_to_ros", status=status)

    def action_cancel(self, action_name, goal_id):
        original_name = action_name
        action_name = self._normalize_action_name(action_name)
        if original_name != action_name:
            self.tcp_server.loginfo(
                "action_cancel: normalized action name '{}' -> '{}'".format(original_name, action_name)
            )
        client_id = self._get_active_client_id()
        if self.tcp_server.client_owns(
            self.tcp_server.ros_action_clients_owner, action_name, client_id
        ):
            self.tcp_server.cancel_ros_action_goal(
                action_name, goal_id, client_id=client_id
            )
        else:
            self.tcp_server.send_unity_error(
                "Action cancel received for unowned ROS action '{}'".format(action_name),
                client_id=client_id,
            )

    def response(self, srv_id):  # the next message is a service response
        client = self._get_active_client()
        if client is None:
            self.tcp_server.logwarn("Ignoring __response without an active client context")
            return
        client.set_pending_service(srv_id, is_request=False)

    def request(self, srv_id):  # the next message is a service request
        client = self._get_active_client()
        if client is None:
            self.tcp_server.logwarn("Ignoring __request without an active client context")
            return
        client.set_pending_service(srv_id, is_request=True)

    def topic_list(self):
        self.tcp_server.unity_tcp_sender.send_topic_list(client_id=self._get_active_client_id())

    def resolve_message_name(self, name, extension_hint=None):
        import importlib

        try:
            parts = name.split('/')
            if len(parts) == 3:
                package_name, extension, class_name = parts
            elif len(parts) == 2:
                package_name, class_name = parts
                extension = extension_hint or "msg"
            else:
                raise ValueError(f"Invalid ROS type string: {name}")

            if extension not in ["msg", "srv", "action"]:
                extension = "msg"

            # Construct proper module name
            # For all types, import from <package>.<extension> and get the class by name
            mod_name = f"{package_name}.{extension}"

            try:
                module = importlib.import_module(mod_name)
                msg_class = getattr(module, class_name)
                return msg_class
            except (ModuleNotFoundError, AttributeError):
                # If not found in the specified extension, try other extensions
                # This handles cases where action goal/result/feedback are registered with their ROS message name
                # but the actual class lives in the action module
                if extension == "msg":
                    for fallback_ext in ["action", "srv"]:
                        try:
                            fallback_mod_name = f"{package_name}.{fallback_ext}"
                            fallback_module = importlib.import_module(fallback_mod_name)
                            fallback_class = getattr(fallback_module, class_name)
                            
                            # Special handling for action types:
                            # If we found an action wrapper class (which cannot be instantiated),
                            # we need to return the Goal/Result/Feedback subclass instead.
                            # Try to instantiate it - if it fails with NotImplementedError, 
                            # it's an action wrapper class, so extract Goal subclass as fallback
                            if fallback_ext == "action":
                                try:
                                    # Try to instantiate to check if it's a wrapper class
                                    test_instance = fallback_class()
                                    # If successful, return the class as-is
                                    return fallback_class
                                except NotImplementedError:
                                    # It's an action wrapper class, extract the Goal subclass
                                    action_goal = getattr(fallback_class, "Goal", None)
                                    if action_goal is not None:
                                        self.tcp_server.loginfo(f"Resolved {name} to action Goal type (wrapper class detected)")
                                        return action_goal
                                    # If no Goal subclass, re-raise the error
                                    raise
                            
                            self.tcp_server.loginfo(f"Resolved {name} from {fallback_ext} module instead of {extension}")
                            return fallback_class
                        except (ModuleNotFoundError, AttributeError):
                            continue
                # If we get here, nothing worked
                raise

        except Exception as e:
            self.tcp_server.logerr(f"Failed to resolve {name}: {e}")
            return None


    def _set_pending_action(self, action_name, goal_id, phase, status=None):
        client = self._get_active_client()
        if client is None:
            self.tcp_server.logwarn("Ignoring pending action without an active client context")
            return
        client.set_pending_action(action_name, goal_id, phase, status=status)
