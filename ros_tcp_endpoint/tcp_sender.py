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

import threading
import json

from rclpy.serialization import deserialize_message

from .client import ClientThread
from .thread_pauser import ThreadPauser

# queue module was renamed between python 2 and 3
try:
    from queue import Queue
    from queue import Empty
except:
    from Queue import Queue
    from Queue import Empty


class UnityTcpSender:
    """
    Sends messages to Unity.
    """

    def __init__(self, tcp_server):
        self.sender_id = 1
        self.time_between_halt_checks = 5
        self.tcp_server = tcp_server

        # Each connected client has a dedicated outgoing queue.
        self.client_queues = {}
        self.queue_lock = threading.Lock()

        # variables needed for matching up unity service requests with responses
        self.next_srv_id = 1001
        self.srv_lock = threading.Lock()
        self.services_waiting = {}
        self.unity_service_timeout_sec = 5.0

    def _get_queue(self, client_id):
        with self.queue_lock:
            return self.client_queues.get(client_id)

    def _enqueue(self, payload, client_id=None):
        if client_id is not None:
            queue = self._get_queue(client_id)
            if queue is None:
                return False
            queue.put(payload)
            return True

        with self.queue_lock:
            queues = list(self.client_queues.values())
        if not queues:
            return False
        for queue in queues:
            queue.put(payload)
        return True

    def _resolve_owner(self, owner_table, key, client_id=None):
        if client_id is not None:
            return client_id
        return owner_table.get(key)

    def send_unity_info(self, text, client_id=None):
        command = SysCommand_Log()
        command.text = text
        serialized_bytes = ClientThread.serialize_command("__log", command)
        self._enqueue(serialized_bytes, client_id=client_id)

    def send_unity_warning(self, text, client_id=None):
        command = SysCommand_Log()
        command.text = text
        serialized_bytes = ClientThread.serialize_command("__warn", command)
        self._enqueue(serialized_bytes, client_id=client_id)

    def send_unity_error(self, text, client_id=None):
        command = SysCommand_Log()
        command.text = text
        serialized_bytes = ClientThread.serialize_command("__error", command)
        self._enqueue(serialized_bytes, client_id=client_id)

    def send_ros_service_response(self, srv_id, destination, response, client_id=None):
        command = SysCommand_Service()
        command.srv_id = srv_id
        serialized_header = ClientThread.serialize_command("__response", command)
        serialized_message = ClientThread.serialize_message(destination, response)
        if not self._enqueue(b"".join([serialized_header, serialized_message]), client_id=client_id):
            self.tcp_server.logwarn(
                "Dropping ROS service response {} for '{}' because client queue is unavailable".format(
                    srv_id, destination
                )
            )

    def send_unity_message(self, topic, message, client_id=None):
        target_client_id = self._resolve_owner(
            self.tcp_server.subscriber_clients, topic, client_id=client_id
        )
        if target_client_id is None:
            self.tcp_server.logwarn(
                "Dropping message for topic '{}' because no client owns this subscription".format(
                    topic
                )
            )
            return
        serialized_message = ClientThread.serialize_message(topic, message)
        if not self._enqueue(serialized_message, client_id=target_client_id):
            self.tcp_server.logwarn(
                "Dropping message for topic '{}' because target client queue is unavailable".format(
                    topic
                )
            )

    def send_action_feedback(self, topic, goal_id, feedback_msg):
        target_client_id = self._resolve_owner(self.tcp_server.ros_action_clients_owner, topic)
        if target_client_id is None:
            self.tcp_server.logwarn(
                "Dropping action feedback for '{}' goal {} because no client owns this action".format(
                    topic, goal_id
                )
            )
            return
        header = SysCommand_Action()
        header.goal_id = goal_id
        header.action_name = topic
        serialized_header = ClientThread.serialize_command("__action_feedback", header)
        serialized_message = ClientThread.serialize_message(topic, feedback_msg)
        if not self._enqueue(b"".join([serialized_header, serialized_message]), client_id=target_client_id):
            self.tcp_server.logwarn(
                "Dropping action feedback for '{}' goal {} because target client queue is unavailable".format(
                    topic, goal_id
                )
            )

    def send_action_result(self, topic, goal_id, status, result_msg):
        target_client_id = self._resolve_owner(self.tcp_server.ros_action_clients_owner, topic)
        if target_client_id is None:
            self.tcp_server.logwarn(
                "Dropping action result for '{}' goal {} because no client owns this action".format(
                    topic, goal_id
                )
            )
            return
        header = SysCommand_Action()
        header.goal_id = goal_id
        header.status = status
        header.action_name = topic
        serialized_header = ClientThread.serialize_command("__action_result", header)
        serialized_message = ClientThread.serialize_message(topic, result_msg)
        if not self._enqueue(b"".join([serialized_header, serialized_message]), client_id=target_client_id):
            self.tcp_server.logwarn(
                "Dropping action result for '{}' goal {} because target client queue is unavailable".format(
                    topic, goal_id
                )
            )

    def send_action_goal_response(
        self, action_name, goal_id, accepted, ros_goal_id="", message=""
    ):
        target_client_id = self._resolve_owner(
            self.tcp_server.ros_action_clients_owner, action_name
        )
        if target_client_id is None:
            self.tcp_server.logwarn(
                "Dropping action goal response for '{}' goal {} because no client owns this action".format(
                    action_name, goal_id
                )
            )
            return
        header = SysCommand_ActionGoalResponse()
        header.goal_id = goal_id
        header.action_name = action_name
        header.accepted = bool(accepted)
        header.ros_goal_id = ros_goal_id or ""
        header.message = message or ""
        serialized_header = ClientThread.serialize_command("__action_goal_response", header)
        if not self._enqueue(serialized_header, client_id=target_client_id):
            self.tcp_server.logwarn(
                "Dropping action goal response for '{}' goal {} because target client queue is unavailable".format(
                    action_name, goal_id
                )
            )

    def send_unity_action_goal_request(self, action_name, goal_id, goal_msg):
        target_client_id = self._resolve_owner(
            self.tcp_server.unity_action_servers_owner, action_name
        )
        if target_client_id is None:
            self.tcp_server.logwarn(
                "Cannot forward action goal {} for {} because no Unity action owner is registered".format(
                    goal_id, action_name
                )
            )
            return

        header = SysCommand_Action()
        header.goal_id = goal_id
        header.action_name = action_name
        serialized_header = ClientThread.serialize_command("__action_goal_request", header)
        serialized_message = ClientThread.serialize_message(action_name, goal_msg)
        if not self._enqueue(b"".join([serialized_header, serialized_message]), client_id=target_client_id):
            self.tcp_server.logwarn(
                "Cannot forward action goal {} for {} because Unity queue is unavailable".format(
                    goal_id, action_name
                )
            )

    def send_unity_action_cancel_request(self, action_name, goal_id):
        target_client_id = self._resolve_owner(
            self.tcp_server.unity_action_servers_owner, action_name
        )
        if target_client_id is None:
            self.tcp_server.logwarn(
                "Cannot forward action cancel {} for {} because no Unity action owner is registered".format(
                    goal_id, action_name
                )
            )
            return

        header = SysCommand_Action()
        header.goal_id = goal_id
        header.action_name = action_name
        serialized_header = ClientThread.serialize_command("__action_cancel_request", header)
        if not self._enqueue(serialized_header, client_id=target_client_id):
            self.tcp_server.logwarn(
                "Cannot forward action cancel {} for {} because Unity queue is unavailable".format(
                    goal_id, action_name
                )
            )

    def send_unity_service_request(self, topic, service_class, request, client_id=None):
        target_client_id = self._resolve_owner(
            self.tcp_server.unity_service_clients, topic, client_id=client_id
        )
        if target_client_id is None:
            self.tcp_server.logerr(
                "No Unity client registered for service '{}'".format(topic)
            )
            return None

        thread_pauser = ThreadPauser()
        with self.srv_lock:
            srv_id = self.next_srv_id
            self.next_srv_id += 1
            self.services_waiting[srv_id] = {
                "pauser": thread_pauser,
                "client_id": target_client_id,
            }

        command = SysCommand_Service()
        command.srv_id = srv_id
        serialized_header = ClientThread.serialize_command("__request", command)
        serialized_message = ClientThread.serialize_message(topic, request)
        if not self._enqueue(b"".join([serialized_header, serialized_message]), client_id=target_client_id):
            with self.srv_lock:
                self.services_waiting.pop(srv_id, None)
            self.tcp_server.logerr(
                "Unable to send Unity service request {} for '{}' because target queue is unavailable".format(
                    srv_id, topic
                )
            )
            return None

        resumed = thread_pauser.sleep_until_resumed(timeout_sec=self.unity_service_timeout_sec)
        if not resumed:
            with self.srv_lock:
                self.services_waiting.pop(srv_id, None)
            self.tcp_server.logerr(
                "Timed out waiting for Unity service response {} for '{}'".format(srv_id, topic)
            )
            return None

        if thread_pauser.result is None:
            return None

        try:
            return deserialize_message(thread_pauser.result, service_class.Response())
        except Exception as exc:  # noqa: pylint: disable=broad-except
            self.tcp_server.logerr(
                "Failed to deserialize Unity service response {} for '{}': {}".format(
                    srv_id, topic, exc
                )
            )
            return None

    def send_unity_service_response(self, srv_id, data, client_id=None):
        with self.srv_lock:
            pending = self.services_waiting.get(srv_id)
            if pending is None:
                self.tcp_server.logwarn(
                    "Dropping unexpected Unity service response for unknown srv_id {}".format(
                        srv_id
                    )
                )
                return

            expected_client_id = pending.get("client_id")
            if (
                client_id is not None
                and expected_client_id is not None
                and expected_client_id != client_id
            ):
                self.tcp_server.logwarn(
                    "Ignoring Unity service response for srv_id {} from client {} (expected client {})".format(
                        srv_id, client_id, expected_client_id
                    )
                )
                return

            thread_pauser = pending.get("pauser")
            del self.services_waiting[srv_id]

        if thread_pauser is not None:
            thread_pauser.resume_with_result(data)

    def remove_client(self, client_id):
        with self.queue_lock:
            self.client_queues.pop(client_id, None)

        stale_pausers = []
        with self.srv_lock:
            stale_srv_ids = [
                srv_id
                for srv_id, pending in self.services_waiting.items()
                if pending.get("client_id") == client_id
            ]
            for srv_id in stale_srv_ids:
                pending = self.services_waiting.pop(srv_id, None)
                if pending is not None:
                    stale_pausers.append(pending.get("pauser"))

        for thread_pauser in stale_pausers:
            if thread_pauser is not None:
                thread_pauser.resume_with_result(None)

    def get_registered_topic(self, topic):
        if topic in self.tcp_server.publishers_table:
            return self.tcp_server.publishers_table[topic]
        elif topic in self.tcp_server.subscribers_table:
            return self.tcp_server.subscribers_table[topic]
        elif topic in self.tcp_server.ros_services_table:
            return self.tcp_server.ros_services_table[topic]
        elif topic in self.tcp_server.unity_services_table:
            return self.tcp_server.unity_services_table[topic]
        else:
            return None

    def send_topic_list(self, client_id=None):
        topic_list = SysCommand_TopicsResponse()
        topics_and_types = self.tcp_server.get_topic_names_and_types()
        topic_list.topics = [item[0] for item in topics_and_types]
        topic_list.types = []

        for topic_name, resolved_types in topics_and_types:
            if not resolved_types:
                topic_list.types.append("")
                continue
            if len(resolved_types) <= 1:
                topic_list.types.append(resolved_types[0].replace("/msg/", "/"))
                continue

            node = self.get_registered_topic(topic_name)
            parsed_type = self.parse_message_name(getattr(node, "msg", None))
            if parsed_type is None:
                parsed_type = resolved_types[0].replace("/msg/", "/")
            topic_list.types.append(parsed_type)

            if node is not None:
                self.tcp_server.get_logger().warning(
                    "Only one message type per topic is supported, but found multiple types for topic {}; maintaining {} as the subscribed type.".format(
                        topic_name, parsed_type
                    )
                )

        serialized_bytes = ClientThread.serialize_command("__topic_list", topic_list)
        self._enqueue(serialized_bytes, client_id=client_id)

    def start_sender(self, conn, halt_event, client_id):
        sender_thread = threading.Thread(
            target=self.sender_loop, args=(conn, self.sender_id, halt_event, client_id)
        )
        self.sender_id += 1

        # Exit the server thread when the main thread terminates
        sender_thread.daemon = True
        sender_thread.start()

    def sender_loop(self, conn, tid, halt_event, client_id):
        local_queue = Queue()

        # send a handshake message to confirm the connection and version number
        handshake_metadata = SysCommand_Handshake_Metadata()
        handshake = SysCommand_Handshake(handshake_metadata)
        local_queue.put(ClientThread.serialize_command("__handshake", handshake))

        with self.queue_lock:
            self.client_queues[client_id] = local_queue

        try:
            while not halt_event.is_set():
                try:
                    item = local_queue.get(timeout=self.time_between_halt_checks)
                except Empty:
                    # I'd like to just wait on the queue, but we also need to check occasionally for the connection being closed
                    # (otherwise the thread never terminates.)
                    continue

                # print("Sender {} sending an item".format(tid))

                try:
                    conn.sendall(item)
                except Exception as e:
                    self.tcp_server.logerr("Exception {}".format(e))
                    break
        finally:
            halt_event.set()
            with self.queue_lock:
                queue = self.client_queues.get(client_id)
                if queue is local_queue:
                    del self.client_queues[client_id]

    def parse_message_name(self, msg_class):
        try:
            # Example input string: <class 'std_msgs.msg._string.Metaclass_String'>
            module_path = msg_class.__module__
            class_name = msg_class.__name__

            if ".action." in module_path:
                pkg = module_path.split(".")[0]
                return f"{pkg}/action/{class_name}"
            elif ".msg." in module_path:
                pkg = module_path.split(".")[0]
                return f"{pkg}/msg/{class_name}"
            else:
                return f"{module_path}/{class_name}"
        except (IndexError, AttributeError, ImportError) as e:
            self.tcp_server.logerr("Failed to resolve message name: {}".format(e))
            return None


class SysCommand_Log:
    def __init__(self):
        text = ""


class SysCommand_Service:
    def __init__(self):
        srv_id = 0


class SysCommand_TopicsResponse:
    def __init__(self):
        topics = []
        types = []


class SysCommand_Action:
    def __init__(self):
        self.goal_id = ""
        self.status = 0
        self.action_name = ""

class SysCommand_ActionGoalResponse:
    def __init__(self):
        self.goal_id = ""
        self.action_name = ""
        self.accepted = False
        self.ros_goal_id = ""
        self.message = ""


class SysCommand_Handshake:
    def __init__(self, metadata):
        self.version = "v0.8.0"
        self.metadata = json.dumps(metadata.__dict__)


class SysCommand_Handshake_Metadata:
    def __init__(self):
        self.protocol = "ROS2"
        self.features = ["actions-preview"]
