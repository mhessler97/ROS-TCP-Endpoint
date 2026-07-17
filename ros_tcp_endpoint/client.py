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

from collections import deque
import json
import socket
import struct
import threading
import time

from rclpy.serialization import serialize_message


class ClientThread(threading.Thread):
    """
    Thread class to read all data from a connection and pass along the data to the
    desired source.
    """

    PAYLOAD_HEADER_COMMANDS = frozenset(
        ("__request", "__response", "__action_goal", "__action_feedback", "__action_result")
    )

    def __init__(self, conn, tcp_server, incoming_ip, incoming_port, client_id):
        """
        Set class variables
        Args:
            conn:
            tcp_server: server object
            incoming_ip: connected from this IP address
            incoming_port: connected from this port
        """
        self.conn = conn
        self.tcp_server = tcp_server
        self.incoming_ip = incoming_ip
        self.incoming_port = incoming_port
        self.client_id = client_id
        self.pending_srv_id = None
        self.pending_srv_is_request = False
        self.pending_action = None
        self.pending_payload_deadline = None
        self.pending_payload_timeout_sec = 5.0
        self.deferred_payload_headers = deque()
        threading.Thread.__init__(self)

    @staticmethod
    def recvall(conn, size, flags=0):
        """
        Receive exactly bufsize bytes from the socket.
        """
        buffer = bytearray(size)
        view = memoryview(buffer)
        pos = 0
        while pos < size:
            try:
                read = conn.recv_into(view[pos:], size - pos, flags)
            except socket.timeout:
                # If no bytes have been read yet, let the outer loop poll for
                # pending-protocol timeouts. Once bytes are partially consumed,
                # keep waiting to avoid corrupting frame boundaries.
                if pos == 0:
                    raise
                continue
            if not read:
                raise IOError("No more data available")
            pos += read
        return bytes(buffer)

    @staticmethod
    def read_int32(conn):
        """
        Reads four bytes from socket connection and unpacks them to an int

        Returns: int

        """
        raw_bytes = ClientThread.recvall(conn, 4)
        num = struct.unpack("<I", raw_bytes)[0]
        return num

    def read_string(self):
        """
        Reads int32 from socket connection to determine how many bytes to
        read to get the string that follows. Read that number of bytes and
        decode to utf-8 string.

        Returns: string

        """
        str_len = ClientThread.read_int32(self.conn)

        str_bytes = ClientThread.recvall(self.conn, str_len)
        decoded_str = str_bytes.decode("utf-8")

        return decoded_str

    def read_message(self, conn):
        """
        Decode destination and full message size from socket connection.
        Grab bytes in chunks until full message has been read.
        """
        data = b""

        destination = self.read_string()
        full_message_size = ClientThread.read_int32(conn)

        data = ClientThread.recvall(conn, full_message_size)

        if full_message_size > 0 and not data:
            self.tcp_server.logerr(
                "No data for a message size of {}, breaking!".format(full_message_size)
            )
            return

        destination = destination.rstrip("\x00")
        return destination, data

    @staticmethod
    def serialize_message(destination, message):
        """
        Serialize a destination and message class.

        Args:
            destination: name of destination
            message:     message class to serialize

        Returns:
            serialized destination and message as a list of bytes
        """
        dest_bytes = destination.encode("utf-8")
        length = len(dest_bytes)
        dest_info = struct.pack("<I%ss" % length, length, dest_bytes)

        serial_response = serialize_message(message)

        msg_length = struct.pack("<I", len(serial_response))
        serialized_message = dest_info + msg_length + serial_response

        return serialized_message

    @staticmethod
    def serialize_command(command, params):
        cmd_bytes = command.encode("utf-8")
        cmd_length = len(cmd_bytes)
        cmd_info = struct.pack("<I%ss" % cmd_length, cmd_length, cmd_bytes)

        json_bytes = json.dumps(params.__dict__).encode("utf-8")
        json_length = len(json_bytes)
        json_info = struct.pack("<I%ss" % json_length, json_length, json_bytes)

        return cmd_info + json_info

    def send_ros_service_request(self, srv_id, destination, data):
        ros_communicator = self.tcp_server.get_owned_registration(
            self.tcp_server.ros_services_table,
            self.tcp_server.ros_service_clients,
            destination,
            self.client_id,
        )
        if ros_communicator is None:
            error_msg = "Service destination '{}' is not registered! Known services are: {} ".format(
                destination,
                self.tcp_server.get_registration_keys(
                    self.tcp_server.ros_services_table
                ),
            )
            self.tcp_server.send_unity_error(error_msg, client_id=self.client_id)
            self.tcp_server.logerr(error_msg)
            # TODO: send a response to Unity anyway?
            return
        else:
            if not self.tcp_server.submit_service_call(
                self.service_call_thread, srv_id, destination, data, ros_communicator
            ):
                error_msg = "Unable to schedule service call '{}'".format(destination)
                self.tcp_server.send_unity_error(error_msg, client_id=self.client_id)
                self.tcp_server.logerr(error_msg)

    def service_call_thread(self, srv_id, destination, data, ros_communicator):
        response = ros_communicator.send(data)

        if not response:
            error_msg = "No response data from service '{}'!".format(destination)
            self.tcp_server.send_unity_error(error_msg, client_id=self.client_id)
            self.tcp_server.logerr(error_msg)
            # TODO: send a response to Unity anyway?
            return

        self.tcp_server.unity_tcp_sender.send_ros_service_response(
            srv_id, destination, response, client_id=self.client_id
        )

    def set_pending_service(self, srv_id, is_request):
        self.pending_srv_id = srv_id
        self.pending_srv_is_request = is_request
        self.pending_payload_deadline = time.monotonic() + self.pending_payload_timeout_sec

    def set_pending_action(self, action_name, goal_id, phase, status=None):
        self.pending_action = {
            "action_name": action_name,
            "goal_id": goal_id,
            "phase": phase,
            "status": status,
        }
        self.pending_payload_deadline = time.monotonic() + self.pending_payload_timeout_sec

    def clear_pending_payload(self):
        self.pending_srv_id = None
        self.pending_srv_is_request = False
        self.pending_action = None
        self.pending_payload_deadline = None

    def expire_pending_payload_if_needed(self):
        deadline = self.pending_payload_deadline
        if deadline is None:
            return
        if time.monotonic() < deadline:
            return

        if self.pending_srv_id is not None:
            self.tcp_server.send_unity_error(
                "Timed out waiting for service payload for srv_id {}".format(self.pending_srv_id),
                client_id=self.client_id,
            )
            self.tcp_server.logwarn(
                "Client {} timed out waiting for pending service payload".format(self.client_id)
            )
        elif self.pending_action is not None:
            self.tcp_server.send_unity_error(
                "Timed out waiting for action payload for '{}' goal {}".format(
                    self.pending_action.get("action_name"), self.pending_action.get("goal_id")
                ),
                client_id=self.client_id,
            )
            self.tcp_server.logwarn(
                "Client {} timed out waiting for pending action payload".format(self.client_id)
            )
        self.clear_pending_payload()
        self._activate_next_payload_header()

    def run(self):
        """
        Receive a message from Unity and determine where to send it based on the publishers table
         and topic string. Then send the read message.

        If there is a response after sending the serialized data, assume it is a
        ROS service response.

        Message format is expected to arrive as
            int: length of destination bytes
            str: destination. Publisher topic, Subscriber topic, Service name, etc
            int: size of full message
            msg: the ROS msg type as bytes

        """
        self.tcp_server.loginfo(
            "Connection from {}:{} (client_id={})".format(
                self.incoming_ip, self.incoming_port, self.client_id
            )
        )
        halt_event = threading.Event()
        self.conn.settimeout(0.2)
        self.tcp_server.unity_tcp_sender.start_sender(self.conn, halt_event, self.client_id)
        try:
            while not halt_event.is_set():
                try:
                    message = self.read_message(self.conn)
                except socket.timeout:
                    self.expire_pending_payload_if_needed()
                    continue
                if message is None:
                    break
                destination, data = message
                self.process_frame(destination, data)
        except IOError as e:
            self.tcp_server.logerr("Exception: {}".format(e))
        finally:
            halt_event.set()
            self.conn.close()
            self.tcp_server.on_client_disconnect(self.client_id)
            self.tcp_server.loginfo(
                "Disconnected from {}:{} (client_id={})".format(
                    self.incoming_ip, self.incoming_port, self.client_id
                )
            )

    def process_frame(self, destination, data):
        """Dispatch one complete TCP frame without corrupting pending payload state."""
        # Keepalives and system commands are self-describing frames. They may be
        # queued between a service/action header and its payload, so they must
        # never consume the pending payload slot.
        if destination == "":
            return
        if destination.startswith("__"):
            if self._has_pending_payload() and destination in self.PAYLOAD_HEADER_COMMANDS:
                self.deferred_payload_headers.append((destination, data))
                return
            self.tcp_server.handle_syscommand(destination, data, client_thread=self)
            return

        if self.pending_srv_id is not None:
            expected_table = (
                self.tcp_server.ros_services_table
                if self.pending_srv_is_request
                else self.tcp_server.unity_services_table
            )
            expected_owners = (
                self.tcp_server.ros_service_clients
                if self.pending_srv_is_request
                else self.tcp_server.unity_service_clients
            )
            is_expected_service = self.tcp_server.get_owned_registration(
                expected_table, expected_owners, destination, self.client_id
            ) is not None
            if not is_expected_service and self._try_publish_frame(destination, data):
                return

            # If we've been told that the next data frame is a service
            # request/response, process it as such.
            pending_srv_id = self.pending_srv_id
            pending_srv_is_request = self.pending_srv_is_request
            self.clear_pending_payload()
            if pending_srv_is_request:
                self.send_ros_service_request(pending_srv_id, destination, data)
            else:
                self.tcp_server.send_unity_service_response(
                    pending_srv_id, data, client_id=self.client_id
                )
            self._activate_next_payload_header()
            return

        if self.pending_action is not None:
            expected_action = self.pending_action.get("action_name")
            if destination != expected_action and self._try_publish_frame(destination, data):
                return
            self._handle_pending_action(destination, data)
            self._activate_next_payload_header()
            return

        if self._try_publish_frame(destination, data):
            return

        error_msg = "Not registered to publish topic '{}'! Valid publish topics are: {} ".format(
            destination,
            self.tcp_server.get_registration_keys(self.tcp_server.publishers_table),
        )
        self.tcp_server.send_unity_error(error_msg, client_id=self.client_id)
        self.tcp_server.logerr(error_msg)

    def _has_pending_payload(self):
        return self.pending_srv_id is not None or self.pending_action is not None

    def _activate_next_payload_header(self):
        while not self._has_pending_payload() and self.deferred_payload_headers:
            destination, data = self.deferred_payload_headers.popleft()
            self.tcp_server.handle_syscommand(destination, data, client_thread=self)

    def _try_publish_frame(self, destination, data):
        ros_communicator = self.tcp_server.get_owned_registration(
            self.tcp_server.publishers_table,
            self.tcp_server.publisher_clients,
            destination,
            self.client_id,
        )
        if ros_communicator is not None:
            ros_communicator.send(data)
            return True
        return False

    def _handle_pending_action(self, destination, data):
        action_context = self.pending_action
        self.clear_pending_payload()

        if action_context is None:
            return

        action_name = action_context.get("action_name", destination)
        phase = action_context.get("phase")

        if phase == "goal_to_ros":
            self._deliver_ros_action_goal(action_name, action_context.get("goal_id"), destination, data)
        elif phase == "feedback_to_ros":
            self._deliver_unity_action_feedback(action_name, action_context.get("goal_id"), data)
        elif phase == "result_to_ros":
            self._deliver_unity_action_result(
                action_name, action_context.get("goal_id"), action_context.get("status"), data
            )
        else:
            self.tcp_server.logwarn("Unhandled action phase '{}'; dropping payload.".format(phase))

    def _deliver_ros_action_goal(self, action_name, goal_id, destination, data):
        if action_name is None:
            action_name = destination

        action_client = self.tcp_server.get_owned_registration(
            self.tcp_server.ros_action_clients,
            self.tcp_server.ros_action_clients_owner,
            action_name,
            self.client_id,
        )
        if action_client is None:
            # Provide better diagnostics to help track mismatches/race conditions
            known = self.tcp_server.get_registration_keys(
                self.tcp_server.ros_action_clients
            )
            error_msg = "Action goal received for unregistered action '{}' (known actions: {})".format(
                action_name, known
            )
            self.tcp_server.send_unity_error(error_msg, client_id=self.client_id)
            self.tcp_server.logerr(error_msg)
            return

        action_client.send_goal(goal_id, data, client_id=self.client_id)

    def _deliver_unity_action_feedback(self, action_name, goal_id, data):
        action_server = self.tcp_server.get_owned_registration(
            self.tcp_server.unity_action_servers,
            self.tcp_server.unity_action_servers_owner,
            action_name,
            self.client_id,
        )
        if action_server is None:
            error_msg = "Action feedback received for unregistered Unity action '{}'".format(action_name)
            self.tcp_server.send_unity_error(error_msg, client_id=self.client_id)
            self.tcp_server.logerr(error_msg)
            return

        action_server.handle_unity_feedback(goal_id, data)

    def _deliver_unity_action_result(self, action_name, goal_id, status, data):
        action_server = self.tcp_server.get_owned_registration(
            self.tcp_server.unity_action_servers,
            self.tcp_server.unity_action_servers_owner,
            action_name,
            self.client_id,
        )
        if action_server is None:
            error_msg = "Action result received for unregistered Unity action '{}'".format(action_name)
            self.tcp_server.send_unity_error(error_msg, client_id=self.client_id)
            self.tcp_server.logerr(error_msg)
            return

        action_server.handle_unity_result(goal_id, status, data)
