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

"""ROS2 action client bridge for Unity TCP endpoint."""

import re
import threading
from functools import partial

from rclpy.action import ActionClient
from action_msgs.msg import GoalStatus
from action_msgs.srv import CancelGoal

from .communication import RosSender, deserialize_ros_message


class RosActionClient(RosSender):
    """Proxy that forwards Unity action goals into the ROS graph."""

    def __init__(self, action_name, action_type, tcp_server):
        stripped_name = re.sub("[^A-Za-z0-9_]+", "", action_name)
        node_name = f"{stripped_name}_RosActionClient"
        super().__init__(node_name)

        self.action_name = action_name
        self.action_type = action_type
        self.tcp_server = tcp_server
        self._client = ActionClient(self, action_type, action_name)
        self._inflight_goals = {}
        self._pending_goals = {}
        self._ros_goal_lookup = {}
        self._lock = threading.Lock()
        self._server_health_timer = self.create_timer(0.1, self._check_server_health)

        # Ensure the ROS action server is discoverable before Unity sends goals.
        if not self._client.wait_for_server(timeout_sec=5.0):
            self.get_logger().warning(
                "Action server '%s' not reachable within timeout; goals may fail" % action_name
            )

    def wait_for_server(self, timeout_sec=0.0):
        """Expose ActionClient.wait_for_server for health checks."""
        return self._client.wait_for_server(timeout_sec=timeout_sec)

    def send_goal(self, goal_uuid, serialized_goal, client_id=None):
        """Deserialize Unity payload and dispatch ROS action goal."""
        if not self._client.server_is_ready():
            message = (
                f"Action server '{self.action_name}' is not ready; ignoring goal {goal_uuid}"
            )
            self.tcp_server.unity_tcp_sender.send_action_goal_response(
                self.action_name,
                goal_uuid,
                False,
                message=message,
                client_id=client_id,
            )
            self._emit_unity_error(message, client_id=client_id)
            return

        try:
            goal_msg = deserialize_ros_message(serialized_goal, self.action_type.Goal)
        except Exception as exc:  # noqa: pylint: disable=broad-except
            message = (
                f"Failed to deserialize goal {goal_uuid} for '{self.action_name}' "
                f"(payload_len={len(serialized_goal)}, "
                f"prefix={bytes(serialized_goal[:16]).hex()}): {exc}"
            )
            self.tcp_server.unity_tcp_sender.send_action_goal_response(
                self.action_name,
                goal_uuid,
                False,
                message=message,
                client_id=client_id,
            )
            self._emit_unity_error(message, client_id=client_id)
            return
        with self._lock:
            self._inflight_goals[goal_uuid] = client_id
        try:
            goal_future = self._client.send_goal_async(
                goal_msg,
                feedback_callback=self._make_feedback_callback(goal_uuid, client_id),
            )
        except Exception as exc:  # noqa: pylint: disable=broad-except
            with self._lock:
                self._inflight_goals.pop(goal_uuid, None)
            message = (
                f"Failed to send goal {goal_uuid} to '{self.action_name}': {exc}"
            )
            self.tcp_server.unity_tcp_sender.send_action_goal_response(
                self.action_name,
                goal_uuid,
                False,
                message=message,
                client_id=client_id,
            )
            self._emit_unity_error(message, client_id=client_id)
            return
        goal_future.add_done_callback(partial(self._on_goal_response, goal_uuid, client_id))

    def cancel_goal(self, goal_uuid, client_id=None):
        """Request cancellation of a tracked goal."""
        goal_handle = None
        with self._lock:
            goal_meta = self._pending_goals.get(goal_uuid)
            if goal_meta is not None and (
                client_id is None or goal_meta.get("client_id") == client_id
            ):
                goal_handle = goal_meta.get("handle")

        if goal_handle is None:
            self.get_logger().warning(
                f"Received cancel for unknown goal {goal_uuid} on {self.action_name}"
            )
            return

        cancel_future = goal_handle.cancel_goal_async()
        self.get_logger().info(
            "Forwarded cancel request for goal %s on %s"
            % (goal_uuid, self.action_name)
        )
        cancel_future.add_done_callback(partial(self._on_cancel_response, goal_uuid))

    def unregister(self):
        """Tear down the underlying rclpy node."""
        self._client.destroy()
        self.destroy_node()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _make_feedback_callback(self, unity_goal_id, client_id):
        def _callback(feedback_msg):
            ros_goal_id = self._ros_uuid_to_str(feedback_msg.goal_id)
            with self._lock:
                mapped_goal_id = self._ros_goal_lookup.get(ros_goal_id, unity_goal_id)
            self.tcp_server.unity_tcp_sender.send_action_feedback(
                self.action_name,
                mapped_goal_id,
                feedback_msg.feedback,
                client_id=client_id,
            )

        return _callback

    def _on_goal_response(self, unity_goal_id, client_id, future):
        try:
            goal_handle = future.result()
        except Exception as exc:  # noqa pylint: disable=broad-except
            with self._lock:
                if unity_goal_id not in self._inflight_goals:
                    return
                self._inflight_goals.pop(unity_goal_id)
            self.tcp_server.unity_tcp_sender.send_action_goal_response(
                self.action_name,
                unity_goal_id,
                False,
                message=str(exc),
                client_id=client_id,
            )
            self._emit_unity_error(
                f"Failed to send goal {unity_goal_id} to '{self.action_name}': {exc}",
                client_id=client_id,
            )
            return

        if not goal_handle.accepted:
            with self._lock:
                if unity_goal_id not in self._inflight_goals:
                    return
                self._inflight_goals.pop(unity_goal_id)
            self.tcp_server.unity_tcp_sender.send_action_goal_response(
                self.action_name,
                unity_goal_id,
                False,
                message="rejected",
                client_id=client_id,
            )
            self._emit_unity_error(
                f"Action server '{self.action_name}' rejected goal {unity_goal_id}",
                client_id=client_id,
            )
            return

        ros_goal_id = self._ros_uuid_to_str(goal_handle.goal_id)
        with self._lock:
            if unity_goal_id not in self._inflight_goals:
                # The action server health check already rejected this goal.
                return
            self._inflight_goals.pop(unity_goal_id)
            self._pending_goals[unity_goal_id] = {
                "handle": goal_handle,
                "ros_goal_id": ros_goal_id,
                "client_id": client_id,
            }
            self._ros_goal_lookup[ros_goal_id] = unity_goal_id
            # Queue acceptance before the health timer can emit a terminal result.
            self.tcp_server.unity_tcp_sender.send_action_goal_response(
                self.action_name,
                unity_goal_id,
                True,
                ros_goal_id=ros_goal_id,
                client_id=client_id,
            )
        self.get_logger().info(
            "Goal %s accepted on %s (ROS id %s)"
            % (unity_goal_id, self.action_name, ros_goal_id)
        )

        result_future = goal_handle.get_result_async()
        result_future.add_done_callback(
            partial(self._on_result_response, unity_goal_id, client_id)
        )

    def _on_result_response(self, unity_goal_id, client_id, future):
        goal_meta = self._take_goal(unity_goal_id)
        if goal_meta is None:
            # A server shutdown or another terminal path already completed it.
            return

        try:
            result = future.result()
        except Exception as exc:  # noqa pylint: disable=broad-except
            self.tcp_server.unity_tcp_sender.send_action_result(
                self.action_name,
                unity_goal_id,
                GoalStatus.STATUS_ABORTED,
                self.action_type.Result(),
                client_id=client_id,
            )
            self._emit_unity_error(
                f"Result future for goal {unity_goal_id} on '{self.action_name}' failed: {exc}",
                client_id=client_id,
            )
            return

        status = getattr(result, "status", GoalStatus.STATUS_UNKNOWN)
        result_msg = getattr(result, "result", None)
        if result_msg is None:
            self.tcp_server.unity_tcp_sender.send_action_result(
                self.action_name,
                unity_goal_id,
                GoalStatus.STATUS_ABORTED,
                self.action_type.Result(),
                client_id=client_id,
            )
            self._emit_unity_error(
                f"No result payload for goal {unity_goal_id} on '{self.action_name}'",
                client_id=client_id,
            )
        else:
            self.tcp_server.unity_tcp_sender.send_action_result(
                self.action_name,
                unity_goal_id,
                status,
                result_msg,
                client_id=client_id,
            )

    def _on_cancel_response(self, unity_goal_id, future):
        try:
            response = future.result()
            if response.return_code != CancelGoal.Response.ERROR_NONE:
                self.get_logger().warning(
                    "Cancel request for goal %s on %s returned %s"
                    % (unity_goal_id, self.action_name, response.return_code)
                )
            else:
                self.get_logger().info(
                    "Cancel acknowledged for goal %s on %s"
                    % (unity_goal_id, self.action_name)
                )
        except Exception as exc:  # noqa pylint: disable=broad-except
            self.get_logger().error(
                "Cancel future for goal %s on %s failed: %s"
                % (unity_goal_id, self.action_name, exc)
            )

    def _take_goal(self, unity_goal_id):
        with self._lock:
            goal_meta = self._pending_goals.pop(unity_goal_id, None)
            if goal_meta:
                ros_goal_id = goal_meta.get("ros_goal_id")
                if ros_goal_id in self._ros_goal_lookup:
                    del self._ros_goal_lookup[ros_goal_id]
            return goal_meta

    def _check_server_health(self):
        """Finish TCP-side requests if a ROS action server disappears."""
        if self._client.server_is_ready():
            return

        with self._lock:
            inflight_goals = list(self._inflight_goals.items())
            self._inflight_goals.clear()
            pending_goal_ids = list(self._pending_goals.keys())

        for goal_id, client_id in inflight_goals:
            message = (
                f"Action server '{self.action_name}' shut down while accepting goal "
                f"{goal_id}"
            )
            self.tcp_server.unity_tcp_sender.send_action_goal_response(
                self.action_name,
                goal_id,
                False,
                message=message,
                client_id=client_id,
            )
            self._emit_unity_error(message, client_id=client_id)

        for goal_id in pending_goal_ids:
            goal_meta = self._take_goal(goal_id)
            if goal_meta is None:
                continue
            client_id = goal_meta.get("client_id")
            self.tcp_server.unity_tcp_sender.send_action_result(
                self.action_name,
                goal_id,
                GoalStatus.STATUS_ABORTED,
                self.action_type.Result(),
                client_id=client_id,
            )
            self._emit_unity_error(
                f"Action server '{self.action_name}' shut down while waiting for "
                f"goal {goal_id}",
                client_id=client_id,
            )

    def _ros_uuid_to_str(self, goal_id):
        return "".join(["{:02x}".format(b) for b in goal_id.uuid])

    def _emit_unity_error(self, message, client_id=None):
        self.tcp_server.send_unity_error(message, client_id=client_id)
        self.get_logger().error(message)
