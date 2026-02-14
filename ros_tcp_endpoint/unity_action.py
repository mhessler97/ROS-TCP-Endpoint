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

"""ROS2 action server surface that proxies callbacks to Unity over TCP."""

import asyncio
import re
import threading

from action_msgs.msg import GoalStatus
from rclpy.action import ActionServer, GoalResponse, CancelResponse
from rclpy.serialization import deserialize_message

from .communication import RosReceiver


class UnityActionServer(RosReceiver):
    """ActionServer wrapper whose callbacks talk to Unity via TcpServer."""

    def __init__(self, action_name, action_type, tcp_server):
        stripped_name = re.sub("[^A-Za-z0-9_]+", "", action_name)
        node_name = f"{stripped_name}_UnityActionServer"
        super().__init__(node_name)

        self.action_name = action_name
        self.action_type = action_type
        self.tcp_server = tcp_server
        self._goal_handles = {}
        self._result_futures = {}
        self._lock = threading.Lock()
        self.result_timeout_sec = 10.0

        self.action_server = ActionServer(
            self,
            action_type,
            action_name,
            execute_callback=self._execute_callback,
            goal_callback=self._goal_callback,
            cancel_callback=self._cancel_callback,
        )

    def unregister(self):
        self.action_server.destroy()
        self.destroy_node()

    def handle_unity_feedback(self, goal_id, serialized_feedback):
        goal_handle = self._goal_handles.get(goal_id)
        if goal_handle is None:
            self.get_logger().warning(
                "Received Unity feedback for unknown goal %s on %s", goal_id, self.action_name
            )
            return

        feedback_msg = deserialize_message(serialized_feedback, self.action_type.Feedback)
        goal_handle.publish_feedback(feedback_msg)

    def handle_unity_result(self, goal_id, status, serialized_result):
        goal_handle = self._goal_handles.get(goal_id)
        if goal_handle is None:
            self.get_logger().warning(
                "Received Unity result for unknown goal %s on %s", goal_id, self.action_name
            )
            return

        result_msg = deserialize_message(serialized_result, self.action_type.Result)

        if status == GoalStatus.STATUS_SUCCEEDED:
            goal_handle.succeed()
        elif status == GoalStatus.STATUS_CANCELED:
            goal_handle.canceled()
        else:
            goal_handle.abort()

        future = None
        with self._lock:
            future = self._result_futures.pop(goal_id, None)
            if goal_id in self._goal_handles:
                del self._goal_handles[goal_id]

        if future is not None and not future.done():
            future.set_result(result_msg)

    # ------------------------------------------------------------------
    # ActionServer callbacks
    # ------------------------------------------------------------------

    def _goal_callback(self, goal_request):
        """Accept incoming goals and forward to Unity for execution."""
        self.get_logger().info("Forwarding goal to Unity for action %s" % self.action_name)
        return GoalResponse.ACCEPT

    def _cancel_callback(self, goal_handle):
        """Accept cancel requests; Unity cancellation pass-through TBD."""
        self.get_logger().info("Received cancel request for goal on %s" % self.action_name)
        goal_id = self._ros_uuid_to_str(goal_handle.goal_id)
        self.tcp_server.unity_tcp_sender.send_unity_action_cancel_request(
            self.action_name, goal_id
        )
        return CancelResponse.ACCEPT

    async def _execute_callback(self, goal_handle):
        goal_id = self._ros_uuid_to_str(goal_handle.goal_id)
        with self._lock:
            loop = asyncio.get_running_loop()
            result_future = loop.create_future()
            self._goal_handles[goal_id] = goal_handle
            self._result_futures[goal_id] = result_future

        self.tcp_server.unity_tcp_sender.send_unity_action_goal_request(
            self.action_name, goal_id, goal_handle.request
        )

        try:
            result_msg = await asyncio.wait_for(result_future, timeout=self.result_timeout_sec)
            return result_msg
        except asyncio.TimeoutError:
            self.get_logger().error(
                "Timed out waiting for Unity action result on %s for goal %s",
                self.action_name,
                goal_id,
            )
            with self._lock:
                self._result_futures.pop(goal_id, None)
                self._goal_handles.pop(goal_id, None)
            goal_handle.abort()
            return self.action_type.Result()

    def _ros_uuid_to_str(self, goal_id):
        return "".join(["{:02x}".format(b) for b in goal_id.uuid])
