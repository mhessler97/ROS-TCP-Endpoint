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

import re
import threading
import time

from action_msgs.msg import GoalStatus
from rclpy.action import ActionServer, GoalResponse, CancelResponse
from rclpy.serialization import deserialize_message
from rclpy.task import Future

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
        self._result_timers = {}
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._accepted_goals_awaiting_execution = 0
        self._closing = False
        self._shutdown_prepared = False
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
        self.prepare_unregister()
        self.action_server.destroy()
        self.destroy_node()

    def prepare_unregister(self, timeout_sec=2.0):
        """Finish accepted goals before the executor removes this action server."""
        deadline = time.monotonic() + timeout_sec
        with self._condition:
            if self._shutdown_prepared:
                return
            self._closing = True
            while self._accepted_goals_awaiting_execution and time.monotonic() < deadline:
                self._condition.wait(timeout=max(0.0, deadline - time.monotonic()))
            pending_goal_ids = list(self._result_futures.keys())

        pending_ros_goal_ids = []
        for goal_id in pending_goal_ids:
            pending = self._take_goal(goal_id)
            if pending is None:
                continue
            goal_handle, future = pending
            pending_ros_goal_ids.append(bytes(goal_handle.goal_id.uuid))
            try:
                goal_handle.abort()
            except Exception as exc:  # noqa: pylint: disable=broad-except
                self.get_logger().warning(
                    "Failed to abort Unity action goal {} during shutdown: {}".format(
                        goal_id, exc
                    )
                )
            if not future.done():
                future.set_result(self.action_type.Result())

        # Jazzy's ActionServer completes its internal result future immediately
        # after our execute callback returns.  Wait for those futures while the
        # node is still in the executor so pending ROS clients receive an ABORTED
        # result before the action server handle is destroyed.
        internal_futures = getattr(self.action_server, "_result_futures", {})
        futures_to_finish = [
            internal_futures[goal_id]
            for goal_id in pending_ros_goal_ids
            if goal_id in internal_futures
        ]
        while (
            any(not future.done() for future in futures_to_finish)
            and time.monotonic() < deadline
        ):
            time.sleep(0.005)

        if any(not future.done() for future in futures_to_finish):
            self.get_logger().warning(
                "Timed out draining pending goals while unregistering Unity action {}".format(
                    self.action_name
                )
            )
        with self._condition:
            self._shutdown_prepared = True

    def handle_unity_feedback(self, goal_id, serialized_feedback):
        with self._lock:
            goal_handle = self._goal_handles.get(goal_id)
        if goal_handle is None:
            self.get_logger().warning(
                "Received Unity feedback for unknown goal {} on {}".format(
                    goal_id, self.action_name
                )
            )
            return

        try:
            feedback_msg = deserialize_message(serialized_feedback, self.action_type.Feedback)
            goal_handle.publish_feedback(feedback_msg)
        except Exception as exc:  # noqa: pylint: disable=broad-except
            self.get_logger().error(
                "Failed to process Unity feedback for goal {} on {}: {}".format(
                    goal_id, self.action_name, exc
                )
            )

    def handle_unity_result(self, goal_id, status, serialized_result):
        try:
            result_msg = deserialize_message(serialized_result, self.action_type.Result)
        except Exception as exc:  # noqa: pylint: disable=broad-except
            if not self._fail_goal(goal_id, exc):
                self.get_logger().warning(
                    "Received invalid Unity result for unknown goal {} on {}".format(
                        goal_id, self.action_name
                    )
                )
            return

        pending = self._take_goal(goal_id)
        if pending is None:
            self.get_logger().warning(
                "Received Unity result for unknown goal {} on {}".format(
                    goal_id, self.action_name
                )
            )
            return

        goal_handle, future = pending

        if status == GoalStatus.STATUS_SUCCEEDED:
            goal_handle.succeed()
        elif status == GoalStatus.STATUS_CANCELED:
            goal_handle.canceled()
        else:
            goal_handle.abort()

        future.set_result(result_msg)

    # ------------------------------------------------------------------
    # ActionServer callbacks
    # ------------------------------------------------------------------

    def _goal_callback(self, goal_request):
        """Accept incoming goals and forward to Unity for execution."""
        with self._condition:
            if self._closing:
                self.get_logger().warning(
                    "Rejecting goal because Unity action {} is shutting down".format(
                        self.action_name
                    )
                )
                return GoalResponse.REJECT
            self._accepted_goals_awaiting_execution += 1
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
        with self._condition:
            if self._accepted_goals_awaiting_execution:
                self._accepted_goals_awaiting_execution -= 1
            closing = self._closing
            self._condition.notify_all()

        if closing:
            goal_handle.abort()
            return self.action_type.Result()

        result_future = Future()
        timeout_timer = threading.Timer(
            self.result_timeout_sec,
            self._timeout_goal,
            args=(goal_id,),
        )
        timeout_timer.daemon = True

        with self._lock:
            self._goal_handles[goal_id] = goal_handle
            self._result_futures[goal_id] = result_future
            self._result_timers[goal_id] = timeout_timer

        timeout_timer.start()

        self.tcp_server.unity_tcp_sender.send_unity_action_goal_request(
            self.action_name, goal_id, goal_handle.request
        )

        try:
            return await result_future
        except TimeoutError:
            self.get_logger().error(
                "Timed out waiting for Unity action result on {} for goal {}".format(
                    self.action_name, goal_id
                )
            )
            goal_handle.abort()
            return self.action_type.Result()
        except Exception as exc:  # noqa: pylint: disable=broad-except
            self.get_logger().error(
                "Unity action {} goal {} failed: {}".format(
                    self.action_name, goal_id, exc
                )
            )
            goal_handle.abort()
            return self.action_type.Result()

    def _take_goal(self, goal_id):
        with self._lock:
            goal_handle = self._goal_handles.pop(goal_id, None)
            future = self._result_futures.pop(goal_id, None)
            timer = self._result_timers.pop(goal_id, None)

        if timer is not None:
            timer.cancel()
        if goal_handle is None or future is None:
            return None
        return goal_handle, future

    def _fail_goal(self, goal_id, exception):
        pending = self._take_goal(goal_id)
        if pending is None:
            return False
        _, future = pending
        future.set_exception(exception)
        return True

    def _timeout_goal(self, goal_id):
        self._fail_goal(
            goal_id,
            TimeoutError(
                "Timed out waiting for Unity action result on {} for goal {}".format(
                    self.action_name, goal_id
                )
            ),
        )

    def _ros_uuid_to_str(self, goal_id):
        return "".join(["{:02x}".format(b) for b in goal_id.uuid])
