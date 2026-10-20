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

import re
import threading

from rclpy.callback_groups import MutuallyExclusiveCallbackGroup

from .communication import RosReceiver


class _DrainingServiceCallbackGroup(MutuallyExclusiveCallbackGroup):
    """Keep the service alive until rclpy has sent an executing call's response."""

    def __init__(self):
        """Create a mutually exclusive group with bounded teardown support."""
        super().__init__()
        self._condition = threading.Condition()
        self._closing = False
        self._running = False

    def can_execute(self, entity):
        """Stop scheduling new calls once teardown begins."""
        with self._condition:
            return not self._closing and super().can_execute(entity)

    def beginning_execution(self, entity):
        """Track the whole executor callback, including response transmission."""
        with self._condition:
            if self._closing or not super().beginning_execution(entity):
                return False
            self._running = True
            return True

    def ending_execution(self, entity):
        """Release teardown only after the executor completes the callback."""
        with self._condition:
            super().ending_execution(entity)
            self._running = False
            self._condition.notify_all()

    def drain(self, timeout_sec):
        """Prevent new callbacks and wait for any running callback to finish."""
        with self._condition:
            self._closing = True
            return self._condition.wait_for(lambda: not self._running, timeout_sec)


class UnityService(RosReceiver):
    """
    Class to register a ROS service that's implemented in Unity.
    """

    def __init__(self, topic, service_class, tcp_server, queue_size=10):
        """

        Args:
            topic:         Topic name to publish messages to
            service_class: The message class in catkin workspace
            queue_size:    Max number of entries to maintain in an outgoing queue
        """
        strippedTopic = re.sub("[^A-Za-z0-9_]+", "", topic)
        node_name = f"{strippedTopic}_service"
        RosReceiver.__init__(self, node_name)

        self.topic = topic
        self.node_name = node_name
        self.service_class = service_class
        self.tcp_server = tcp_server
        self.queue_size = queue_size

        self._service_callback_group = _DrainingServiceCallbackGroup()
        self.service = self.create_service(
            self.service_class, self.topic, self.send,
            callback_group=self._service_callback_group,
        )

    def send(self, request, response):
        """
        Connect to TCP endpoint on client, pass along message and get reply
        Args:
            data: message data to send outside of ROS network

        Returns:
            The response message
        """
        unity_response = self.tcp_server.send_unity_service(
            self.topic, self.service_class, request
        )
        if unity_response is None:
            self.get_logger().error(
                "Unity service '{}' did not return a valid response; returning the default ROS response.".format(
                    self.topic
                )
            )
            return response
        return unity_response

    def prepare_unregister(self, timeout_sec=2.0):
        """Drain through response transmission, which follows our send callback."""
        if not self._service_callback_group.drain(timeout_sec):
            self.get_logger().warning(
                "Timed out draining Unity service '{}' during unregister".format(self.topic)
            )

    def unregister(self):
        """

        Returns:

        """
        self.destroy_node()
