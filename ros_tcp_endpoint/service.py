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

from rclpy.serialization import deserialize_message

from .communication import RosSender


class RosService(RosSender):
    """
    Class to send messages to a ROS service.
    """

    def __init__(self, service, service_class):
        """
        Args:
            service:        The service name in ROS
            service_class:  The service class in catkin workspace
        """
        strippedService = re.sub("[^A-Za-z0-9_]+", "", service)
        node_name = f"{strippedService}_RosService"
        RosSender.__init__(self, node_name)

        self.service_topic = service
        self.cli = self.create_client(service_class, service)
        self.req = service_class.Request()
        self.service_wait_timeout_sec = 5.0

    def send(self, data):
        """
        Takes in serialized message data from source outside of the ROS network,
        deserializes it into it's class, calls the service with the message, and returns
        the service's response.

        Args:
            data: The already serialized message_class data coming from outside of ROS

        Returns:
            service response
        """
        message_type = type(self.req)

        try:
            message = deserialize_message(data, message_type)
        except Exception as exc:  # noqa: pylint: disable=broad-except
            self.get_logger().error(
                "Ignoring service call to {} - failed to deserialize request: {}".format(
                    self.service_topic, exc
                )
            )
            return None

        if not self.cli.wait_for_service(timeout_sec=self.service_wait_timeout_sec):
            self.get_logger().error(
                "Ignoring service call to {} - service is not ready within {}s.".format(
                    self.service_topic, self.service_wait_timeout_sec
                )
            )
            return None

        future = self.cli.call_async(message)
        done_event = threading.Event()
        future.add_done_callback(lambda _: done_event.set())

        if not done_event.wait(timeout=self.service_wait_timeout_sec):
            self.get_logger().error(
                "Service call to {} timed out after {}s".format(
                    self.service_topic, self.service_wait_timeout_sec
                )
            )
            return None

        try:
            response = future.result()
            return response
        except Exception as exc:  # noqa: pylint: disable=broad-except
            self.get_logger().error("Service call to {} failed: {}".format(self.service_topic, exc))

        return None

    def unregister(self):
        """

        Returns:

        """
        self.destroy_client(self.cli)
        self.destroy_node()
