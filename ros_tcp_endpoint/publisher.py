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

from .communication import (
    RosSender,
    deserialize_ros_message,
    message_type_is_effectively_empty,
)
from .qos import make_qos_profile


class RosPublisher(RosSender):
    """
    Class to publish messages to a ROS topic
    """

    def __init__(self, topic, message_class, queue_size=10, latch=False, qos=None):
        """

        Args:
            topic:         Topic name to publish messages to
            message_class: The message class in catkin workspace
            queue_size:    Max number of entries to maintain in an outgoing queue
            latch:         Use transient-local durability for late joiners
            qos:           Optional QoS preset name or policy dictionary
        """
        strippedTopic = re.sub("[^A-Za-z0-9_]+", "", topic)
        node_name = f"{strippedTopic}_RosPublisher"
        RosSender.__init__(self, node_name)
        self.msg = message_class()
        self.qos_profile = make_qos_profile(
            queue_size=queue_size, latch=latch, qos=qos
        )
        self.pub = self.create_publisher(message_class, topic, self.qos_profile)

    def send(self, data):
        """
        Takes in serialized message data from source outside of the ROS network,
        deserializes it into it's message class, and publishes the message to ROS topic.

        Args:
            data: The already serialized message_class data coming from outside of ROS

        Returns:
            None: Explicitly return None so behaviour can be
        """
        message_type = type(self.msg)
        if message_type_is_effectively_empty(message_type):
            try:
                data = deserialize_ros_message(data, message_type)
            except Exception as exc:  # noqa: pylint: disable=broad-except
                self.get_logger().error(
                    "Ignoring malformed fieldless topic message "
                    "(payload_len={}, prefix={}): {}".format(
                        len(data), bytes(data[:16]).hex(), exc
                    )
                )
                return None

        self.pub.publish(data)

        return None

    def unregister(self):
        """

        Returns:

        """
        self.destroy_publisher(self.pub)
        self.destroy_node()
