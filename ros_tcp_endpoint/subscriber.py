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

from .communication import RosReceiver
from .qos import make_qos_profile


class RosSubscriber(RosReceiver):
    """
    Class to send messages outside of ROS network
    """

    def __init__(
        self,
        topic,
        message_class,
        tcp_server,
        queue_size=10,
        latch=False,
        qos=None,
        client_id=None,
    ):
        """

        Args:
            topic:         Topic name to publish messages to
            message_class: The message class in catkin workspace
            queue_size:    Max number of entries to maintain in an outgoing queue
            latch:         Use transient-local durability for late joiners
            qos:           Optional QoS preset name or policy dictionary
        """
        strippedTopic = re.sub("[^A-Za-z0-9_]+", "", topic)
        client_suffix = "_{}".format(client_id) if client_id is not None else ""
        self.node_name = f"{strippedTopic}_RosSubscriber{client_suffix}"
        RosReceiver.__init__(self, self.node_name)
        self.topic = topic
        self.msg = message_class
        self.tcp_server = tcp_server
        self.queue_size = queue_size
        self.client_id = client_id

        self.qos_profile = make_qos_profile(
            queue_size=queue_size, latch=latch, qos=qos
        )

        # Start Subscriber listener function
        self.subscription = self.create_subscription(
            self.msg, self.topic, self.send, self.qos_profile
        )
        self.subscription

    def send(self, data):
        """
        Connect to TCP endpoint on client and pass along message
        Args:
            data: message data to send outside of ROS network

        Returns:
            self.msg: The deserialize message

        """
        self.tcp_server.send_unity_message(
            self.topic, data, client_id=self.client_id
        )
        return self.msg

    def unregister(self):
        """

        Returns:

        """
        self.destroy_subscription(self.subscription)
        self.destroy_node()
