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

from rclpy.node import Node
from rclpy.serialization import deserialize_message


def message_type_is_effectively_empty(message_type):
    """Return whether a ROS request/response type contains no user fields."""
    try:
        field_map = message_type.get_fields_and_field_types()
    except (AttributeError, TypeError):
        return False

    if not field_map:
        return True

    # Some ROS generators add this placeholder to otherwise-empty structs.
    return set(field_map.keys()) <= {"structure_needs_at_least_one_member"}


def deserialize_service_message(data, message_type):
    """Deserialize a service message, accepting empty payloads for empty types."""
    try:
        return deserialize_message(data, message_type)
    except Exception:
        if not data and message_type_is_effectively_empty(message_type):
            return message_type()
        raise


class RosSender(Node):
    """
        Base class for ROS communication where data is sent to the ROS network.
    """

    def __init__(self, node_name):
        super().__init__(node_name)
        pass

    def send(self, *args):
        raise NotImplementedError


class RosReceiver(Node):
    """
        Base class for ROS communication where data is being sent outside of the ROS network.
    """

    def __init__(self, node_name):
        super().__init__(node_name)
        pass

    def send(self, *args):
        raise NotImplementedError
