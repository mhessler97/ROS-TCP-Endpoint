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
from rclpy.parameter import Parameter
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


def payload_is_effectively_empty(data):
    """Recognize zero-byte and header-only CDR encodings of empty messages."""
    if not data:
        return True

    # Some ROS 2 serializers emit only the four-byte CDR encapsulation header
    # for a fieldless message, while Fast DDS expects the generated padding
    # member as well.  Accept the standard CDR/XCDR representation identifiers
    # with zero encapsulation options, but do not treat arbitrary bytes as empty.
    if len(data) != 4 or data[2:] != b"\x00\x00":
        return False
    representation_identifier = int.from_bytes(data[:2], byteorder="big")
    return representation_identifier in {
        0x0000,
        0x0001,
        0x0002,
        0x0003,
        0x0006,
        0x0007,
        0x0008,
        0x0009,
        0x000A,
        0x000B,
    }


def deserialize_ros_message(data, message_type):
    """Deserialize a ROS message, accepting empty encodings for fieldless types."""
    try:
        return deserialize_message(data, message_type)
    except Exception:
        if (
            payload_is_effectively_empty(data)
            and message_type_is_effectively_empty(message_type)
        ):
            return message_type()
        raise


# Retain the service-specific name for downstream users of the initial fix.
deserialize_service_message = deserialize_ros_message


def bridge_node_options():
    """Return lean rclpy options for dynamically created bridge nodes."""
    return {
        "enable_rosout": False,
        "start_parameter_services": False,
        "parameter_overrides": [
            Parameter("start_type_description_service", value=False)
        ],
    }


class RosSender(Node):
    """
        Base class for ROS communication where data is sent to the ROS network.
    """

    def __init__(self, node_name):
        super().__init__(node_name, **bridge_node_options())

    def send(self, *args):
        raise NotImplementedError


class RosReceiver(Node):
    """
        Base class for ROS communication where data is being sent outside of the ROS network.
    """

    def __init__(self, node_name):
        super().__init__(node_name, **bridge_node_options())

    def send(self, *args):
        raise NotImplementedError
