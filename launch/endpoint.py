from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, OpaqueFunction
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def _build_endpoint_node(context):
    respawn_value = LaunchConfiguration("respawn").perform(context).strip().lower()
    respawn = respawn_value in ("1", "true", "yes", "on")

    return [
        Node(
            package="ros_tcp_endpoint",
            executable="default_server_endpoint",
            emulate_tty=True,
            respawn=respawn,
            parameters=[{"ROS_IP": "0.0.0.0"}, {"ROS_TCP_PORT": 10000}],
        )
    ]


def generate_launch_description():
    return LaunchDescription(
        [
            DeclareLaunchArgument(
                "respawn",
                default_value="false",
                description="Whether to respawn the ROS TCP endpoint if it exits.",
            ),
            OpaqueFunction(function=_build_endpoint_node),
        ]
    )
