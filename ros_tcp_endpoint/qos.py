# Copyright 2026 Unity Technologies
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Backward-compatible ROS topic QoS profile construction."""

from rclpy.qos import (
    QoSDurabilityPolicy,
    QoSHistoryPolicy,
    QoSProfile,
    QoSReliabilityPolicy,
)


_RELIABILITY_POLICIES = {
    "system_default": QoSReliabilityPolicy.SYSTEM_DEFAULT,
    "reliable": QoSReliabilityPolicy.RELIABLE,
    "best_effort": QoSReliabilityPolicy.BEST_EFFORT,
    "best_effort_reliability": QoSReliabilityPolicy.BEST_EFFORT,
}

_DURABILITY_POLICIES = {
    "system_default": QoSDurabilityPolicy.SYSTEM_DEFAULT,
    "transient_local": QoSDurabilityPolicy.TRANSIENT_LOCAL,
    "volatile": QoSDurabilityPolicy.VOLATILE,
}

_HISTORY_POLICIES = {
    "system_default": QoSHistoryPolicy.SYSTEM_DEFAULT,
    "keep_last": QoSHistoryPolicy.KEEP_LAST,
    "keep_all": QoSHistoryPolicy.KEEP_ALL,
}

_PRESETS = {
    "default": {
        "reliability": QoSReliabilityPolicy.RELIABLE,
        "durability": QoSDurabilityPolicy.VOLATILE,
        "history": QoSHistoryPolicy.KEEP_LAST,
    },
    "sensor_data": {
        "reliability": QoSReliabilityPolicy.BEST_EFFORT,
        "durability": QoSDurabilityPolicy.VOLATILE,
        "history": QoSHistoryPolicy.KEEP_LAST,
    },
    "transient_local": {
        "reliability": QoSReliabilityPolicy.RELIABLE,
        "durability": QoSDurabilityPolicy.TRANSIENT_LOCAL,
        "history": QoSHistoryPolicy.KEEP_LAST,
    },
    "latched": {
        "reliability": QoSReliabilityPolicy.RELIABLE,
        "durability": QoSDurabilityPolicy.TRANSIENT_LOCAL,
        "history": QoSHistoryPolicy.KEEP_LAST,
    },
}


def _normalize_name(value):
    return str(value).strip().lower().replace("-", "_").replace(" ", "_")


def _resolve_policy(value, policies, policy_type, field_name):
    if isinstance(value, policy_type):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        try:
            return policy_type(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("Unknown QoS {} value: {}".format(field_name, value)) from exc

    normalized = _normalize_name(value)
    if normalized not in policies:
        raise ValueError(
            "Unknown QoS {} '{}'; expected one of {}".format(
                field_name, value, sorted(policies.keys())
            )
        )
    return policies[normalized]


def _validate_depth(value):
    if isinstance(value, bool):
        raise ValueError("QoS depth must be a positive integer")
    try:
        depth = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("QoS depth must be a positive integer") from exc
    if depth <= 0:
        raise ValueError("QoS depth must be a positive integer")
    return depth


def make_qos_profile(queue_size=10, latch=False, qos=None):
    """Create a topic QoS profile while preserving the legacy defaults."""
    options = {}
    if qos is None:
        preset_name = "default"
    elif isinstance(qos, str):
        preset_name = _normalize_name(qos)
    elif isinstance(qos, dict):
        unknown_fields = set(qos) - {
            "preset",
            "reliability",
            "durability",
            "history",
            "depth",
        }
        if unknown_fields:
            raise ValueError(
                "Unknown QoS field(s): {}".format(", ".join(sorted(unknown_fields)))
            )
        options = dict(qos)
        preset_name = _normalize_name(options.pop("preset", "default"))
    else:
        raise ValueError("QoS must be a preset name or an object")

    if preset_name not in _PRESETS:
        raise ValueError(
            "Unknown QoS preset '{}'; expected one of {}".format(
                preset_name, sorted(_PRESETS.keys())
            )
        )

    policies = dict(_PRESETS[preset_name])
    if "reliability" in options:
        policies["reliability"] = _resolve_policy(
            options["reliability"],
            _RELIABILITY_POLICIES,
            QoSReliabilityPolicy,
            "reliability",
        )
    if "durability" in options:
        policies["durability"] = _resolve_policy(
            options["durability"],
            _DURABILITY_POLICIES,
            QoSDurabilityPolicy,
            "durability",
        )
    if "history" in options:
        policies["history"] = _resolve_policy(
            options["history"],
            _HISTORY_POLICIES,
            QoSHistoryPolicy,
            "history",
        )

    # Preserve the ROS-TCP legacy queue_size behavior unless the new QoS object
    # explicitly supplies depth.  latch=True is the legacy spelling of
    # TRANSIENT_LOCAL and intentionally wins over a conflicting durability.
    depth = _validate_depth(options.get("depth", queue_size))
    if latch:
        policies["durability"] = QoSDurabilityPolicy.TRANSIENT_LOCAL

    return QoSProfile(depth=depth, **policies)
