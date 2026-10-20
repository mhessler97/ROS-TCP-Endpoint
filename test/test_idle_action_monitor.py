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

"""Action-monitor lifecycle checks, including failures and overlapping goals."""

from types import SimpleNamespace
import threading
from unittest.mock import Mock

import pytest
from action_msgs.msg import GoalStatus
from example_interfaces.action import Fibonacci
from rclpy.serialization import serialize_message
from rclpy.task import Future

from ros_tcp_endpoint.action_client import RosActionClient


class Timer:
    """Record timer transitions without a clock or middleware dependency."""

    def __init__(self):
        self.canceled = True
        self.resets = 0

    def is_canceled(self):
        return self.canceled

    def reset(self):
        self.canceled = False
        self.resets += 1

    def cancel(self):
        self.canceled = True


@pytest.fixture
def bridge():
    client = object.__new__(RosActionClient)
    client.action_name = '/monitor_test'
    client.action_type = Fibonacci
    client._lock = threading.Lock()
    client._Node__executor_weakref = None
    client._unregistered = False
    client._inflight_goals = {}
    client._pending_goals = {}
    client._ros_goal_lookup = {}
    client._server_health_timer = Timer()
    client.get_logger = Mock(return_value=Mock())
    client._client = Mock()
    client._client.server_is_ready.return_value = True
    client.tcp_server = Mock()
    client.destroy_node = Mock()
    return client


def send(bridge, name):
    future = Future()
    bridge._client.send_goal_async.return_value = future
    bridge.send_goal(name, serialize_message(Fibonacci.Goal(order=3)), client_id=7)
    return future


def accept(future, sequence=1):
    result = Future()
    handle = SimpleNamespace(
        accepted=True, goal_id=SimpleNamespace(uuid=bytes([sequence]) * 16),
        get_result_async=lambda: result,
    )
    future.set_result(handle)
    return result


def finish(future):
    future.set_result(SimpleNamespace(
        status=GoalStatus.STATUS_SUCCEEDED, result=Fibonacci.Result(sequence=[1, 2, 3])
    ))


def test_idle_monitor_does_not_query_graph(bridge):
    bridge._check_server_health()
    bridge._client.server_is_ready.assert_not_called()
    assert bridge._server_health_timer.is_canceled()


def test_overlapping_goals_preserve_cadence_and_stop_only_after_last_result(bridge):
    first = send(bridge, 'first')
    second = send(bridge, 'second')
    assert not bridge._server_health_timer.is_canceled()
    assert bridge._server_health_timer.resets == 1
    result1 = accept(first, 1)
    result2 = accept(second, 2)
    finish(result1)
    assert not bridge._server_health_timer.is_canceled()
    finish(result2)
    assert bridge._server_health_timer.is_canceled()
    assert not bridge._pending_goals and not bridge._inflight_goals
    assert not bridge._ros_goal_lookup
    finish(accept(send(bridge, 'third'), 3))
    assert bridge._server_health_timer.resets == 2
    assert bridge._server_health_timer.is_canceled()


@pytest.mark.parametrize('failure', ['send', 'accept_exception', 'reject', 'result_exception'])
def test_failure_paths_disarm_monitor(bridge, failure):
    if failure == 'send':
        bridge._client.send_goal_async.side_effect = RuntimeError('send failed')
        send(bridge, 'failed')
    else:
        future = send(bridge, 'failed')
        if failure == 'accept_exception':
            future.set_exception(RuntimeError('accept failed'))
        elif failure == 'reject':
            future.set_result(SimpleNamespace(accepted=False))
        else:
            accept(future).set_exception(RuntimeError('result failed'))
    assert bridge._server_health_timer.is_canceled()
    assert not bridge._pending_goals and not bridge._inflight_goals


def test_server_loss_finishes_pending_and_accepted_goals_once(bridge):
    pending = send(bridge, 'pending')
    result = accept(send(bridge, 'accepted'))
    bridge._client.server_is_ready.return_value = False
    bridge._check_server_health()
    assert bridge._server_health_timer.is_canceled()
    sender = bridge.tcp_server.unity_tcp_sender
    assert sender.send_action_goal_response.call_count == 2
    assert sender.send_action_result.call_count == 1
    assert sender.send_action_result.call_args.args[2] == GoalStatus.STATUS_ABORTED
    # Late completions must not reactivate the timer or duplicate terminal frames.
    pending.set_result(SimpleNamespace(accepted=False))
    finish(result)
    assert sender.send_action_goal_response.call_count == 2
    assert sender.send_action_result.call_count == 1
    assert bridge._server_health_timer.is_canceled()


def test_unregister_prevents_late_future_from_touching_destroyed_timer(bridge):
    pending = send(bridge, 'pending')
    bridge.unregister()
    bridge._server_health_timer.cancel = Mock(side_effect=AssertionError('destroyed timer'))
    bridge._server_health_timer.reset = Mock(side_effect=AssertionError('destroyed timer'))
    pending.set_exception(RuntimeError('disconnected'))
    bridge._check_server_health()
