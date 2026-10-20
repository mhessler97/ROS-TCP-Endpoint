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

"""Keep Fast DDS subscription status events from waking on queued message data."""

import weakref

from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
from rclpy.subscription import Subscription


class SubscriptionCallbackGroup(MutuallyExclusiveCallbackGroup):
    """Defer a subscription's events until its queued callback can run.

    Fast DDS shares the subscription's DATA_AVAILABLE status condition with its
    QoS events (ros2/rmw_fastrtps#929). Once rclpy queues a subscription, it omits
    that subscription from the wait set. Including its events during this gap
    can make the wait return immediately, repeatedly, until a worker takes the
    sample. Keep those events out of the wait set for the same queued interval.

    This group belongs to exactly one subscription and its event handlers.
    Normal mutual exclusion still applies while a callback is executing. Event
    handlers remain registered and run after the subscription's queued work;
    rclpy wakes the executor on both message take and callback completion.
    """

    def __init__(self):
        """Create a group for one subscription and its associated event handlers."""
        super().__init__()
        self._subscription = None

    def add_entity(self, entity):
        """Remember the subscription without extending its lifetime."""
        super().add_entity(entity)
        if isinstance(entity, Subscription):
            self._subscription = weakref.ref(entity)

    def can_execute(self, entity):
        """Keep events out of the wait set while their subscription is queued."""
        subscription = self._subscription() if self._subscription is not None else None
        # _executor_event is the same flag rclpy's Executor.can_execute uses.
        # Fall back to ordinary mutual exclusion if a future rclpy removes it.
        if (
            subscription is not None
            and entity is not subscription
            and getattr(subscription, "_executor_event", False)
        ):
            return False
        return super().can_execute(entity)
