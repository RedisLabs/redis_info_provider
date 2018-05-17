import logging
from enum import Enum
from .redis_shard import RedisShard
from typing import List, Set, Callable


logger = logging.getLogger(__name__)


EventTarget = Callable[[RedisShard], None]


class _ShardPublisher(object):
    """Tracks live shards in the system and provides a notification service for shard
    addition/removal events. This is a service-provider class that implements the
    management logic, and should not be subclassed or replaced. The logic implementing
    actual shard detection and tracking is separate and customizable.

    This class is intended to be used as a global object via the ShardPublisher field.
    You most likely do not want to instantiate it manually.
    """

    def __init__(self):
        self._subs_new = []
        self._subs_del = []
        self._shards = {}

    def get_live_shards(self):
        # type: () -> List[RedisShard]

        """Return all live shards in the system, as a list of RedisShard objects.
        The returned objects may be updated and changed as required; all changes will be
        reflected in further calls to methods of this class.
        """
        return self._shards.values()

    def get_live_shard_ids(self):
        # type: () -> Set[str]

        """
        Return identifiers of all live shards in the system, as a set of strings.
        """
        return set(self._shards.keys())

    def get_shard(self, identifier):
        # type: (str) -> RedisShard

        """
        Return Shard object corresponding to the provided identifier.
        """
        return self._shards[identifier]

    class ShardEvent(Enum):
        """Describes a type of shard-related event."""
        ADDED = 1
        REMOVED = 2

        @classmethod
        def select_event_list(cls, pub, event):
            # type: (_ShardPublisher, _ShardPublisher.ShardEvent) -> List[EventTarget]

            """
            Selects the appropriate event subscriber list from ShardPublisher.
            Don't call this outside ShardPublisher!
            :param pub: A ShardPublisher instance.
            :param event: The event type.
            :return: Reference to the appropriate subscriber list in `pub`.
            """
            if event == cls.ADDED:
                return pub._subs_new
            elif event == cls.REMOVED:
                return pub._subs_del
            else:
                raise KeyError('unknown event type {}'.format(event))

    def subscribe_shard_event(self, event, target):
        # type: (ShardEvent, EventTarget) -> None

        """
        Add a subscriber to be notified of shard events.
        :param event: A ShardEvent specifying the type of event to subscribe to.
        :param target: The subscriber. When a shard event occurs, `target` will be
            called with the RedisShard instance.
        """
        self.ShardEvent.select_event_list(self, event).append(target)

    def unsub_shard_event(self, event, target):
        # type: (ShardEvent, EventTarget) -> None

        """
        Remove a subscriber from further shard event notifications.
        :param event: The type of event to unsubscribe from.
        :param target: The subscriber. It is an error for this to not be a current
            subscriber to that event type.
        """
        self.ShardEvent.select_event_list(self, event).remove(target)

    def clear_shards(self):
        # type: () -> None

        """
        Remove all tracked shards.
        """
        self._shards.clear()

    def add_shard(self, shard):
        # type: (RedisShard) -> None

        """
        Notify the publisher a new shard has been added to the system. This is an interface
        for use by shard watcher implementations.
        :param shard: A RedisShard instance.
        """
        logger.info('Adding shard %s', shard.id)
        self._shards[shard.id] = shard
        for tgt in self._subs_new:
            tgt(shard)

    def del_shard(self, shard_id):
        # type: (str) -> None

        """
        Notify the publisher a shard has been removed from the system. This is an interface
        for use by shard watcher implementations.
        :param shard_id: An identifier for a previously-added RedisShard.
        """
        logger.info('Deleting shard %s', shard_id)
        shard = self._shards.pop(shard_id)
        for tgt in self._subs_del:
            tgt(shard)


ShardPublisher = _ShardPublisher()
