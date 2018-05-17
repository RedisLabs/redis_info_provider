import logging
from typing import Union, Callable, Dict, Any
import redis


logger = logging.getLogger(__name__)


InfoType = Dict[str, Any]


class RedisShard(object):
    """
    Encapsulates all per-shard data and logic.
    """

    #: Minimum polling interval permitted, in seconds
    MIN_POLLING_INTERVAL = 0.5

    #: Maximum polling interval permitted, in seconds
    MAX_POLLING_INTERVAL = 3

    #: Size of step, in seconds, to increase polling interval in response to shard in-activity
    POLLING_INTERVAL_STEP = 0.5

    #: Threshold for instantaneous_ops_per_sec above which the shard is considered "active"
    OPS_ACTIVE_THRESH = 10

    #: Threshold for instantaneous_ops_per_sec below which the shard is considered "inactive"
    OPS_INACTIVE_THRESH = 5

    def __init__(self,
                 ident,      # type: str
                 redis_conn  # type: Union[redis.StrictRedis, Callable[[], redis.StrictRedis]]
                 ):
        # type: (...) -> None

        """
        :param ident: An implementation-defined string uniquely identifying this shard.
            The argument passed may be of any type; it will be converted to a string
            with str(ident).
        :param redis_conn: An instance of redis.StrictRedis or its subclass that
            provides an open connection to this shard, OR a callable that, when called,
            returns such an instance.
        """
        #: An implementation-defined string uniquely identifying this shard.
        self.id = str(ident)

        #: An instance of redis.StrictRedis or its subclass that provides an open
        #: connection to this shard, OR a callable that, when called, returns such an
        #: instance.
        self.redis_conn = redis_conn

        #: The latest available INFO for this shard, as a dictionary
        self.info = None

        #: Timestamp (as returned by time.time()) when RedisShard.info was updated last.
        self.info_timestamp = 0.0

        self._interval = self.MIN_POLLING_INTERVAL

    def polling_interval(self):
        # type: () -> float

        """
        Returns the interval, as a float representing seconds and fractions-of, after which
        the INFO for this shard should be refreshed. This value may change between invocations
        according to implementation logic based on arbitrary internal and/or external state.
        (e.g.: This default implementation adjusts the polling interval based on shard
        activity level as reported by the INFO.)"""
        return self._interval

    @property
    def info(self):
        # type: () -> InfoType
        return self._info

    @info.setter
    def info(self, value):
        # type: (InfoType) -> None
        self._info = value
        self._update_interval()

    def _update_interval(self):
        # type: () -> None

        """
        Adjust desired shard polling interval based on the instantaneous_ops_per_sec reported
        in the latest info.
        """

        if self.info is None:
            # no info to use yet
            return

        ops_sec = self.info['instantaneous_ops_per_sec']

        if ops_sec >= self.OPS_ACTIVE_THRESH:
            # shard was active; immediately drop to frequent polling
            self._interval = self.MIN_POLLING_INTERVAL
        elif ops_sec <= self.OPS_INACTIVE_THRESH:
            # shard was inactive; gradually increase polling interval
            self._interval += self.POLLING_INTERVAL_STEP

        # clamp to range [MIN_POLLING_INTERVAL..MAX_POLLING_INTERVAL]
        self._interval = max(self.MIN_POLLING_INTERVAL, min(self.MAX_POLLING_INTERVAL, self._interval))

    def __str__(self):
        return 'Shard<{}>'.format(self.id)

    def __repr__(self):
        return '{class_name}({id}, {conn})'.format(
            class_name=self.__class__.__name__,
            id=self.id,
            conn=self.redis_conn
        )
