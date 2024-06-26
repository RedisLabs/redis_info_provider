from gevent.event import AsyncResult
from greenlet import GreenletExit

from .shard_pub import ShardPublisher
import gevent
import gevent.socket
import redis
import redis.connection
import logging
from typing import Dict
from .redis_shard import RedisShard

# Make Redis gevent-aware
redis.connection.socket = gevent.socket


class InfoPoller(object):
    """
    Component for polling and storing Redis shard INFO.
    This single-threaded class uses cooperative multitasking based on the gevent
    module to efficiently poll many shards, while minimizing resource usage.

    The results of the INFO queries are stored in the corresponding RedisShard
    and made available via the ShardPublisher.
    """

    def __init__(self, grace=3, logger=None):
        # type: (int,Logger) -> None

        """
        :param grace: Number of consecutive times a shard polling needs to fail before
            an error is logged. This gives a grace period for the running shard-watcher
            to notice a shard is no longer active and signal its removal, without
            errors being logged unnecessarily.
        """
        self._greenlets = {}  # type: Dict[str, gevent.Greenlet]
        self.logger = logger or logging.getLogger(__name__)
        self._grace = grace
        self.exception_event = None

        # Subscribe to receive shard event notifications
        ShardPublisher.subscribe_shard_event(ShardPublisher.ShardEvent.ADDED, self._add_shard)
        ShardPublisher.subscribe_shard_event(ShardPublisher.ShardEvent.REMOVED, self._remove_shard)

        # Start polling shards that already existed when we started
        for shard in ShardPublisher.get_live_shards():
            self._add_shard(shard)

    def set_exception_event(self, exception_evt):
        # type: (AsyncResult) -> None

        """
        Configure an external event object that will be signaled if an unexpected exception occurs
        inside a poller greenlet. This allows users of the module to be alerted if a poller terminates
        due to an unhandled exception.
        :param exception_evt: The event object. `exception_evt.set_exception` will be called if an unhandled
        exception occurs in a poller.
        """
        self.exception_event = exception_evt

    def stop(self):
        # type: () -> None

        """
        Stop polling all shards. Equivalent to calling _remove_shard() for all shards
        currently being polled.
        """
        ShardPublisher.unsub_shard_event(ShardPublisher.ShardEvent.ADDED, self._add_shard)
        ShardPublisher.unsub_shard_event(ShardPublisher.ShardEvent.REMOVED, self._remove_shard)

        self.logger.info('Stopping all pollers')
        gevent.killall(self._greenlets.values())
        self._greenlets.clear()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()

    def _add_shard(self, shard):
        # type: (RedisShard) -> None

        """
        Add a new shard to be polled.
        :param shard: A RedisShard instance representing the shard to be polled.
        """
        # Already tracking this shard; nothing to do
        if shard.id in self._greenlets:
            return

        self.logger.info('Spawning poller greenlet for shard %s', shard.id)
        self._greenlets[shard.id] = gevent.spawn(
            lambda: self._poll_shard(shard)
        )

    def _remove_shard(self, shard):
        # type: (RedisShard) -> None

        """
        Stop polling a specified shard.
        :param shard: A RedisShard instance representing the shard to be removed.
            Specifying a shard that is not currently being polled has no effect.
        """
        self.logger.info('Killing poller for shard %s', shard.id)
        try:
            self._greenlets[shard.id].kill()
            del self._greenlets[shard.id]
        except KeyError:
            # Unknown shard; ignore
            self.logger.warning('Attempted to remove unknown shard %s', shard.id)

    def _poll_shard(self, shard):
        # type: (RedisShard) -> None

        """
        Shard-polling greenlet main().
        """

        consecutive_redis_failures = 0
        consecutive_general_failures = 0

        # Retry loop. Redis errors (disconnects etc.) shouldn't stop us from polling as
        # long as the shard lives. However, other unexpected problems should at the
        # least terminate the greenlet.
        while True:
            try:
                redis_conn = (shard.redis_conn() if
                              callable(shard.redis_conn) else
                              shard.redis_conn)

                # Will be stopped by a call to Greenlet.kill()
                while True:
                    info = redis_conn.info('everything')
                    self.logger.debug('Polled shard %s', shard.id)
                    info['meta'] = {}
                    shard.info = info
                    consecutive_redis_failures = 0; consecutive_general_failures = 0
                    gevent.sleep(shard.polling_interval())
            except redis.RedisError:
                consecutive_redis_failures += 1
                if consecutive_redis_failures < self._grace:
                    self.logger.debug(
                        'Redis error polling shard %s for %d consecutive times; still within grace period; will retry',
                        shard.id, consecutive_redis_failures
                    )
                else:
                    self.logger.warning(
                        'Redis error polling shard %s for %d consecutive times; will retry',
                        shard.id, consecutive_redis_failures
                    )
                gevent.sleep(1)  # Cool-off period
                continue  # Retry
            except GreenletExit:
                self.logger.info("poller %s exiting..", shard.id)
                raise
            except Exception as e:
                consecutive_general_failures += 1
                self.logger.error(" info_poller shard %s caught exception: %s",shard.id,e)
                if consecutive_general_failures < self._grace:
                    gevent.sleep(2)
                else:
                    self.logger.error("general exception caught more than %s consecutive times; poller %s exiting..",
                                 consecutive_general_failures, shard.id)
                    if self.exception_event:
                        self.exception_event.set_exception(e)
                    raise
