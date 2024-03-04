import gevent
import gevent.event
import psutil
import logging
import redis
from greenlet import GreenletExit

from .shard_pub import ShardPublisher
from .redis_shard import RedisShard
from typing import Mapping, Iterator, Any
from gevent.event import AsyncResult


logger = logging.getLogger(__name__)


class LocalShardWatcher(object):
    """
    The default implementation of a Redis shard watcher.
    This implementation opens a gevent green thread that attempts to monitor running redis-server
    processes on the local machine.
    Note that the process running this code must have sufficient permissions to introspect
    open connections of redis-server processes you wish to be listed.
    It is also currently unable to extract authentication information, if required, so it will
    not allow you to connect to Redis servers that require auth.

    Information about detected local shards is published via the ShardPublisher.

    NOTE: Currently this is only meant as a sample implementation, to be replaced in production
    code by your own subclass.
    """

    def __init__(self, update_frequency=1.0):
        # type: (float) -> None

        """
        :param update_frequency: Floating point interval, in seconds, in which to re-scan the system
            for running Redis processes.
        """
        self._update_freq = update_frequency
        self._should_stop = gevent.event.Event()
        self.exception_event = None
        self._greenlet = gevent.spawn(self._greenlet_main)

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
        Stop the shard-tracking greenlet and remove tracked shards from the ShardPublisher.
        This class assumes it is the only watcher running currently, and tries to be helpful by
        cleaning up all tracked shards from the publisher before stopping.
        """
        self._should_stop.set()
        self._greenlet.join()
        ShardPublisher.clear_shards()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()

    def _greenlet_main(self):
        # type: () -> None

        try:
            while not self._should_stop.is_set():
                published_shard_ids = ShardPublisher.get_live_shard_ids()
                live_shards = self._get_live_shards()
                live_shard_ids = set(live_shards.keys())

                logger.info('Updated Redis shards: %s', live_shard_ids)

                for shard_id in live_shard_ids - published_shard_ids:
                    # New shard
                    ShardPublisher.add_shard(live_shards[shard_id])
                for shard_id in published_shard_ids - live_shard_ids:
                    # Removed shard
                    ShardPublisher.del_shard(shard_id)
                gevent.sleep(self._update_freq)
        except GreenletExit:
            self.logger.info("local watcher exiting..")
            raise
        except Exception as e:
            if self.exception_event:
                self.exception_event.set_exception(e)

    @classmethod
    def _get_live_shards(cls):
        # type: () -> Mapping[str, RedisShard]

        """
        Get an identifier --> RedisShard dictionary of redis-server processes on the system. PID
        is used as the unique identifier of a Redis instance.
        """
        result = {}
        for redis_proc in cls._get_running_redises():
            kwargs = cls._get_connection_kwargs(redis_proc)
            if kwargs is not None:
                conn_maker = lambda _kwargs=kwargs: redis.StrictRedis(**_kwargs)
                shard = RedisShard(redis_proc.pid, conn_maker)
                result[shard.id] = shard
        return result

    @staticmethod
    def _get_connection_kwargs(proc):
        # type: (psutil.Process) -> Mapping[str, Any]

        """
        Finds the connection parameters (host; port) for a psutil.Process instance representing a running
        Redis server process. Returns the connection parameters as a kwargs dictionary suitable for passing
        to the __init__ method of redis.StrictRedis, or None if no suitable connection can be found in
        the process.
        """

        # Find the first TCP connection the process is listening to. This should be a valid connection
        # to access the Redis server on
        try:
            conn = next(c for c in proc.connections(kind='tcp') if c.status == psutil.CONN_LISTEN)
            return {
                'host': conn.laddr.ip,
                'port': conn.laddr.port
            }
        except StopIteration:
            # No suitable connection found in process
            logger.warning('Failed to get connection parameters for redis-server %d', proc.pid)
            return None
        except psutil.Error:
            # Insufficient permissions to examine the process's open connections,
            # process is already closed, or some other unexpected problem. Skip
            logger.warning('Problem examining redis-server with PID %d', proc.pid, exc_info=True)
            return None

    @staticmethod
    def _get_running_redises():
        # type: () -> Iterator[psutil.Process]

        """
        Returns a generator yielding a psutil.Process object for each running Redis-Server process on
        the local machine.
        """

        for p in psutil.process_iter(attrs=['pid', 'name']):
            try:
                if p.name() == 'redis-server':
                    yield p
            except psutil.Error:
                # Process may have been closed suddenly, or some other problem with process-inspection
                pass
