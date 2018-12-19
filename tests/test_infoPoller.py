from unittest import TestCase
from mock import NonCallableMock, patch
import gevent
import six
from redis_info_provider import *


class TestInfoPoller(TestCase):
    def setUp(self):
        self._tick = 1

        # Mock out the StrictRedis definition for InfoPoller
        patcher = patch('redis_info_provider.info_poller.redis.StrictRedis', autospec=True,
                        # This captures the kwargs passed to the initializer of the mock StrictRedis
                        side_effect=lambda **kwargs: (
                            NonCallableMock(**{
                                # This causes info() to return a dict containing the Redis port and the
                                # _tick value at the time of the call to info()
                                'info.side_effect': lambda *_: {
                                    'port': kwargs['port'],
                                    'tick': self._tick,
                                }
                            })
                        ))
        self._mockStrictRedis = patcher.start()
        self.addCleanup(patcher.stop)

        # Mock out the ShardPublisher definition for InfoPoller
        patcher = patch('redis_info_provider.info_poller.ShardPublisher', autospec=True)
        self._mockPublisher = patcher.start()
        self.addCleanup(patcher.stop)

        super(TestInfoPoller, self).setUp()

    # Verify the poller subscribes to all event types as expected
    def test_subscribe(self):
        with InfoPoller() as poller:
            expected_registered_callbacks = [
                (self._mockPublisher.ShardEvent.ADDED, poller._add_shard),
                (self._mockPublisher.ShardEvent.REMOVED, poller._remove_shard)
            ]
            actual_registered_callbacks = [
                call_args[0] for call_args in self._mockPublisher.subscribe_shard_event.call_args_list
            ]
            six.assertCountEqual(self,
                                 expected_registered_callbacks,
                                 actual_registered_callbacks)

    def test_poll(self):
        shard = self.NoDelayShard(1, self._mockStrictRedis(port=6379))

        with InfoPoller() as poller:
            poller._add_shard(shard)
            gevent.sleep(0)  # yield so the poller polls
            six.assertCountEqual(
                self,
                (6379, self._tick),
                (shard.info['port'], shard.info['tick'])
            )
            self._tick += 1
            gevent.sleep(0)  # yield so the poller polls
            six.assertCountEqual(
                self,
                (6379, self._tick),
                (shard.info['port'], shard.info['tick'])
            )

    def test_stop_poll(self):
        shard = self.NoDelayShard(1, self._mockStrictRedis(port=6379))

        poller = InfoPoller()
        poller._add_shard(shard)
        gevent.sleep(0)  # yield so the poller polls
        six.assertCountEqual(
            self,
            (6379, self._tick),
            (shard.info['port'], shard.info['tick'])
        )

        poller.stop()
        gevent.sleep(0)  # yield so the poller greenlets have a chance to stop

        self._tick += 1
        gevent.sleep(0)  # yield so the poller has a chance to poll (it shouldn't)
        six.assertCountEqual(
            self,
            (6379, 1),
            (shard.info['port'], shard.info['tick'])
        )

    # RedisShard that specifies no delay between consecutive pollings
    class NoDelayShard(RedisShard):
        def polling_interval(self): return 0.0

        def _update_interval(self): pass  # the real one will fail because our INFO is missing required keys
