from unittest import TestCase
from redis_info_provider.shard_pub import ShardPublisher, _ShardPublisher
from redis_info_provider.redis_shard import RedisShard
import six


class TestShardPublisher(TestCase):
    def tearDown(self):
        # recreate the global instance to reset state between tests
        global ShardPublisher
        ShardPublisher = _ShardPublisher()
        super(TestShardPublisher, self).tearDown()

    @staticmethod
    def make_dummy_shard(id):
        return RedisShard(id, {})

    def test_shard_tracking(self):
        s1 = self.make_dummy_shard(1)
        s2 = self.make_dummy_shard(2)
        shards = [s1, s2]

        ShardPublisher.add_shard(s1)
        ShardPublisher.add_shard(s2)

        six.assertCountEqual(self, shards, ShardPublisher.get_live_shards())
        six.assertCountEqual(self, [s.id for s in shards], ShardPublisher.get_live_shard_ids())

        ShardPublisher.del_shard(s1.id)

        six.assertCountEqual(self, shards[-1:], ShardPublisher.get_live_shards())
        six.assertCountEqual(self, [s.id for s in shards[-1:]], ShardPublisher.get_live_shard_ids())

    def test_get_shard(self):
        s = self.make_dummy_shard(5)
        ShardPublisher.add_shard(s)
        self.assertEqual(s, ShardPublisher.get_shard(s.id))

    def test_subscribe_shard_event(self):
        s = self.make_dummy_shard(5)

        notify_add = NotificationTracker()
        ShardPublisher.subscribe_shard_event(ShardPublisher.ShardEvent.ADDED, notify_add.notify)
        self.assertEqual(0, notify_add.times_notified)
        ShardPublisher.add_shard(s)
        self.assertEqual(1, notify_add.times_notified)

        notify_del = NotificationTracker()
        ShardPublisher.subscribe_shard_event(ShardPublisher.ShardEvent.REMOVED, notify_del.notify)
        self.assertEqual(0, notify_del.times_notified)
        ShardPublisher.del_shard(s.id)
        self.assertEqual(1, notify_del.times_notified)

    def test_unsub_shard_event(self):
        s = self.make_dummy_shard(5)

        notify = NotificationTracker()
        ShardPublisher.subscribe_shard_event(ShardPublisher.ShardEvent.ADDED, notify.notify)
        ShardPublisher.subscribe_shard_event(ShardPublisher.ShardEvent.REMOVED, notify.notify)
        self.assertEqual(0, notify.times_notified)
        ShardPublisher.add_shard(s)
        self.assertEqual(1, notify.times_notified)

        ShardPublisher.unsub_shard_event(ShardPublisher.ShardEvent.REMOVED, notify.notify)
        ShardPublisher.del_shard(s.id)
        self.assertEqual(1, notify.times_notified)

    def test_clear_shards(self):
        shards = [self.make_dummy_shard(i) for i in range(2)]
        for shard in shards:
            ShardPublisher.add_shard(shard)
        ShardPublisher.clear_shards()
        self.assertEqual(0, len(ShardPublisher.get_live_shards()))


class NotificationTracker(object):
    def __init__(self):
        self.times_notified = 0

    def notify(self, _):
        self.times_notified += 1
