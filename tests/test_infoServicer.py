from unittest import TestCase
from mock import NonCallableMock, patch
import six
from redis_info_provider import *


class TestInfoServicer(TestCase):
    def setUp(self):
        self.servicer = InfoProviderServicer()

    def tearDown(self):
        ShardPublisher.clear_shards()

    @staticmethod
    def make_shard(id, info={}, info_timestamp=None):
        fixedup_info = dict(info)
        fixedup_info['meta'] = {}  # this is required to be present, even if empty
        fixedup_info['instantaneous_ops_per_sec'] = 1.0  # needed in order to set the shard's polling frequency,
                                                         # even though in this case no one will ever poll it...

        shard = RedisShard(id, None)
        shard.info = fixedup_info

        # override the timestamp, if requested. (by default this is set to current time when shard.info is set.)
        if info_timestamp is not None:
            shard.info_timestamp = info_timestamp

        return shard

    def assertSensibleAge(self, info_age, max_age=0.1):
        self.assertLessEqual(0.0, info_age)
        self.assertLessEqual(info_age, max_age)

    def test_query_single(self):
        shard_ids = ['shard-1', 'shard-2', 'shard-3']

        for shard_id in shard_ids:
            ShardPublisher.add_shard(self.make_shard(shard_id))

        response = self.servicer.GetInfos([shard_ids[0]])

        self.assertEqual(len(response), 1, 'Incorrect number of INFOs in response')
        self.assertEqual(response[0]['meta']['shard_identifier'], shard_ids[0])
        self.assertSensibleAge(response[0]['meta']['info_age'])

    def test_query_multi(self):
        shard_ids = ['shard-1', 'shard-2', 'shard-3', 'shard-4']
        query_ids = ['shard-2', 'shard-3']

        for shard_id in shard_ids:
            ShardPublisher.add_shard(self.make_shard(shard_id))

        response = self.servicer.GetInfos(query_ids)

        response_shard_ids = [info['meta']['shard_identifier'] for info in response]

        six.assertCountEqual(self, query_ids, response_shard_ids,
                             msg='Unexpected or missing shards in response')

        for info in response:
            self.assertSensibleAge(info['meta']['info_age'])

    def test_query_all(self):
        shard_ids = ['shard-1', 'shard-2', 'shard-3', 'shard-4']

        for shard_id in shard_ids:
            ShardPublisher.add_shard(self.make_shard(shard_id))

        response = self.servicer.GetInfos()

        response_shard_ids = [info['meta']['shard_identifier'] for info in response]

        six.assertCountEqual(self, shard_ids, response_shard_ids,
                             msg='Unexpected or missing shards in response')

        for info in response:
            self.assertSensibleAge(info['meta']['info_age'])

    def test_query_filtering(self):
        shard_id = 'shard-1'
        info = {
            'dummy_key1': 'dummy',
            'dummy_key2': 'dummy',
            'removed_key1': 'removed',
            'removed_key2': 'removed',
        }

        ShardPublisher.add_shard(self.make_shard(shard_id, info=info))

        resp_info = self.servicer.GetInfos(shard_ids=[shard_id], key_patterns=['dummy*'])[0]

        for k in resp_info.keys():
            self.assertNotIn('removed', k, 'key in response INFO that should have been filtered')
