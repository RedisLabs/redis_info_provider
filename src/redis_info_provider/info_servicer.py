from __future__ import print_function
import fnmatch
import time
from .shard_pub import ShardPublisher
from .redis_shard import InfoType, RedisShard
import logging
from typing import List, Sequence, AbstractSet


logger = logging.getLogger(__name__)


class InfoProviderServicer(object):
    """Implements the InfoProvider RPC interface."""

    @classmethod
    def _filter_info(cls, full_info, keys):
        # type: (InfoType, AbstractSet[str]) -> InfoType

        """
        Returns a filtered view of `full_info` according to the key-list specified in
        `keys`. This reduces network overhead by only transmitting the desired subset
        of keys.

        :param full_info: An info_pb2.Info instance to filter.
        :param keys: A set of strings to match for.
        """

        # Optimization: If no keys specified, just return the full info that's already waiting
        if not keys:
            return full_info

        info = {k: full_info[k] for k in keys if k in full_info}
        # always include the metadata
        info['meta'] = full_info['meta']

        return info

    @staticmethod
    def _get_shard_with_info(shard_id):
        # type: (str) -> RedisShard

        """
        Gets the shard object from the ShardPublisher if it contains valid INFO, or raises an
        appropriate exception otherwise.
        :param shard_id: The identifier of the shard.
        :return: The shard object for shard 'shard_id'.
        """

        try:
            shard = ShardPublisher.get_shard(shard_id)
        except KeyError:
            logger.warning('received request for unknown shard (%s)', shard_id)
            raise KeyError('shard {} not found'.format(shard_id))

        if shard.info is None:
            logger.warning('received request for shard (%s) which seems to have not yet been polled', shard_id)
            raise KeyError('info for shard {} not available'.format(shard_id))

        return shard

    def GetInfos(self, shard_ids=(), keys=(), allow_partial=False, max_age=0.0):
        # type: (Sequence[str], Sequence[str], bool, float) -> List[InfoType]

        """
        Returns a list of info dicts according to the shard-ids and keys
        specified in the query selector.
        :param shard_ids: List of shard identifiers to query. If empty, all live
            shards will be returned.
        :param keys: List of exact-match keys to filter for. If not empty, only
            keys that are in this list will appear in the response INFOs.
            (Plus the 'meta' key that is always present.)
        :param allow_partial: If True, and a requested shard is missing or cannot
            be queried, a response will still be returned, with the info_age for that
            shard set to a very large value (>> century), and an additional 'error'
            string in the meta dictionary. If False (the default), the same condition
            will raise an exception.
        :param max_age: If specified and non-zero, only shard infos whose age is less-
            than-or-equal-to max_age will be returned in the response.
        """

        resp = []

        logger.debug('Received request for shards %s, keys %s', shard_ids, keys)

        shards_to_query = (
                shard_ids or
                # If all shards were requested, only consider the live ones that have already been polled at least once
                [shard.id for shard in ShardPublisher.get_live_shards() if shard.info]
        )

        for shard_id in shards_to_query:
            try:
                shard = self._get_shard_with_info(shard_id)
                info_age = time.time() - shard.info_timestamp
                if max_age and info_age > max_age:
                    logger.debug('Shard %s info age %s > %s (max-age); Skipping', shard_id, info_age, max_age)
                    continue
                msg = self._filter_info(full_info=shard.info, keys=set(keys))
                msg['meta']['info_age'] = info_age
            except KeyError as e:
                if allow_partial:
                    msg = {'meta': {
                        'info_age': 1e38,  # Very large, but still fits in a single-precision float
                        'error': str(e).strip('\'"'),  # str() for KeyError adds extra quotes that we don't want
                    }}
                else:
                    raise

            msg['meta']['shard_identifier'] = shard_id

            resp.append(msg)

        return resp
