from __future__ import print_function
import fnmatch
import time
from .shard_pub import ShardPublisher
from .redis_shard import InfoType, RedisShard
import logging
from typing import List, Sequence


logger = logging.getLogger(__name__)


class InfoProviderServicer(object):
    """Implements the InfoProvider RPC interface."""

    @classmethod
    def _filter_info(cls, full_info, key_patterns):
        # type: (InfoType, Sequence[str]) -> InfoType

        """
        Returns a filtered view of `full_info` according to the glob-style patterns in
        `key_patterns`. This allows, for example, filtering for only specific cmd stats.
        (e.g.: by using the pattern "cmdstat_hget*.")

        :param full_info: An info_pb2.Info instance to filter.
        :param key_patterns: A sequence of glob-style key patterns.
        """

        # Optimization: If no patterns specified, just return the full info that's already waiting
        if not key_patterns:
            return full_info

        # always include the metadata
        info = {'meta': full_info['meta']}

        for k, v in full_info.items():
            if cls._key_matches_pattern(k, key_patterns):
                info[k] = v

        return info

    @staticmethod
    def _key_matches_pattern(key_name, patterns):
        # type: (str, Sequence[str]) -> bool

        """
        Returns True IFF `key_name` matches one of the shell-style wildcard patterns
        in `patterns`.
        """
        return any(
            fnmatch.fnmatchcase(key_name, pattern) for pattern in patterns
        )

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
            logger.info('received request for unknown shard (%s)', shard_id)
            raise KeyError('shard {} not found'.format(shard_id))

        if shard.info is None:
            logger.info('received request for shard (%s) which seems to have not yet been polled', shard_id)
            raise KeyError('info for shard {} not available'.format(shard_id))

        return shard

    def GetInfos(self, shard_ids=(), key_patterns=(), allow_partial=False):
        # type: (Sequence[str], Sequence[str], bool) -> List[InfoType]

        """
        Returns a list of info dicts according to the shard-ids and key patterns
        specified in the query selector.
        :param shard_ids: List of shard identifiers to query. If empty, all live
            shards will be returned.
        :param key_patterns: List of glob-like patterns to filter by. If not
            empty, only keys that match one of the patterns will appear in the
            response INFOs. (Plus the 'meta' key that is always present.)
        :param allow_partial: If True, and a requested shard is missing or cannot
            be queried, a response will still be returned, with the info_age for that
            shard set to a very large value (>> century), and an additional 'error'
            string in the meta dictionary. If False (the default), the same condition
            will raise an exception.
        """

        resp = []

        logger.debug('Received request for shards %s, patterns %s',
                     shard_ids, key_patterns)

        shards_to_query = shard_ids or ShardPublisher.get_live_shard_ids()

        for shard_id in shards_to_query:
            try:
                shard = self._get_shard_with_info(shard_id)
                msg = self._filter_info(full_info=shard.info, key_patterns=key_patterns)
                msg['meta']['info_age'] = time.time() - shard.info_timestamp
            except KeyError as e:
                if allow_partial:
                    msg = {'meta': {
                        'info_age': 1e38,  # Very large, but still fits in a single-precision float
                        'error': str(e),
                    }}
                else:
                    raise

            msg['meta']['shard_identifier'] = shard_id

            resp.append(msg)

        return resp
