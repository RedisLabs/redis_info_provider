from __future__ import print_function
import fnmatch
import time
from .shard_pub import ShardPublisher
from .redis_shard import InfoType
import logging
from typing import Mapping, Any, List


logger = logging.getLogger(__name__)


class InfoProviderServicer(object):
    """Implements the InfoProvider RPC interface."""

    @classmethod
    def _filter_info(cls, full_info, key_patterns):
        # type: (InfoType, List[str]) -> InfoType

        """
        Returns a filtered view of `full_info` according to the glob-style patterns in
        `key_patterns`. This allows, for example, filtering for only specific cmd stats.
        (e.g.: by using the pattern "cmdstat_hget*.")

        :param full_info: An info_pb2.Info instance to filter.
        :param key_patterns: An iterable of glob-style key patterns.
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
        # type: (str, List[str]) -> bool

        """
        Returns True IFF `key_name` matches one of the shell-style wildcard patterns
        in `patterns`.
        """
        return any(
            fnmatch.fnmatchcase(key_name, pattern) for pattern in patterns
        )

    def GetInfos(self, shard_ids, key_patterns):
        # type: (List[str], List[str]) -> List[InfoType]

        """
        Returns a list of info dicts according to the shard-ids and key patterns
        specified in the query selector.
        """

        resp = []

        logger.debug('Received request for shards %s, patterns %s',
                     shard_ids, key_patterns)

        shards_to_query = shard_ids or ShardPublisher.get_live_shard_ids()

        for shard_id in shards_to_query:
            try:
                shard = ShardPublisher.get_shard(shard_id)
            except KeyError:
                logger.info('received request for unknown shard (%s)', shard_id)
                raise KeyError('shard {} not found'.format(shard_id))

            if shard.info is None:
                logger.info('received request for shard (%s) which seems to have not yet been polled', shard_id)
                raise KeyError('info for shard {} not available'.format(shard_id))

            msg = self._filter_info(full_info=shard.info, key_patterns=key_patterns)
            msg['meta']['info_age'] = time.time() - shard.info_timestamp

            resp.append(msg)

        return resp
