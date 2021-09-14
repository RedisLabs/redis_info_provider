from __future__ import print_function
import time
import functools
import warnings
from .shard_pub import ShardPublisher
from .redis_shard import InfoType, RedisShard
import logging
from typing import List, Sequence, AbstractSet


logger = logging.getLogger(__name__)


def deprecated_alias(**aliases):
    """
    Decorator that renames deprecated parameter names of a function to their new names and then calls the function with
    the new parameter names.
    :param aliases: Dict[str,str] - mapping of deprecated parameter names to their new names.
    """
    def deco(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            rename_kwargs(f.__name__, kwargs, aliases)
            return f(*args, **kwargs)
        return wrapper
    return deco


def rename_kwargs(func_name, kwargs, aliases):
    """
    Rename deprecated parameters of kwargs to their new name as as it appears in aliases dict.
    :param func_name: str (name of function that the decorator is applied to)
    :param kwargs: Dict[str, str] of arguments the given function was called with
    :param aliases: Dict[str, str] that maps deprecated parameters to new parameter names
    :raises TypeError if both an argument and its deprecated alias were received
    """
    for alias, new in aliases.items():
        if alias in kwargs:
            if new in kwargs:
                raise TypeError('{} received both {} and {}'.format(func_name, alias, new))
            warnings.warn('{} is deprecated; use {}'.format(alias, new), DeprecationWarning)
            kwargs[new] = kwargs.pop(alias)


class InfoProviderServicer(object):
    """Implements the InfoProvider RPC interface."""

    @classmethod
    def _filter_info(cls, full_info, keys, prefix_matching=False):
        # type: (InfoType, AbstractSet[str]) -> InfoType

        """
        Returns a filtered view of `full_info` according to the key-list specified in
        `keys`. This reduces network overhead by only transmitting the desired subset
        of keys.

        :param full_info: An info_pb2.Info instance to filter.
        :param keys: A set of strings to match for.
        :param prefix_matching: If True, we treat each key in `keys` as a prefix of a
            field in `full_info`
        """

        def is_matching_key(full_key, keys, prefix_matching=False):
            if not prefix_matching:
                return full_key in keys
            else:
                for key in keys:
                    if full_key.startswith(key):
                        return True
                return False

        # Optimization: If no keys specified, just return the full info that's already waiting
        if not keys:
            return full_info

        info = {k: full_info[k] for k in full_info if is_matching_key(k, keys, prefix_matching=prefix_matching)}
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

    @deprecated_alias(key_patterns='keys')
    def GetInfos(self, shard_ids=(), keys=(), allow_partial=False, max_age=0.0, prefix_matching=False):
        # type: (Sequence[str], Sequence[str], bool, float) -> List[InfoType]

        """
        Returns a list of info dicts according to the shard-ids and keys
        specified in the query selector.
        Note: if deprecated parameter "key_patterns" is passed, we use it as "keys" for backwards compatibility.
        However, filtering by glob-like patterns is no longer supported.
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
        :param prefix_matching: If True, we treat each key in `keys` as a prefix of a
            field in `full_info`
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
                msg = self._filter_info(full_info=shard.info, keys=set(keys), prefix_matching=prefix_matching)
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
