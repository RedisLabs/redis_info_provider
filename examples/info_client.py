#!/usr/bin/env python

from __future__ import print_function
import argparse
import pprint

try:
    import xmlrpclib as xmlrpcclient
except ImportError:
    import xmlrpc.client as xmlrpcclient


def main():
    """
    Example client for reading INFOs from redis-info-server.
    """

    parser = argparse.ArgumentParser(description=main.__doc__,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-p', '--port', type=int, default=50051,
                        help='the port on which to connect to the server')
    parser.add_argument('--host', default='127.0.0.1',
                        help='the address of the server to connect to')
    parser.add_argument('-s', '--shard-ids', nargs='*', default=[],
                        help='identifiers of Redis shards to query (omit to query all shards)')
    parser.add_argument('--keys', nargs='*', default=[],
                        help='list of INFO keys to query (omit to query all keys)')
    args = parser.parse_args()

    server_addr = 'http://{}:{}/'.format(args.host, args.port)

    print('Connecting to INFO server at {}'.format(server_addr))
    printer = pprint.PrettyPrinter(indent=2)

    proxy = xmlrpcclient.ServerProxy(server_addr)
    print()

    for info in proxy.GetInfos(args.shard_ids, args.keys):
        print('-' * 20, 'INFO for shard {}'.format(info['meta']['shard_identifier']), '-' * 20)
        printer.pprint(info)
        print()


if __name__ == '__main__':
    main()
