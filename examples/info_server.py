#!/usr/bin/env python


from __future__ import print_function
import gevent.hub
from gevent.event import AsyncResult

import redis_info_provider
import argparse
from threading import Thread

try:
    import xmlrpclib
    from SimpleXMLRPCServer import SimpleXMLRPCServer
except ImportError:
    import xmlrpc.client as xmlrpclib
    from xmlrpc.server import SimpleXMLRPCServer


def main():
    """
    Example server for serving INFO of all locally running redis-servers via XML-RPC.
    Since it is based on the non-production-quality LocalShardWatcher, it might fail for
    Redis servers using non-standard configurations, and will not work for Redises that
    require authentication.
    """

    parser = argparse.ArgumentParser(description=main.__doc__,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-p', '--port', type=int, default=50051,
                        help='the port on which the server should listen')
    parser.add_argument('-a', '--address', default='127.0.0.1',
                        help='the address on which the server should listen')

    args = parser.parse_args()

    # prevent KeyboardInterrupt thrown in a greenlet from causing a stack trace to be printed
    gevent.hub.Hub.NOT_ERROR = (KeyboardInterrupt,)

    # Python's xmlrpc module serializes integers as i4 (32-bits) out of the box. This line causes all ints to
    # be serialized as i8 (64-bits), as some Redis INFO values require the additional space.
    xmlrpclib.Marshaller.dispatch[int] = lambda _, value, write: write('<value><i8>{}</i8></value>'.format(value))

    print('Starting INFO server on {}:{}...'.format(args.address, args.port))

    server = SimpleXMLRPCServer((args.address, args.port))
    server.register_instance(redis_info_provider.InfoProviderServicer())
    Thread(target=server.serve_forever).start()

    try:
        with redis_info_provider.LocalShardWatcher() as watcher , redis_info_provider.InfoPoller() as poller:
            wait_event  = AsyncResult()
            watcher.set_wait(wait_event)
            poller.set_wait(wait_event)
            print('INFO server running.')
            wait_event.get()
    except KeyboardInterrupt:
        print('INFO server stopping.')
    finally:
        server.shutdown()
        server.server_close()


if __name__ == '__main__':
    main()

