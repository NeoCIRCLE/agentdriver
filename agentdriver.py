from celery.apps.worker import Worker
from twisted.internet import reactor, inotify
from twisted.python import filepath
from agentcelery import celery
from protocol import inotify_handler
from os import getenv
import logging


SOCKET_DIR = getenv('SOCKET_DIR', '/var/lib/libvirt/serial')


def main():
    w = Worker(app=celery, concurrency=1,
               pool_cls='threads',
               loglevel=logging.DEBUG)
    reactor.callInThread(w.run)
    notifier = inotify.INotify(reactor)
    notifier.startReading()
    notifier.watch(filepath.FilePath(SOCKET_DIR),
                   callbacks=[inotify_handler])
    reactor.run()

if __name__ == '__main__':
    main()
