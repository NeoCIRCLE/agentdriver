from celery.apps.worker import Worker
from twisted.internet import reactor, inotify
from twisted.python import filepath
from agentcelery import celery, HOSTNAME
from protocol import inotify_handler
from os import getenv, listdir, path
import logging


SOCKET_DIR = getenv('SOCKET_DIR', '/var/lib/libvirt/serial')


old_install_platform_tweaks = Worker.install_platform_tweaks


def install_platform_tweaks(self, worker):
    self.worker = worker
    old_install_platform_tweaks(self, worker)
Worker.install_platform_tweaks = install_platform_tweaks


def reactor_started():
    for f in listdir(SOCKET_DIR):
        f = path.join(SOCKET_DIR, f)
        inotify_handler(None, filepath.FilePath(f), None)


def reactor_stopped(worker):
    worker.worker.stop()


def main():
    w = Worker(app=celery, concurrency=1,
               pool_cls='threads',
               hostname=HOSTNAME + '.agentdriver',
               loglevel=logging.DEBUG)
    reactor.callInThread(w.run)
    notifier = inotify.INotify(reactor)
    notifier.startReading()
    notifier.watch(filepath.FilePath(SOCKET_DIR),
                   callbacks=[inotify_handler])
    reactor.callWhenRunning(reactor_started)
    reactor.addSystemEventTrigger("before", "shutdown", reactor_stopped, w)
    reactor.run()

if __name__ == '__main__':
    main()
