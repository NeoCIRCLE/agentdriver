# from twisted.internet.defer import Deferred
from twisted.internet import reactor  # threads
from celery import Celery
from kombu import Queue, Exchange
from os import getenv
from socket import gethostname
import logging

logger = logging.getLogger(__name__)

HOSTNAME = gethostname()
AMQP_URI = getenv('AMQP_URI')
CACHE_URI = getenv('CACHE_URI')

celery = Celery('agent', broker=AMQP_URI)
celery.conf.update(CELERY_CACHE_BACKEND=CACHE_URI,
                   CELERY_RESULT_BACKEND='cache',
                   CELERY_TASK_RESULT_EXPIRES=300,
                   CELERY_QUEUES=(Queue(HOSTNAME + '.agent',
                                        Exchange('agent', type='direct'),
                                        routing_key='agent'), ))


@celery.task(name='agent.change_password')
def change_password(vm, password):
    reactor.connections[vm].send_command(command='change_password',
                                         args={'password':
                                               password})
    logger.debug('change_password(%s,%s)' % (vm, password))


@celery.task(name='agent.set_hostname')
def set_hostname(vm, hostname):
    reactor.connections[vm].send_command(command='set_hostname',
                                         args={'hostname':
                                               hostname})
    logger.debug('set_hostname(%s,%s)' % (vm, hostname))


@celery.task(name='agent.restart_networking')
def restart_networking(vm):
    reactor.connections[vm].send_command(command='restart_networking',
                                         args={})
    logger.debug('restart_networking(%s)' % (vm))


@celery.task(name='agent.set_time')
def set_time(vm, time):
    reactor.connections[vm].send_command(command='set_time',
                                         args={'time': time})
    logger.debug('set_time(%s,%s)' % (vm, time))


@celery.task(name='agent.mount_store')
def mount_store(vm, host, username, password):
    reactor.connections[vm].send_command(command='mount_store',
                                         args={'host': host,
                                               'username': username,
                                               'password': password})
    logger.debug('mount_store(%s,%s,%s)' % (vm, host, username))


@celery.task(name='vm.tasks.local_agent_tasks.agent_started')
def agent_started(vm):
    print vm


@celery.task(name='vm.tasks.local_agent_tasks.agent_stopped')
def agent_stopped(vm):
    print vm


@celery.task(name='vm.tasks.local_agent_tasks.agent_ok')
def agent_ok(vm):
    print vm


# class StartProcTask(celery.Task):
#    def run(self):
#        print 'HELLO'*10
#        self.app.proc = WCProcessProtocol('testing')
#        self.app.proc._waiting['startup'] = Deferred()
#        def lofasz(asd):
#            print 'ezjott%s' % asd
#        self.app.proc._waiting['startup'].addCallback(lofasz)
#        threads.blockingCallFromThread(reactor, reactor.spawnProcess,
#                                       self.app.proc, 'ls', ['ls'])
#        return True
