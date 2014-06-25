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
    logger.debug('change_password(%s,%s)', vm, password)


@celery.task(name='agent.set_hostname')
def set_hostname(vm, hostname):
    reactor.connections[vm].send_command(command='set_hostname',
                                         args={'hostname':
                                               hostname})
    logger.debug('set_hostname(%s,%s)', vm, hostname)


@celery.task(name='agent.restart_networking')
def restart_networking(vm):
    reactor.connections[vm].send_command(command='restart_networking',
                                         args={})
    logger.debug('restart_networking(%s)', vm)


@celery.task(name='agent.set_time')
def set_time(vm, time):
    reactor.connections[vm].send_command(command='set_time',
                                         args={'time': time})
    logger.debug('set_time(%s,%s)', vm, time)


@celery.task(name='agent.mount_store')
def mount_store(vm, host, username, password):
    reactor.connections[vm].send_command(command='mount_store',
                                         args={'host': host,
                                               'username': username,
                                               'password': password})
    logger.debug('mount_store(%s,%s,%s)', vm, host, username)


@celery.task(name='agent.cleanup')
def cleanup(vm):
    reactor.connections[vm].send_command(command='cleanup', args={})
    logger.debug('cleanup(%s)', vm)


@celery.task(name='agent.start_access_server')
def start_access_server(vm):
    reactor.connections[vm].send_command(
        command='start_access_server', args={})
    logger.debug('start_access_server(%s)', vm)


@celery.task(name='agent.update')
def update(vm, data):
    logger.debug('update(%s)', vm)
    return reactor.connections[vm].send_command(
        command='update', args={'data': data}, uuid=update.request.id)


@celery.task(name='agent.add_keys')
def add_keys(vm, keys):
    logger.debug('add_keys(%s, %s)', vm, keys)
    reactor.connections[vm].send_command(
        command='add_keys', args={'keys': keys})


@celery.task(name='agent.del_keys')
def del_keys(vm, keys):
    logger.debug('del_keys(%s, %s)', vm, keys)
    reactor.connections[vm].send_command(
        command='del_keys', args={'keys': keys})


@celery.task(name='agent.get_keys')
def get_keys(vm):
    logger.debug('get_keys(%s)', vm)
    return reactor.connections[vm].send_command(
        command='get_keys', args={}, uuid=get_keys.request.id)


@celery.task(name='agent.send_notification')
def send_notification(vm, msg):
    logger.debug('send_notification(%s, %s)', vm, msg)
    return reactor.connections[vm].send_command(
        command='send_notification', args={'msg': msg})


@celery.task(name='vm.tasks.local_agent_tasks.renew')
def renew(vm):
    print vm


@celery.task(name='vm.tasks.local_agent_tasks.agent_started')
def agent_started(vm):
    print vm


@celery.task(name='vm.tasks.local_agent_tasks.agent_stopped')
def agent_stopped(vm):
    print vm


@celery.task(name='vm.tasks.local_agent_tasks.agent_ok')
def agent_ok(vm):
    print vm
