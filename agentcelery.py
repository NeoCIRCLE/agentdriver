# from twisted.internet.defer import Deferred
from twisted.internet import reactor  # threads
from celery.result import TimeoutError
from celery import Celery
from kombu import Queue, Exchange
from os import getenv
from socket import gethostname
from threading import Event
import logging

logger = logging.getLogger()

HOSTNAME = gethostname()
AMQP_URI = getenv('AMQP_URI')

celery = Celery('agent', broker=AMQP_URI)
celery.conf.update(CELERY_RESULT_BACKEND='amqp',
                   CELERY_TASK_RESULT_EXPIRES=300,
                   CELERY_QUEUES=(Queue(HOSTNAME + '.agent',
                                        Exchange('agent', type='direct'),
                                        routing_key='agent'), ))


def send_command(vm, command, *args, **kwargs):
    uuid = kwargs.get('uuid', None)
    timeout = kwargs.get('timeout', 58)
    if uuid:
        event = Event()
        reactor.running_tasks[vm][uuid] = event
        reactor.ended_tasks[vm][uuid] = None

    for conn in reactor.connections[vm]:
        if command == 'append' and 'vio-cloud' not in conn.transport.addr:
            continue
        logger.info('%s(%s, %s)', command, vm,
                    ', '.join(map(lambda x: str(x)[:100], kwargs.values())))
        conn.send_command(command=command, args=kwargs)

    if uuid:
        success = event.wait(timeout)
        retval = reactor.ended_tasks[vm][uuid]

        del reactor.ended_tasks[vm][uuid]
        del reactor.running_tasks[vm][uuid]

        if not success:
            raise TimeoutError()

        return retval


@celery.task(name='agent.change_password')
def change_password(vm, password):
    send_command(vm, command='change_password', password=password)


@celery.task(name='agent.set_hostname')
def set_hostname(vm, hostname):
    send_command(vm, command='set_hostname', hostname=hostname)


@celery.task(name='agent.restart_networking')
def restart_networking(vm):
    send_command(vm, command='restart_networking')


@celery.task(name='agent.set_time')
def set_time(vm, time):
    send_command(vm, command='set_time', time=time)


@celery.task(name='agent.mount_store')
def mount_store(vm, host, username, password):
    send_command(vm, command='mount_store', host=host,
                 username=username, password=password)


@celery.task(name='agent.cleanup')
def cleanup(vm):
    send_command(vm, command='cleanup')


@celery.task(name='agent.start_access_server')
def start_access_server(vm):
    send_command(vm, command='start_access_server')


@celery.task(name='agent.append')
def append(vm, data, filename, chunk_number):
    kwargs = {'command': 'append', 'data': data, 'chunk_number': chunk_number,
              'filename': filename, 'uuid': append.request.id}
    return send_command(vm, **kwargs)


@celery.task(name='agent.update_legacy')
def update_legacy(vm, data):
    kwargs = {'command': 'update', 'uuid': update_legacy.request.id,
              'data': data}
    return send_command(vm, **kwargs)


@celery.task(name='agent.update')
def update(vm, filename, executable, checksum):
    kwargs = {'command': 'update', 'uuid': update.request.id,
              'filename': filename, 'checksum': checksum,
              'executable': executable}
    return send_command(vm, **kwargs)


@celery.task(name='agent.add_keys')
def add_keys(vm, keys):
    send_command(vm, command='add_keys', keys=keys)


@celery.task(name='agent.del_keys')
def del_keys(vm, keys):
    send_command(vm, command='del_keys', keys=keys)


@celery.task(name='agent.get_keys')
def get_keys(vm):
    return send_command(vm, command='get_keys')


@celery.task(name='agent.send_expiration')
def send_expiration(vm, url):
    return send_command(vm, command='send_expiration',
                        url=url)


@celery.task(name='agent.change_ip')
def change_ip(vm, interfaces, dns):
    send_command(vm, command='change_ip', interfaces=interfaces, dns=dns)


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
