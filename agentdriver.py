import select
import socket
import logging
import signal

import json
import pickle
from inotify.watcher import Watcher
from inotify import IN_ALL_EVENTS
from os.path import basename, join
from os import getenv, listdir
from urlparse import urlparse
from time import sleep

from librabbitmq import Connection, ConnectionError

logger = logging.getLogger()
logging.basicConfig()
logger.setLevel(getenv('LOGLEVEL', 'INFO'))

SOCKET_DIR = getenv('SOCKET_DIR', '/var/lib/libvirt/serial')


class LineReceiver(socket.socket):
    buf = ''
    MAX_LINE = 10240

    def readlines(self):
        data = self.recv(4096)
        if not len(data):
            raise Exception('Disconnected.')

        if len(self.buf) + len(data) > self.MAX_LINE:
            self.buf = data
        else:
            self.buf += data

        lines = [line for line in self.buf.splitlines() if line]
        if not (self.buf.endswith('\n') or self.buf.endswith('\r')):
            self.buf = lines.pop()
        else:
            self.buf = ''

        return lines


class AgentDriver(object):
    def __init__(self, url, hostname):
        self._enabled = True
        self._ports = {}
        self._pending_tasks = {}
        self._url = urlparse(url)
        self._hostname = hostname

    def stop(self):
        logger.info('STOP signal')
        self._enabled = False

    def open_serial(self, path):
        name = basename(path)
        if name.startswith('vio-'):
            name = name.replace('vio-', '')
            type = 'virtio'
        else:
            type = 'old'

        for k, v in self._ports.items():
            if v['name'] == name and v['type'] == type:
                logger.info('%s: already connected' % path)
                return

        logger.info("Connecting to %s (%s)", name, path)
        conn = LineReceiver(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            conn.settimeout(1.0)
            conn.connect(path)  # TODO reconnect?
        except Exception as e:
            logger.info("Unable to connect to %s: %s", path, str(e))
            return

        logger.info("Connected to %s (%s)", name, path)

        fd = conn.fileno()
        self._poller.register(
            fd, select.EPOLLIN | select.EPOLLHUP | select.EPOLLERR)
        self._ports[fd] = {"name": name, "type": type, "socket": conn}

    def close_serial(self, fd):
        port = self._ports.pop(fd)
        port["socket"].close()
        self._poller.unregister(fd)
        logger.info("Disconnected from %s", port["name"])

    def send_celery_result(self, task_id, vm, result=None, exception=None):
        sleep(0.05)

        status = 'FAILURE' if exception else 'SUCCESS'

        body = {'status': status, 'result': exception or result,
                'task_id': task_id, 'children': []}

        self._channel.basic_publish(
            body=pickle.dumps(body, protocol=2),
            exchange='celeryresults',
            routing_key=task_id.replace('-', ''),
            correlation_id=task_id,
            delivery_mode=2,
            content_type='application/x-python-serialize',
            content_encoding='binary')

    def send_celery_task(self, task, vm, *args):
        task_id = 32 * '5'
        args = (vm, ) + args
        task = "vm.tasks.local_agent_tasks.%s" % task
        logger.info('New task from agent: %s(%s, %s)',
                    task, vm, str(args)[:30])

        body = {'utc': True, 'chord': None, 'args': args, 'retries': 0,
                'expires': None,  'task': task, 'callbacks': None,
                'errbacks': None, 'timelimit': (None, None),
                'taskset': None, 'kwargs': {}, 'eta': None, 'id': task_id}

        self._channel.basic_publish(
            body=pickle.dumps(body, protocol=2),
            exchange='manager',
            routing_key='manager',
            correlation_id=task_id,
            delivery_mode=2,
            content_type='application/x-python-serialize',
            content_encoding='binary')

    def handle_pidbox(self, message):
        msg = json.loads(str(message.body))
        if msg.get('method') != 'active_queues':
            return
        reply = {("celery@%s" % self.queue): [{"name": self.queue}]}
        self._pidbox.basic_publish(
            body=json.dumps(reply),
            exchange=msg['reply_to']['exchange'],
            routing_key=msg['reply_to']['routing_key'],
            delivery_mode=2,
            content_type='application/json',
            content_encoding='utf-8')
        message.ack()

    def handle_celery_task(self, message):
        message.ack()

        msg = pickle.loads(message.body)
        task_name = msg['task'].split('.')[-1]
        task_id = msg['id']
        vm = msg['args'][0]
        args = msg['args'][1:]

        try:
            logger.info('New task (%s) from manager: %s(%s, %s)',
                        task_id, task_name, vm, str(args)[:50])
            wait = Task.call(task_name=task_name, task_id=task_id,
                             driver=self, vm=vm, args=args)
            exception = None
        except BaseException as e:
            logger.exception('Unhandled exception:')
            wait = False
            exception = e

        if wait:
            self._pending_tasks[task_id] = vm
        else:
            self.send_celery_result(task_id, vm, exception=exception)

    def handle_serial_task(self, port, data):
        try:
            data = json.loads(data)
            args = data.get('args', {})
            if not isinstance(args, dict):
                args = {}
            command = data.get('command', None)
            response = data.get('response', None)
            logger.debug('[serial] valid json: %s', data)
        except (ValueError, KeyError) as e:
            logger.error('[serial] invalid json: %s (%s)', data, e)
            return

        task = (command or response)
        task_id = args.get('uuid', None)
        if not task or not isinstance(task, unicode):
            logger.error('[serial] invalid json (missing task name)')
            return

        logger.debug('[serial] received %s (%s)', task, args)

        if task == 'agent_stopped':
            self.send_celery_task('agent_stopped', port["name"])
        elif task == 'agent_started':
            version = args.get('version', None)
            system = args.get('system', None)
            self.send_celery_task(
                'agent_started', port["name"], version, system)
        elif task_id and task_id in self._pending_tasks:
            if self._pending_tasks[task_id] == port["name"]:
                logger.info('Reply (%s) from %s: %s', task_id, port["name"],
                            str(args)[:50])
                self.send_celery_result(task_id, port["name"], args)
            else:
                logger.warning('Bad reply (%s) from %s (!= %s)', task_id,
                               port["name"], self._pending_tasks[task_id])

    def get_ports_by_vm_name(self, vm):
        for port in self._ports.values():
            if port["name"] == vm:
                yield port

    def send_command_to_vm(self, vm, command, **kwargs):
        # TODO logging
        # TODO handle connection errors
        for port in self.get_ports_by_vm_name(vm):
            if port['type'] == 'old' and command == 'append':
                continue
            port['socket'].send(
                json.dumps({'command': command, 'args': kwargs}) + '\r\n')

    @property
    def queue(self):
        return '%s.agent' % self._hostname

    def open_amqp(self):
        exchange = self._hostname
        url = self._url
        vhost = url.path.strip('/')
        while True:
            try:
                self._conn = Connection(
                    host=url.hostname, userid=url.username,
                    password=url.password, virtual_host=vhost)
            except ConnectionError as e:
                logger.error('Unable to connect to amqp server: %s' % str(e))
                sleep(2)
            else:
                break

        self._pidbox = pidbox = self._conn.channel()
        q = 'celery@%s.celery.pidbox' % self.queue
        pidbox.exchange_declare('celery.pidbox', type='fanout')
        pidbox.queue_declare(q, auto_delete=True)
        pidbox.queue_bind(q, 'celery.pidbox')
        pidbox.basic_consume(q, callback=self.handle_pidbox)

        self._channel = channel = self._conn.channel()
        channel.exchange_declare(exchange, type='direct', durable=True)
        channel.queue_declare(self.queue, durable=True)
        channel.queue_bind(self.queue, exchange)
        channel.basic_consume(self.queue, callback=self.handle_celery_task)

        logger.info('Connected to amqp server: %s', url.geturl())
        self._poller.register(
            self._conn.fileno(),
            select.EPOLLIN | select.EPOLLHUP | select.EPOLLERR)

    def close_amqp(self):
        self._conn.close()

    def create_watcher(self):
        watcher = Watcher()
        watcher.add(SOCKET_DIR, IN_ALL_EVENTS)
        self._poller.register(
            watcher.fileno(),
            select.EPOLLIN | select.EPOLLHUP | select.EPOLLERR)

        for f in listdir(SOCKET_DIR):
            self.open_serial(join(SOCKET_DIR, f))

        return watcher

    def start(self):
        self._poller = select.poll()

        self.open_amqp()

        watcher = self.create_watcher()

        while self._enabled:
            try:
                events = self._poller.poll(1000)
            except select.error:
                break

            try:
                self._conn.drain_events(-1)
            except socket.timeout:
                pass
            except ConnectionError as e:
                logger.error('Disconnected from amqp server: %s' % str(e))
                self.close_amqp()
                self.open_amqp()

            for fd, flag in events:
                port = self._ports.get(fd)
                if port:
                    try:
                        lines = port["socket"].readlines()
                    except:
                        self.close_serial(fd)
                    else:
                        try:
                            for line in lines:
                                self.handle_serial_task(port, line)
                        except:
                            logger.exception('Error %s:', port)

                elif fd == watcher.fileno():
                    for f in watcher.read(0):
                        self.open_serial(f.fullpath)

        for i in self._ports.values():
            i["socket"].close()

        self.close_amqp()


class Task(object):  # celery task
    tasks = {}

    @classmethod
    def add(cls, name, *args, **kwargs):
        cls.tasks[name] = (args, kwargs)

    @classmethod
    def call(cls, task_name, task_id, driver, vm, args):
        param_names, options = cls.tasks[task_name]
        assert len(param_names) == len(args)
        wait = options.get('wait', False)
        kwargs = dict(zip(param_names, args))
        if wait:
            kwargs['uuid'] = task_id

        driver.send_command_to_vm(vm, task_name, **kwargs)

        return wait


Task.add('change_password', 'password')
Task.add('set_hostname', 'hostname')
Task.add('restart_networking')
Task.add('set_time', 'time')
Task.add('mount_store', 'host', 'username', 'password')
Task.add('cleanup')
Task.add('start_access_server')
Task.add('change_ip', 'interfaces', 'dns')

Task.add('send_expiration', 'url')

Task.add('append', 'data', 'filename', 'chunk_number', wait=True)
Task.add('update', 'filename', 'executable', 'checksum', wait=True)

Task.add('add_keys', 'keys')
Task.add('del_keys', 'keys')
Task.add('get_keys')


def signal_handler(signum, frame):
    driver.stop()


driver = AgentDriver(getenv('AMQP_URI'), socket.gethostname())

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

driver.start()
