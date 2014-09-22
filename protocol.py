#!/usr/bin/env python

from twisted.internet import protocol, reactor
import pickle
import logging
import time
import struct
from threading import Event
from os import getenv
from celery.result import TimeoutError

from utils import SerialLineReceiverBase

from agentcelery import agent_started, agent_stopped, renew

logger = logging.getLogger()

reactor.connections = {}


class GraphiteClientProtocol(protocol.Protocol):
    def connectionMade(self):
        timestamp = time.time()
        data_list = []
        for key, value in self.data.items():
            if not isinstance(value, dict):
                continue
            for k, v in value.items():
                data_list.append(('agent.%s.%s.%s' % (self.name, key, k),
                                  (timestamp, float(v))))

        payload = pickle.dumps(data_list)
        header = struct.pack("!L", len(payload))
        message = header + payload
        self.transport.write(message)
        self.transport.loseConnection()
        logger.debug('s: %s' % self.data)


class GraphiteClientFactory(protocol.ClientFactory):
    protocol = GraphiteClientProtocol


def inotify_handler(self, file, mask):
    vm = file.basename()
    logger.info('inotify: %s' % vm)
    if vm in reactor.connections:
        return
    serial = SerialLineReceiverFactory(vm)
    logger.info("connecting to %s (%s)", vm, file.path)
    reactor.connectUNIX(file.path, serial)


class SerialLineReceiver(SerialLineReceiverBase):
    def send_to_graphite(self, data):
        client = GraphiteClientFactory()
        client.protocol.data = data
        client.protocol.name = self.factory.vm
        reactor.connectTCP(getenv('GRAPHITE_HOST', '127.0.0.1'),
                           int(getenv('GRAPHITE_PORT', '2004')),
                           client)

    def handle_command(self, command, args):
        if command == 'agent_stopped':
            agent_stopped.apply_async(queue='localhost.man',
                                      args=(self.factory.vm, ))
        elif command == 'agent_started':
            version = args.get('version', None)
            agent_started.apply_async(queue='localhost.man',
                                      args=(self.factory.vm, version))
        elif command == 'renew':
            renew.apply_async(queue='localhost.man',
                              args=(self.factory.vm, ))
        elif command == 'ping':
            self.send_response(response='pong',
                               args=args)

    def handle_response(self, response, args):
        if response == 'status':
            self.send_to_graphite(args)
        else:
            uuid = args.get('uuid', None)
            if not uuid:
                return
            event = self.factory.running_tasks.get(uuid, None)
            if event:
                self.factory.ended_tasks[uuid] = args
                event.set()

    def connectionMade(self):
        logger.info("connected to %s" % self.factory.vm)
        reactor.connections[self.factory.vm] = self

    def connectionLost(self, reason):
        logger.info("disconnected from %s" % self.factory.vm)
        del reactor.connections[self.factory.vm]

    def send_command(self, command, args, timeout=10.0, uuid=None):
        if not uuid:
            super(SerialLineReceiver, self).send_command(command, args)
            return

        event = Event()
        args['uuid'] = uuid
        self.factory.running_tasks[uuid] = event
        self.factory.ended_tasks[uuid] = None

        super(SerialLineReceiver, self).send_command(command, args)

        success = event.wait(timeout)
        retval = self.factory.ended_tasks[uuid]

        del self.factory.ended_tasks[uuid]
        del self.factory.running_tasks[uuid]

        if not success:
            raise TimeoutError()

        return retval


class SerialLineReceiverFactory(protocol.ClientFactory):
    protocol = SerialLineReceiver

    def __init__(self, vm):
        self.vm = vm
        self.running_tasks = {}
        self.ended_tasks = {}
