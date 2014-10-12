#!/usr/bin/env python

from twisted.internet import protocol, reactor
import pickle
import logging
import time
import struct
from os import getenv

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
        logger.info("Monitor info from: %s", self.name)


class GraphiteClientFactory(protocol.ClientFactory):
    protocol = GraphiteClientProtocol


def inotify_handler(self, file, mask):
    vm = file.basename().replace('vio-', '')
    logger.info('inotify: %s (%s)', vm, file.path)
    for conn in reactor.connections.get(vm, []):
        if file.path == conn.transport.addr:
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
            system = args.get('system', None)
            agent_started.apply_async(queue='localhost.man',
                                      args=(self.factory.vm, version, system))
        elif command == 'renew':
            renew.apply_async(queue='localhost.man',
                              args=(self.factory.vm, ))
        elif command == 'ping':
            self.send_response(response='pong',
                               args=args)

    def handle_response(self, response, args):
        vm = self.factory.vm
        if response == 'status':
            self.send_to_graphite(args)
        else:
            uuid = args.get('uuid', None)
            if not uuid:
                return
            event = reactor.running_tasks[vm].get(uuid, None)
            if event:
                reactor.ended_tasks[vm][uuid] = args
                event.set()

    def connectionMade(self):
        logger.info("connected to %s (%s)", self.factory.vm,
                    self.transport.addr)
        if self.factory.vm not in reactor.connections:
            reactor.connections[self.factory.vm] = set()
        reactor.connections[self.factory.vm].add(self)

    def connectionLost(self, reason):
        logger.info("disconnected from %s (%s)", self.factory.vm,
                    self.transport.addr)
        reactor.connections[self.factory.vm].remove(self)


class SerialLineReceiverFactory(protocol.ClientFactory):
    protocol = SerialLineReceiver

    def __init__(self, vm):
        self.vm = vm
        if vm not in reactor.running_tasks:
            reactor.running_tasks[vm] = {}
        if vm not in reactor.ended_tasks:
            reactor.ended_tasks[vm] = {}
