#!/usr/bin/env python

from twisted.internet import protocol, reactor
import pickle
import logging
import time
import struct
from utils import SerialLineReceiverBase

from agentcelery import agent_started, agent_stopped

logger = logging.getLogger(__name__)

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
    reactor.connectUNIX(file.path, serial)


class SerialLineReceiver(SerialLineReceiverBase):
    def send_to_graphite(self, data):
        client = GraphiteClientFactory()
        client.protocol.data = data
        client.protocol.name = self.factory.vm
        reactor.connectTCP('10.7.0.96', 2004, client)

    def handle_command(self, command, args):
        if command == 'agent_stopped':
            agent_stopped.apply_async(queue='localhost.man',
                                      args=(self.factory.vm, ))
        if command == 'agent_started':
            agent_started.apply_async(queue='localhost.man',
                                      args=(self.factory.vm, ))
        if command == 'ping':
            self.send_response(response='pong',
                               args=args)

    def handle_response(self, response, args):
        if response == 'status':
            self.send_to_graphite(args)

    def connectionMade(self):
        logger.info("connected to %s" % self.factory.vm)
        reactor.connections[self.factory.vm] = self

    def connectionLost(self, reason):
        logger.info("disconnected from %s" % self.factory.vm)
        del reactor.connections[self.factory.vm]


class SerialLineReceiverFactory(protocol.ClientFactory):
    protocol = SerialLineReceiver

    def __init__(self, vm):
        self.vm = vm
