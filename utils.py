from twisted.protocols.basic import LineReceiver
import json
import logging

logger = logging.getLogger()


class SerialLineReceiverBase(LineReceiver, object):
    delimiter = '\r'

    def send_response(self, response, args):
        self.transport.write(json.dumps({'response': response,
                                         'args': args}) + '\r\n')

    def send_command(self, command, args):
        self.transport.write(json.dumps({'command': command,
                                         'args': args}) + '\r\n')

    def handle_command(self, command, args):
        raise NotImplementedError("Subclass must implement abstract method")

    def handle_response(self, response, args):
        raise NotImplementedError("Subclass must implement abstract method")

    def lineReceived(self, data):
        if not data.strip():  # ignore empty lines
            return
        try:
            data = json.loads(data)
            args = data.get('args', {})
            if not isinstance(args, dict):
                args = {}
            command = data.get('command', None)
            response = data.get('response', None)
            logger.debug('[serial] valid json: %s' % (data, ))
        except (ValueError, KeyError) as e:
            logger.error('[serial] invalid json: %s (%s)' % (data, e))
            return

        if command is not None and isinstance(command, unicode):
            logger.debug('received command: %s (%s)' % (command, args))
            self.handle_command(command, args)
        elif response is not None and isinstance(response, unicode):
            logger.debug('received reply: %s (%s)' % (response, args))
            self.handle_response(response, args)
