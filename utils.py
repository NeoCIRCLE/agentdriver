from twisted.protocols.basic import LineReceiver
import json
import logging

root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)


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
        try:
            data = json.loads(data)
            args = data.get('args', {})
            if not isinstance(args, dict):
                args = {}
            command = data.get('command', None)
            response = data.get('response', None)
            logging.debug('[serial] valid json: %s' % (data, ))
        except (ValueError, KeyError) as e:
            logging.error('[serial] invalid json: %s (%s)' % (data, e))
            return

        if command is not None and isinstance(command, unicode):
            logging.debug('received command: %s (%s)' % (command, args))
            self.handle_command(command, args)
        elif response is not None and isinstance(response, unicode):
            logging.debug('received reply: %s (%s)' % (response, args))
            self.handle_response(response, args)
