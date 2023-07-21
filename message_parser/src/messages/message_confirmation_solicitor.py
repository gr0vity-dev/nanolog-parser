from .base_message import Message
import re


class ConfirmationSolicitorMessage(Message):

    def parse_common(self, remainder):
        pass

    def parse_specific(self, remainder):
        pass


class BroadcastMessage(ConfirmationSolicitorMessage):

    def parse_specific(self, line):
        regex = r'channel="(?P<channel>[^"]+)", hash="(?P<hash>[^"]+)"'
        match = re.search(regex, line)

        if match:
            self.channel = match.group('channel')
            self.hash = match.group('hash')
