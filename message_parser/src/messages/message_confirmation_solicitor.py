from .base_message import Message
import re


class BroadcastMessage(Message):

    def parse_specific(self, line):
        regex = r'channel="(?P<channel>[^"]+)", hash="(?P<hash>[^"]+)"'
        match = re.search(regex, line)

        if match:
            self.channel = match.group('channel')
            self.hash = match.group('hash')
