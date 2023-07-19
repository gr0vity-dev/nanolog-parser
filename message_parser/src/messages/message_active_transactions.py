from .base_message import Message
import re


class ActiveStartedMessage(Message):

    def parse_specific(self, line):
        regex = r'root="(?P<root>[^"]+)", hash="(?P<hash>[^"]+)", behaviour="(?P<behaviour>[^"]+)"'
        match = re.search(regex, line)

        if match:
            self.root = match.group('root')
            self.hash = match.group('hash')
            self.behaviour = match.group('behaviour')


class ActiveStoppedMessage(Message):

    def parse_specific(self, line):
        regex = r'root="(?P<root>[^"]+)", hashes=\[(?P<hashes>[^]]+)\], behaviour="(?P<behaviour>[^"]+)", confirmed=(?P<confirmed>\w+)'
        match = re.search(regex, line)

        if match:
            self.root = match.group('root')
            self.hashes = [
                h.strip().replace('"', '')
                for h in match.group('hashes').split(',')
            ]
            self.behaviour = match.group('behaviour')
            self.confirmed = match.group('confirmed') == 'true'
