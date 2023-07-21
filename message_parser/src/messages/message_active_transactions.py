from .base_message import Message
import re


class ActiveTransactionsMessage(Message):

    def parse_common(self, remainder):
        pass

    def parse_specific(self, remainder):
        pass


class ActiveStartedMessage(ActiveTransactionsMessage):

    def parse_specific(self, remainder):
        regex = r'root="(?P<root>[^"]+)", hash="(?P<hash>[^"]+)", behaviour="(?P<behaviour>[^"]+)"'
        match = re.search(regex, remainder)

        if match:
            self.root = match.group('root')
            self.hash = match.group('hash')
            self.behaviour = match.group('behaviour')


class ActiveStoppedMessage(ActiveTransactionsMessage):

    def parse_specific(self, remainder):
        regex = r'root="(?P<root>[^"]+)", hashes=\[(?P<hashes>[^]]+)\], behaviour="(?P<behaviour>[^"]+)", confirmed=(?P<confirmed>\w+)'
        match = re.search(regex, remainder)

        if match:
            self.root = match.group('root')
            self.hashes = [
                h.strip().replace('"', '')
                for h in match.group('hashes').split(',')
            ]
            self.behaviour = match.group('behaviour')
            self.confirmed = match.group('confirmed') == 'true'
