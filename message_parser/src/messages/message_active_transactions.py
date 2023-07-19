from .base_message import Message
from .mixins import BaseAttributesMixin
import re


class ActiveStartedMessage(Message, BaseAttributesMixin):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.root = None
        self.hash = None
        self.behaviour = None

    def parse(self, line):
        self.parse_base_attributes(line)
        self.parse_specific(line)
        return self

    def parse_specific(self, line):
        regex = r'root="(?P<root>[^"]+)", hash="(?P<hash>[^"]+)", behaviour="(?P<behaviour>[^"]+)"'
        match = re.search(regex, line)

        if match:
            self.root = match.group('root')
            self.hash = match.group('hash')
            self.behaviour = match.group('behaviour')


class ActiveStoppedMessage(Message, BaseAttributesMixin):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.root = None
        self.hashes = None
        self.behaviour = None
        self.confirmed = None

    def parse(self, line):
        self.parse_base_attributes(line)
        self.parse_specific(line)
        return self

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