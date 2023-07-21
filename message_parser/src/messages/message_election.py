# src/messages/message_generate_vote.py
from .base_message import Message
import re


class ElectionMessage(Message):

    def parse_common(self, remainder):
        pass

    def parse_specific(self, remainder):
        pass


class GenerateVoteNormalMessage(ElectionMessage):

    def parse_specific(self, line):
        regex = r'root="(?P<root>[^"]+)", hash="(?P<hash>[^"]+)"'
        match = re.search(regex, line)

        if match:
            self.root = match.group('root')
            self.hash = match.group('hash')


class GenerateVoteFinalMessage(ElectionMessage):

    def parse_specific(self, line):
        regex = r'root="(?P<root>[^"]+)", hash="(?P<hash>[^"]+)"'
        match = re.search(regex, line)

        if match:
            self.root = match.group('root')
            self.hash = match.group('hash')
