from .base_message import Message, MessageAttributeParser
import re


class ActiveTransactionsMessage(Message):

    def parse_common(self, remainder):
        pass

    def parse_specific(self, remainder):
        pass


class ActiveStartedMessage(ActiveTransactionsMessage):

    def parse_specific(self, remainder):
        election = MessageAttributeParser.parse_json_attribute(
            remainder, "election")

        self.root = election["root"]
        self.hash = election["winner"]
        self.behaviour = election["behaviour"]


class ActiveStoppedMessage(ActiveTransactionsMessage):

    def parse_specific(self, remainder):
        election = MessageAttributeParser.parse_json_attribute(
            remainder, "election")

        self.root = election["root"]
        self.hashes = [block["hash"] for block in election["blocks"]]
        self.behaviour = election["behaviour"]
        self.confirmed = election["confirmed"]
        self.blocks = election["blocks"]
        self.votes = election["votes"]
        self.tally = election["tally"]
