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
        self.behaviour = election["behaviour"]
        self.state = election["state"]
        self.winner = election["winner"]
        self.tally_amount = election["tally_amount"]
        self.final_tally_amount = election["final_tally_amount"]
        self.confirmed = election["confirmed"]
        self.blocks = election["blocks"]
        self.votes = election["votes"]
        self.tally = election["tally"]
