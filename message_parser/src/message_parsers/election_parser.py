from .base_parser import BaseParser
from src.messages import GenerateVoteNormalMessage, GenerateVoteFinalMessage


class ElectionParser(BaseParser):
    MESSAGE_TYPES = {
        'generate_vote_normal': GenerateVoteNormalMessage,
        'generate_vote_final': GenerateVoteFinalMessage,
    }

    def get_message_type_regex(self):
        return r'\[election\] \[trace\] "(\w+)"'
