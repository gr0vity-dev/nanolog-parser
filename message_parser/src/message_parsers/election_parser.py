from .base_parser import BaseParser
from src.messages import GenerateVoteNormalMessage, GenerateVoteFinalMessage, UnknownMessage


class ElectionParser(BaseParser):
    MESSAGE_TYPES = {
        'generate_vote_normal': GenerateVoteNormalMessage,
        'generate_vote_final': GenerateVoteFinalMessage,
        'unknown': UnknownMessage,
    }

    def get_message_type_regex(self):
        return r'\[(election)\] \[\w+\]'

    def get_message_type_patterns(self):
        return {
            "generate_vote_normal": r'\[trace\] "generate_vote_normal"',
            "generate_vote_final": r'\[trace\] "generate_vote_final"',
        }

    def get_default_message_type(self):
        return 'unknown'
