from .base_parser import BaseParser
from src.messages import UnknownMessage


class UnknownParser(BaseParser):

    MESSAGE_TYPES = {'unknown': UnknownMessage}

    def get_message_type_regex(self):
        return r'\/\/\/ we should not be able to find this \/\/\/'

    def get_message_type_patterns(self):
        return {"unknown": r''}

    def get_default_message_type(self):
        return 'unknown'