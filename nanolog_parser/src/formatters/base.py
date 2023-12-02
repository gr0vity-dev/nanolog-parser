from nanolog_parser.src.message_factory import MessageFactory
from nanolog_parser.src.parsing_utils import ParseException
from abc import ABC, abstractmethod


class IFormatter(ABC):

    @abstractmethod
    def format(self, line, filename, remove_prefix=""):
        pass


class FilenameAdjusterMixin:
    def adjust_filename(self, filename, remove_prefix=""):
        return filename.replace(remove_prefix, "").replace("node.log", "").replace(".log", "")


class MessageCreatorMixin:
    def __init__(self):
        self.ignored_lines = 0

    def create_message(self, line, filename):
        try:
            message = MessageFactory.create_message(line, filename)
            return message
        except ParseException as exc:
            print(exc)
            self.ignored_lines += 1
            return None
    
    def message_get_json(self, line, filename):
        try:
            message = MessageFactory.message_get_json(line, filename)
            return message
        except ParseException as exc:
            print(exc)
            self.ignored_lines += 1
            return None
