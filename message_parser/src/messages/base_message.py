from abc import ABC, abstractmethod
from .mixins import BaseAttributesMixin


class Message(ABC, BaseAttributesMixin):

    def __init__(self, src_file=None):
        self.log_timestamp = None
        self.log_process = None
        self.log_level = None
        self.log_event = None
        self.class_name = self.__class__.__name__
        self.log_file = src_file

    def parse(self, line):
        self.parse_base_attributes(line)
        self.parse_specific(line)
        return self

    @abstractmethod
    def parse_specific(self, line):
        pass

    def normalize_timestamp(self, timestamp):
        if timestamp == 18446744073709551615:
            return -1
        return timestamp
