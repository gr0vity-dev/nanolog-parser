from abc import ABC, abstractmethod


class Message(ABC):

    def __init__(self, src_file=None):
        self.log_timestamp = None
        self.log_process = None
        self.log_level = None
        self.log_event = None
        self.class_name = self.__class__.__name__
        self.log_file = src_file

    @abstractmethod
    def parse(self, line):
        pass

    def normalize_timestamp(self, timestamp):
        if timestamp == 18446744073709551615:
            return -1
        return timestamp