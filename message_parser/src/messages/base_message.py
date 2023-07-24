from abc import ABC, abstractmethod
from .mixins import BaseAttributesMixin
from src.parsing_utils import ParseException


class Message(ABC, BaseAttributesMixin):

    def __init__(self, src_file=None):
        self.log_timestamp = None
        self.log_process = None
        self.log_level = None
        self.log_event = None
        self.class_name = self.__class__.__name__
        self.log_file = src_file

    def parse(self, line, base_attributes=True):  # Template Method
        self.parse_base_attributes(line, base_attributes)
        self._parse_common(self.remainder)
        self._parse_specific(self.remainder)
        return self

    def _parse_common(self, remainder):  # Protected method
        try:
            self.parse_common(remainder)
        except Exception as ex:
            raise ParseException(
                f'Error parsing {self.__class__.__name__} message') from ex

    @abstractmethod
    def parse_common(self, remainder):
        pass

    @abstractmethod
    def parse_specific(self, remainder):
        pass

    def _parse_specific(self, remainder):  # Protected method
        try:
            self.parse_specific(remainder)
        except Exception as ex:
            raise ParseException(
                f'Error parsing {self.__class__.__name__} message') from ex

    def normalize_timestamp(self, timestamp):
        if timestamp == 18446744073709551615:
            return -1
        return timestamp
