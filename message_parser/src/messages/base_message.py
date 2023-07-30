from abc import ABC, abstractmethod
from .mixins import BaseAttributesMixin
from src.parsing_utils import ParseException
import re
import json

import re
import json


class MessageAttributeParser:

    @staticmethod
    def parse_attribute(line, attribute):
        regex = f'{attribute}="(?P<{attribute}>[^"]+)"'
        match = re.search(regex, line)
        if match:
            return match.group(attribute)

    @staticmethod
    def parse_json_attribute(line, attribute):
        attr_location = line.find(f'{attribute}=')
        if attr_location == -1: return None

        open_count = 0
        close_count = 0
        attr_value = ''
        for char in line[attr_location + len(attribute) + 1:]:
            if char == '{': open_count += 1
            if char == '}': close_count += 1
            attr_value += char
            if open_count != 0 and open_count == close_count:
                break

        # Converting to proper json
        attr_value = re.sub(r'\s*=\s*', ':', attr_value)
        attr_value = re.sub(r'(?<=\{|,|\[)\s*([a-zA-Z0-9_]+)\s*(?=:)', r'"\1"',
                            attr_value)

        return json.loads(attr_value)

    @staticmethod
    def parse_base_attributes(line, file_name=None):

        response = {}

        # define the pattern for the basic attributes
        pattern = r'\[(.+?)\] \[(.+?)\] \[(.+?)\]'
        base_attributes_match = re.match(pattern, line)

        # check if the base attributes match the pattern
        if base_attributes_match:
            response["log_timestamp"] = base_attributes_match.group(1)
            response["log_process"] = base_attributes_match.group(2)
            response["log_level"] = base_attributes_match.group(3).split(
                '"')[0].strip()
            response["log_file"] = file_name
        else:
            raise ValueError(f"Wrong log format for base attributes: {line}")

        # try to match the log_event
        event_pattern = r'\"(.+?)\"'
        log_event_match = re.search(event_pattern, line)

        # if log_event exists, assign it and cut it from the remainder, otherwise leave it None
        if log_event_match:
            response["log_event"] = log_event_match.group(1)
            response["content"] = line[base_attributes_match.end():log_event_match.start()] + \
                             line[log_event_match.end():]
        else:
            response["log_event"] = None
            response["content"] = line[base_attributes_match.end():]

        return response

    # def parse_json_attribute(line, attribute):
    #     regex = f'{attribute}={{(.*)}}'
    #     matches = re.findall(regex, line)
    #     string = "{" + matches[0] + "}" if matches else None
    #     string = re.sub(r'\s*=\s*', ':', string)
    #     string = re.sub(r'(?<=\{|,|\[)\s*([a-zA-Z0-9_]+)\s*(?=:)', r'"\1"',
    #                     string)
    #     return json.loads(string) if string else None


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