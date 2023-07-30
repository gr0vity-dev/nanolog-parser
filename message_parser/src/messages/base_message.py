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
    def extract_attributes(log_line):
        attributes = {"json": [], "string": []}
        log_line = re.sub(r"^\[[^\]]*\] \[[^\]]*\] \[[^\]]*\] ", "", log_line)

        key, value = None, ""
        opened_brackets, closed_brackets = 0, 0
        is_json = False

        for char in log_line:
            if char == ' ' and value and not key:
                value = ""
            if char == '=' and value and not key:
                key = value.strip()
                value = ""
            elif char == '{':
                opened_brackets += 1
                is_json = True
                value += char
            elif char == '}':
                closed_brackets += 1
                value += char
            elif char == ',' and opened_brackets == closed_brackets:
                if is_json:
                    attributes["json"].append(key)
                else:
                    if key: attributes["string"].append(key)
                key, value = None, ""
                is_json = False
            else:
                value += char

        # append the last attribute
        if key:
            if is_json:
                attributes["json"].append(key)
            else:
                attributes["string"].append(key)

        return attributes

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
            response["content"] = str(line[base_attributes_match.end():log_event_match.start()] + \
                             line[log_event_match.end():]).strip()
        else:
            response["log_event"] = None
            response["content"] = str(
                line[base_attributes_match.end():]).strip()

        return response

    # def parse_json_attribute(line, attribute):
    #     regex = f'{attribute}={{(.*)}}'
    #     matches = re.findall(regex, line)
    #     string = "{" + matches[0] + "}" if matches else None
    #     string = re.sub(r'\s*=\s*', ':', string)
    #     string = re.sub(r'(?<=\{|,|\[)\s*([a-zA-Z0-9_]+)\s*(?=:)', r'"\1"',
    #                     string)
    #     return json.loads(string) if string else None


class BaseMessage():

    def __init__(self, message_dict):
        self.__dict__.update(message_dict)
        self.class_name = self.__class__.__name__
        self.post_init()

    def post_init(self):
        # This method does nothing in the base class, and is meant to be overridden in subclasses
        pass

    def __setattr__(self, name, value):
        self.__dict__[name] = value

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        else:
            raise AttributeError("No such attribute: " + name)