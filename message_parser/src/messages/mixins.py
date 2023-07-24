import re


class BaseAttributesMixin:

    def parse_base_attributes(self, line, base_attributes=True):

        if not base_attributes:
            self.remainder = line
            return self

        # define the pattern for the basic attributes
        pattern = r'\[(.+?)\] \[(.+?)\] \[(.+?)\]'
        base_attributes_match = re.match(pattern, line)

        # check if the base attributes match the pattern
        if base_attributes_match:
            self.log_timestamp = base_attributes_match.group(1)
            self.log_process = base_attributes_match.group(2)
            self.log_level = base_attributes_match.group(3).split(
                '"')[0].strip()
        else:
            raise ValueError(f"Wrong log format for base attributes: {line}")

        # try to match the log_event
        event_pattern = r'\"(.+?)\"'
        log_event_match = re.search(event_pattern, line)

        # if log_event exists, assign it and cut it from the remainder, otherwise leave it None
        if log_event_match:
            self.log_event = log_event_match.group(1)
            self.remainder = line[base_attributes_match.end():log_event_match.start()] + \
                             line[log_event_match.end():]
        else:
            self.log_event = None
            self.remainder = line[base_attributes_match.end():]

        # strip any leading/trailing whitespace from the remainder
        self.remainder = self.remainder.strip()

        return self


class HeaderMixin:

    def __init__(self):
        self.message_type = None
        self.network = None
        self.network_int = None
        self.version = None
        self.version_min = None
        self.version_max = None
        self.extensions = None

    def parse_header(self, header_dict):
        self.message_type = header_dict['type']
        self.network = header_dict['network']
        self.network_int = header_dict['network_int']
        self.version = header_dict['version']
        self.version_min = header_dict['version_min']
        self.version_max = header_dict['version_max']
        self.extensions = header_dict['extensions']

        return self
