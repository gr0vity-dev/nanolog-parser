import re


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
