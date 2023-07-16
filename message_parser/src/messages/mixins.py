class BaseAttributesMixin:

    def parse_base_attributes(self, line):
        # Split the line on square brackets to isolate base attributes
        components = line.split('] [')

        # Extract base attributes
        self.log_timestamp = components[0][
            1:]  # Strip off the leading square bracket
        self.log_process = components[1]

        # Split the third component on quotation marks to isolate log_level and log_event
        level_event_components = components[2].split('"')

        # Before the first quote is log_level. Also, we remove any trailing ']'
        self.log_level = level_event_components[0].strip().split(']')[0]

        # Between the first and second quotes is log_event
        self.log_event = level_event_components[1]

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
