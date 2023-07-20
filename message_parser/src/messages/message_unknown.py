from .base_message import Message


class UnknownMessage(Message):

    def __init__(self, src_file=None):
        super().__init__(src_file)
        self.content = None

    def parse(self, line):
        self.parse_base_attributes(line)
        self.parse_specific(self.remainder)
        return self

    def parse_specific(self, line):
        # Extract content after the log_event
        self.content = line
