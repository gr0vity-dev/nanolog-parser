from functools import wraps


class ParseException(Exception):

    def __init__(self, message="Error parsing message"):
        self.message = message
        super().__init__(self.message)