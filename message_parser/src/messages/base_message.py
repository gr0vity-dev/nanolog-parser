import re
import json

import re
import json


class BaseMessage():

    def __init__(self, message_dict):
        self.__dict__.update(message_dict)
        self.class_name = self.__class__.__name__
        self.post_init()

    def post_init(self):
        # This method does nothing in the base class, and is meant to be overridden in subclasses
        pass

    def remove_attribute(self, attribute_name):
        if attribute_name in self.__dict__:
            del self.__dict__[attribute_name]
        else:
            raise AttributeError("No such attribute: " + attribute_name)

    def __setattr__(self, name, value):
        self.__dict__[name] = value

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        else:
            raise AttributeError("No such attribute: " + name)
