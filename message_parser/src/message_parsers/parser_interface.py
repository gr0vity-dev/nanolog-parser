from abc import ABC, abstractmethod
import re


class IMessageToJsonConverter(ABC):

    @abstractmethod
    def convert_to_json(self, line: str) -> dict:
        pass


class MessageToJsonConverter(IMessageToJsonConverter):

    def convert_to_json(self, line: str) -> dict:
        # implement json conversion logic here
        pass


class IMessageTypeIdentifier(ABC):

    @abstractmethod
    def identify_message_type(self, json: dict) -> type:
        pass


class MessageTypeIdentifier(IMessageTypeIdentifier):

    def identify_message_type(self, json: dict) -> type:
        # implement message type identification logic here
        pass
