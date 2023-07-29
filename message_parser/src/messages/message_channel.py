from . import ConfirmAckMessage


class ConfirmAckMessageSent(ConfirmAckMessage):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.action = "message_sent"


class ConfirmAckMessageDropped(ConfirmAckMessage):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.action = "message_dropped"