import json


class MessageMapper:

    def __init__(self, message):
        self.message = message

    def get_table_name(self):
        return self.message.class_name.lower()

    def to_dict(self):
        return {
            'log_timestamp': self.message.log_timestamp,
            'log_process': self.message.log_process,
            'log_level': self.message.log_level,
            'log_event': self.message.log_event,
        }

    def get_table_schema(self):
        return [
            ('log_timestamp', 'text'),
            ('log_process', 'text'),
            ('log_level', 'text'),
            ('log_event', 'text'),
        ]


class NetworkMessageMapper(MessageMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'message_type': self.message.message_type,
            'network': self.message.network,
            'network_int': self.message.network_int,
            'version': self.message.version,
            'version_min': self.message.version_min,
            'version_max': self.message.version_max,
            'extensions': self.message.extensions
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [
            ('message_type', 'text'),
            ('network', 'text'),
            ('network_int', 'integer'),
            ('version', 'integer'),
            ('version_min', 'integer'),
            ('version_max', 'integer'),
            ('extensions', 'integer'),
        ]


class ConfirmAckMessageMapper(NetworkMessageMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'account': self.message.account,
            'timestamp': self.message.timestamp,
            'hashes': json.dumps(self.message.hashes)
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [
            ('account', 'text'),
            ('timestamp', 'integer'),
            ('hashes', 'text'),
        ]


class ConfirmReqMessageMapper(NetworkMessageMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({'roots': json.dumps(self.message.roots)})
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [('roots', 'text')]


class PublishMessageMapper(NetworkMessageMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'block_type': self.message.block_type,
            'hash': self.message.hash,
            'account': self.message.account,
            'previous': self.message.previous,
            'representative': self.message.representative,
            'balance': self.message.balance,
            'link': self.message.link,
            'signature': self.message.signature
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [('block_type', 'text'),
                                             ('hash', 'text'),
                                             ('account', 'text'),
                                             ('previous', 'text'),
                                             ('representative', 'text'),
                                             ('balance', 'text'),
                                             ('link', 'text'),
                                             ('signature', 'text')]


class KeepAliveMessageMapper(NetworkMessageMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({'peers': json.dumps(self.message.peers)})
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [('peers', 'text')]


class ASCPullAckMessageMapper(NetworkMessageMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'id': self.message.id,
            'blocks': json.dumps(self.message.blocks)
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [('id', 'text'),
                                             ('blocks', 'text')]


class ASCPullReqMessageMapper(NetworkMessageMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'id': self.message.id,
            'start': self.message.start,
            'start_type': self.message.start_type,
            'count': self.message.count
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [('id', 'text'), ('start', 'text'),
                                             ('start_type', 'text'),
                                             ('count', 'integer')]


class BlockProcessorMessageMapper(MessageMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'result': self.message.result,
            'block_type': self.message.block_type,
            'hash': self.message.hash,
            'account': self.message.account,
            'previous': self.message.previous,
            'representative': self.message.representative,
            'balance': self.message.balance,
            'link': self.message.link,
            'signature': self.message.signature,
            'work': self.message.work,
            'forced': self.message.forced
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [('result', 'text'),
                                             ('block_type', 'text'),
                                             ('hash', 'text'),
                                             ('account', 'text'),
                                             ('previous', 'text'),
                                             ('representative', 'text'),
                                             ('balance', 'text'),
                                             ('link', 'text'),
                                             ('signature', 'text'),
                                             ('work', 'text'),
                                             ('forced', 'bool')]
