import json
from src.storage.impl.sql_mixins import *
from src.storage.impl.sql_mapper_interface import IMapper
from src.storage.impl.sql_relation import SqlRelations
from src.storage.impl.sql_normalizer import SQLDataNormalizer


class MessageMapper(MessageMixin, IMapper):
    pass


class NetworkMessageMapper(NetworkMessageMixin, IMapper):
    pass


class ConfirmAckMessageMapper(NetworkMessageMixin, IMapper):

    def to_dict(self):
        data = super().to_dict()
        self.message.timestamp = SQLDataNormalizer.adjust_max_timestamp(
            self.message.timestamp)
        data.update({
            'account': self.message.account,
            'timestamp': self.message.timestamp,
            'hash_count': self.message.hash_count,
            'vote_type': self.message.vote_type,
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [
            ('account', 'text'),
            ('timestamp', 'integer'),
            ('hash_count', 'integer'),
            ('vote_type', 'text'),
        ]

    def get_related_entities(self):
        relations = SqlRelations(self, self.message.hashes, 'hashes', 'hash')
        return relations.get_mappers()


class ConfirmReqMessageMapper(NetworkMessageMixin, IMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'root_count': self.message.root_count,
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [
            ('root_count', 'integer'),
        ]

    def get_related_entities(self):
        relations = SqlRelations(self, self.message.roots, 'roots')
        return relations.get_mappers()


class PublishMessageMapper(NetworkMessageMixin, IMapper):

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


class KeepAliveMessageMapper(NetworkMessageMixin, IMapper):

    def to_dict(self):
        data = super().to_dict()
        return data

    def get_table_schema(self):
        return super().get_table_schema()

    def get_related_entities(self):
        relations = SqlRelations(self, self.message.peers, 'peers', 'peer')
        return relations.get_mappers()


class ASCPullAckMessageMapper(NetworkMessageMixin, IMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({'id': self.message.id})
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [('id', 'text')]

    def get_related_entities(self):
        relations = SqlRelations(self, self.message.blocks, 'blocks')
        return relations.get_mappers()


class ASCPullReqMessageMapper(NetworkMessageMixin, IMapper):

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


class BlockProcessedMessageMapper(MessageMixin, IMapper):

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


class ProcessedBlocksMessageMapper(MessageMixin, IMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'processed_blocks': self.message.processed_blocks,
            'forced_blocks': self.message.forced_blocks,
            'process_time': self.message.process_time
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [('processed_blocks', 'int'),
                                             ('forced_blocks', 'int'),
                                             ('process_time', 'int')]


class BlocksInQueueMessageMapper(MessageMixin, IMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'blocks_in_queue': self.message.blocks_in_queue,
            'state_blocks': self.message.state_blocks,
            'forced_blocks': self.message.forced_blocks
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [('blocks_in_queue', 'int'),
                                             ('state_blocks', 'int'),
                                             ('forced_blocks', 'int')]


class NodeProcessConfirmedMessageMapper(MessageMixin, IMapper):

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
            'signature': self.message.signature,
            'work': self.message.work
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
                                             ('signature', 'text'),
                                             ('work', 'text')]

    def get_related_entities(self):

        #Nested dicts can't be dynamcailly mapped
        self.message.sideband["details"] = str(
            self.message.sideband["details"])

        relations = SqlRelations(self, self.message.sideband, 'sideband')
        return relations.get_mappers()


class ActiveStartedMessageMapper(MessageMixin, IMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'root': self.message.root,
            'hash': self.message.hash,
            'behaviour': self.message.behaviour,
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [('root', 'text'),
                                             ('hash', 'text'),
                                             ('behaviour', 'text')]


class ActiveStoppedMessageMapper(MessageMixin, IMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'root': self.message.root,
            'behaviour': self.message.behaviour,
            'confirmed': self.message.confirmed,
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [('root', 'text'),
                                             ('behaviour', 'text'),
                                             ('confirmed', 'boolean')]

    def get_related_entities(self):
        SQLDataNormalizer.normalize_block_fields(self.message.blocks)
        SQLDataNormalizer.normalize_vote_fields(self.message.votes)

        relations = SqlRelations(self, self.message.blocks, 'blocks')
        relations += SqlRelations(self, self.message.votes, 'votes')
        relations += SqlRelations(self, self.message.votes, 'tally')
        return relations.get_mappers()


class BroadcastMessageMapper(MessageMixin, IMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'channel': self.message.channel,
            'hash': self.message.hash,
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [('channel', 'text'),
                                             ('hash', 'text')]


class GenerateVoteNormalMessageMapper(MessageMixin, IMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'root': self.message.root,
            'hash': self.message.hash,
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [('root', 'text'),
                                             ('hash', 'text')]


class GenerateVoteFinalMessageMapper(MessageMixin, IMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'root': self.message.root,
            'hash': self.message.hash,
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [('root', 'text'),
                                             ('hash', 'text')]


class UnknownMessageMapper(MessageMixin, IMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'content': self.message.content,
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [('content', 'text')]


class FlushMessageMapper(MessageMixin, IMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'root_count': self.message.confirm_req.root_count,
            'channel': self.message.channel,
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [
            ('channel', 'text'),
            ('root_count', 'integer'),
        ]

    def get_related_entities(self):
        relations = SqlRelations(self, self.message.confirm_req.roots, 'roots')
        return relations.get_mappers()


class ChannelMessageMapper(MessageMixin, IMapper):

    def to_dict(self):
        data = super().to_dict()
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [('vote_count', 'int')]

    def get_related_entities(self):
        header = self.message.content["message"]["header"]
        vote = SQLDataNormalizer.normalize_vote(
            self.message.content["message"]["vote"])
        channel = SQLDataNormalizer.normalize_channel(
            self.message.content["channel"])

        relations = SqlRelations(self, header, 'headers')
        relations += SqlRelations(self, vote, 'votes')
        relations += SqlRelations(self, channel, 'channels')

        return relations.get_mappers()


class ConfirmAckMessageSentMapper(ChannelMessageMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'vote_count':
            len(self.message.content["message"]["vote"]["hashes"])
        })
        return data
