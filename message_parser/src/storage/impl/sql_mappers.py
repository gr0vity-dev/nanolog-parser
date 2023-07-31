import json
from src.storage.impl.sql_mixins import *
from src.storage.impl.sql_mapper_interface import IMapper
from src.storage.impl.sql_relation import SqlRelations


class MessageMapper(MessageMixin, IMapper):
    pass


# class NetworkMessageMapper(NetworkMessageMixin, IMapper):
#     pass

# class ConfirmAckMessageMapper(NetworkMessageMixin, IMapper):

#     def to_dict(self):
#         data = super().to_dict()
#         self.message.timestamp = SQLDataNormalizer.adjust_max_timestamp(
#             self.message.timestamp)
#         data.update({
#             'account': self.message.account,
#             'timestamp': self.message.timestamp,
#             'hash_count': self.message.hash_count,
#             'vote_type': self.message.vote_type,
#         })
#         return data

#     def get_table_schema(self):
#         return super().get_table_schema() + [
#             ('account', 'text'),
#             ('timestamp', 'integer'),
#             ('hash_count', 'integer'),
#             ('vote_type', 'text'),
#         ]

#     def get_related_entities(self):
#         relations = SqlRelations(self, self.message.hashes, 'hashes', 'hash')
#         return relations.get_mappers()


# class PublishMessageMapper(NetworkMessageMixin, IMapper):

#     def to_dict(self):
#         data = super().to_dict()
#         data.update({
#             'block_type': self.message.block_type,
#             'hash': self.message.hash,
#             'account': self.message.account,
#             'previous': self.message.previous,
#             'representative': self.message.representative,
#             'balance': self.message.balance,
#             'link': self.message.link,
#             'signature': self.message.signature
#         })
#         return data

#     def get_table_schema(self):
#         return super().get_table_schema() + [('block_type', 'text'),
#                                              ('hash', 'text'),
#                                              ('account', 'text'),
#                                              ('previous', 'text'),
#                                              ('representative', 'text'),
#                                              ('balance', 'text'),
#                                              ('link', 'text'),
#                                              ('signature', 'text')]


class BlockProcessedMessageMapper(MessageMixin, IMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'result': self.message.result,
            'hash': self.message.block["hash"],
            'forced': self.message.forced
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [('result', 'text'),
                                             ('hash', 'text'),
                                             ('forced', 'bool')]

    def get_related_entities(self):
        relations = SqlRelations()
        block = self.message.block
        relations.add_relations_from_data(self, block, 'blocks')
        return relations.get_mappers()


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


class ActiveStartedMessageMapper(MessageMixin, IMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'root': self.message.election["root"],
            'behaviour': self.message.election["behaviour"],
            'state': self.message.election["state"],
            'confirmed': self.message.election["confirmed"],
            'winner': self.message.election["winner"],
            'tally_amount': self.message.election["tally_amount"],
            'final_tally_amount': self.message.election["final_tally_amount"],
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [
            ('root', 'text'),
            ('behaviour', 'text'),
            ('state', 'text'),
            ('confirmed', 'boolean'),
            ('winner', 'text'),
            ('tally_amount', 'text'),  # or 'bigint' if the amount fits
            ('final_tally_amount', 'text'),  # or 'bigint' if the amount fits
        ]

    def get_related_entities(self):
        relations = SqlRelations()
        blocks = self.message.election["blocks"]
        votes = self.message.election["votes"]
        tally = self.message.election["tally"]

        relations.add_relations_from_data(self, blocks, 'blocks')
        relations.add_relations_from_data(self, votes, 'votes')
        relations.add_relations_from_data(self, tally, 'tally')
        return relations.get_mappers()


class ActiveStoppedMessageMapper(MessageMixin, IMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'root': self.message.election["root"],
            'behaviour': self.message.election["behaviour"],
            'state': self.message.election["state"],
            'confirmed': self.message.election["confirmed"],
            'winner': self.message.election["winner"],
            'tally_amount': self.message.election["tally_amount"],
            'final_tally_amount': self.message.election["final_tally_amount"],
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [
            ('root', 'text'),
            ('behaviour', 'text'),
            ('state', 'text'),
            ('confirmed', 'boolean'),
            ('winner', 'text'),
            ('tally_amount', 'text'),  # or 'bigint' if the amount fits
            ('final_tally_amount', 'text'),  # or 'bigint' if the amount fits
        ]

    def get_related_entities(self):
        relations = SqlRelations()
        blocks = self.message.election["blocks"]
        votes = self.message.election["votes"]
        tally = self.message.election["tally"]

        relations.add_relations_from_data(self, blocks, 'blocks')
        relations.add_relations_from_data(self, votes, 'votes')
        relations.add_relations_from_data(self, tally, 'tally')
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


class ElectionGenerateVoteNormalMessageMapper(MessageMixin, IMapper):

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


class ElectionGenerateVoteFinalMessageMapper(MessageMixin, IMapper):

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


class HeaderMapper(MessageMixin, IMapper):

    def set_header_relation(self):
        relations = SqlRelations()
        header = self.message.message["header"]
        relations.add_relations_from_data(self, header, 'headers')
        return relations

    def get_related_entities(self):
        relations = self.set_header_relation()

        return relations.get_mappers()


class ChannelMessageMapper(HeaderMapper):
    pass


class ChannelConfirmAckMapper(HeaderMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update(
            {'vote_count': len(self.message.message["vote"]["hashes"])})
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [('vote_count', 'int')]

    def get_related_entities(self):
        relations = super().set_header_relation()
        vote = self.message.message["vote"]
        channel = self.message.channel

        relations.add_relations_from_data(self, vote, 'votes')
        relations.add_relations_from_data(self, channel, 'channels')

        return relations.get_mappers()


class NetworkMessageMapper(HeaderMapper):
    pass


class ConfirmAckMessageMapper(NetworkMessageMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update(
            {'vote_count': len(self.message.message["vote"]["hashes"])})
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [('vote_count', 'int')]

    def get_related_entities(self):
        relations = super().set_header_relation()
        vote = self.message.message["vote"]

        relations.add_relations_from_data(self, vote, 'votes')

        return relations.get_mappers()


class PublishMessageMapper(NetworkMessageMapper):

    def get_related_entities(self):
        relations = super().set_header_relation()
        block = self.message.message["block"]
        relations.add_relations_from_data(self, block, 'blocks')
        return relations.get_mappers()


class KeepAliveMessageMapper(NetworkMessageMapper):

    def get_related_entities(self):
        relations = super().set_header_relation()
        relations.add_relations_from_data(
            self, self.message.message["peers"], 'peers', 'peer')
        return relations.get_mappers()


class ASCPullAckMessageMapper(NetworkMessageMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'id': str(self.message.message["id"]),
            'type': self.message.message["type"]
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [('id', 'text'), ('type', 'text')]

    def get_related_entities(self):
        relations = super().set_header_relation()
        blocks = self.message.message["blocks"]
        relations.add_relations_from_data(self, blocks, 'blocks')
        return relations.get_mappers()


class ASCPullReqMessageMapper(NetworkMessageMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'id': str(self.message.message["id"]),
            'start': self.message.message["start"],
            'start_type': self.message.message["start_type"],
            'count': self.message.message["count"]
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [('id', 'text'), ('start', 'text'),
                                             ('start_type', 'text'),
                                             ('count', 'integer')]


class ConfirmReqMessageMapper(NetworkMessageMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'root_count': len(self.message.message["roots"])
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [
            ('root_count', 'integer'),
        ]

    def get_related_entities(self):
        relations = super().set_header_relation()
        relations.add_relations_from_data(
            self, self.message.message["roots"], 'roots')
        return relations.get_mappers()


class FlushMessageMapper(MessageMixin, IMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'channel': self.message.channel,
            'root_count': len(self.message.confirm_req["roots"])
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [
            ('channel', 'text'),
            ('root_count', 'integer'),
        ]

    def get_related_entities(self):
        relations = SqlRelations()
        header = self.message.confirm_req["header"]
        block = self.message.confirm_req["block"]
        roots = self.message.confirm_req["roots"]

        relations.add_relations_from_data(self, header, 'headers')
        relations.add_relations_from_data(self, roots, 'roots')
        if block:
            relations.add_relations_from_data(self, block, 'blocks')
        return relations.get_mappers()


class ProcessConfirmedMessageMapper(MessageMixin, IMapper):

    def get_related_entities(self):
        relations = SqlRelations()
        block = self.message.block

        relations.add_relations_from_data(
            self, block, 'blocks')
        return relations.get_mappers()
