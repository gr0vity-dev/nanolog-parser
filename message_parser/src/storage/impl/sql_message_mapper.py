import json
from abc import ABC, abstractmethod


class Relation:

    def __init__(self, parent_entity_name, message_id, data, table_name):
        self.hashable_mapper = HashableMapper(data, table_name)
        self.link_mapper = LinkMapper({
            'message_type':
            parent_entity_name,
            'message_id':
            message_id,
            'relation_type':
            table_name,
            'relation_id':
            self.hashable_mapper.to_key(),
        })

    def get_mappers(self):
        # Its important to retrun HashableMapper before LinkMapper
        # as the store_message needs hashable_mapper sql_id to exist
        # when creating the link_mapper sql entry
        return [self.hashable_mapper, self.link_mapper]


class Relations:

    def __init__(self,
                 message_mapper,
                 data_list,
                 table_name,
                 key_for_string=None):
        self.message_mapper = message_mapper
        self.relations = []
        for data in data_list:
            if isinstance(data, dict):
                self.relations.append(
                    Relation(self.message_mapper.parent_entity_name,
                             self.message_mapper.sql_id, data, table_name))

            # If the data is a string, we convert it to a dictionary before creating a Relation object
            # The key for this dictionary is provided by the key_for_string argument.
            # We do this because the Relation object expects its data argument to be a dictionary, not a string.
            elif isinstance(data, str) and key_for_string is not None:
                self.relations.append(
                    Relation(self.message_mapper.parent_entity_name,
                             self.message_mapper.sql_id,
                             {key_for_string: data}, table_name))
            else:
                raise TypeError(
                    "Data list must contain either dictionaries or strings (with key_for_string provided)"
                )

    # This function simply collates all the mappers from all the Relation objects
    def get_mappers(self):
        return [
            mapper for relation in self.relations
            for mapper in relation.get_mappers()
        ]


class BaseMapper:

    def __init__(self, data):
        self.data = data
        self.sql_id = None

    def get_table_name(self):
        raise NotImplementedError()

    def get_table_schema(self):
        raise NotImplementedError()

    def to_dict(self):
        raise NotImplementedError()

    def to_key(self):
        #return "_".join(self.data.values())
        return "_".join(list(self.to_dict().values()))

    def handle_table(self):
        return self.get_table_name(), self.get_table_schema(), self.to_dict()

    def get_related_entities(self):
        return []

    def is_dependent(self):
        return False

    def convert_related_ids(self, id_mappings):
        """
        Used in LinkMapper only. 
        Takes a dictionary where keys are raw IDs (returned by to_key()) and values are the real
        IDs (integers) as used in the database. This method can be overridden by subclasses
        if there's a need to convert some field values before saving them.
        """
        pass


class MessageMapper(BaseMapper):

    def __init__(self, message):
        self.message = message

    def get_table_name(self):
        return self.message.class_name.lower()

    def get_related_entities(self):
        return []

    def to_dict(self):
        return {
            'log_timestamp': self.message.log_timestamp,
            'log_process': self.message.log_process,
            'log_level': self.message.log_level,
            'log_event': self.message.log_event,
            'log_file': self.message.log_file,
        }

    def get_table_schema(self):
        return [
            ('sql_id', 'integer primary key autoincrement'),
            ('log_timestamp', 'text'),
            ('log_process', 'text'),
            ('log_level', 'text'),
            ('log_event', 'text'),
            ('log_file', 'text'),
        ]


class LinkMapper(BaseMapper):

    def __init__(self, data):
        self.data = data

    def to_dict(self):
        return self.data

    def get_table_name(self):
        return 'message_links'

    def get_table_schema(self):
        return [('message_type', 'text'), ('message_id', 'integer'),
                ('relation_type', 'text'), ('relation_id', 'integer')]

    def get_related_entities(self):
        return []

    def is_dependent(self):
        return True

    def convert_related_ids(self, id_mappings):
        self.data['relation_id'] = id_mappings.get(self.data['relation_id'],
                                                   self.data['relation_id'])
        return self.to_dict()


class HashableMapper(BaseMapper):

    def __init__(self, data, table_name):
        self.data = data
        self.table_name = table_name

    # def to_key(self):
    #     return "_".join(self.data.values())

    def to_dict(self):
        return self.data

    def get_table_name(self):
        return self.table_name

    def get_table_schema(self):
        schema = [('id', 'integer primary key')]
        for key in self.data.keys():
            schema.append((key, 'text'))
        return schema

    def get_related_entities(self):
        return []


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
        relations = Relations(self, self.message.hashes, 'hashes', 'hash')
        return relations.get_mappers()

    @property
    def parent_entity_name(self):
        return 'confirmackmessage'


class ConfirmReqMessageMapper(NetworkMessageMapper):

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
        relations = Relations(self, self.message.roots, 'roots')
        return relations.get_mappers()

    @property
    def parent_entity_name(self):
        return 'confirmreqmessage'


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


class BlockProcessedMessageMapper(MessageMapper):

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


class ProcessedBlocksMessageMapper(MessageMapper):

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


class BlocksInQueueMessageMapper(MessageMapper):

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


class NodeProcessConfirmedMessageMapper(MessageMapper):

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
            'work': self.message.work,
            'sideband':
            json.dumps(self.message.sideband)  # save as json string
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
                                             ('work', 'text'),
                                             ('sideband', 'jsonb')]


class ActiveStartedMessageMapper(MessageMapper):

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


class ActiveStoppedMessageMapper(MessageMapper):

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
        relations = Relations(self, self.message.hashes, 'hashes', 'hash')
        return relations.get_mappers()

    @property
    def parent_entity_name(self):
        return 'activestoppedmessage'


class BroadcastMessageMapper(MessageMapper):

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


class GenerateVoteNormalMessageMapper(MessageMapper):

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


class GenerateVoteFinalMessageMapper(MessageMapper):

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


class UnknownMessageMapper(MessageMapper):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'content': self.message.content,
        })
        return data

    def get_table_schema(self):
        return super().get_table_schema() + [('content', 'text')]


class FlushMessageMapper(MessageMapper):

    @property
    def parent_entity_name(self):
        return 'flushmessage'

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
        relations = Relations(self, self.message.confirm_req.roots, 'roots')
        return relations.get_mappers()
