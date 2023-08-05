import json
from src.storage.impl.sql_mixins import *
from src.storage.impl.sql_mapper_interface import IMapper
from src.storage.impl.sql_relation import SqlRelations


class DataResolverMixin:
    def _resolve_nested_key(self, key):
        keys = key.split('.')
        value = self.message
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                value = getattr(value, k, None)
            if value is None:
                return None
        return value

    def _resolve_all_keys(self, keys):
        return {k.replace(".", "_"): self._resolve_nested_key(k) for k in keys}


class SqlBaseMapperMixin(DataResolverMixin, MapperMixin):

    TYPE_MAPPING = {
        int: 'integer',
        float: 'real',
        str: 'text',
        bool: 'integer',
        bytes: 'blob',
        None: 'text'  # for handling None values, default to text
    }

    sql_columns = set()
    column_types = {}  # New dictionary to store explicit types for columns

    @classmethod  # Making this a classmethod so it can be used directly on the class
    def set_type(cls, column, column_type):
        cls.column_types[column] = column_type

    def to_dict(self):
        base_dict = super().to_dict()
        columns_dict = self._resolve_all_keys(self.sql_columns)

        # Convert columns specified as text to string
        for col, col_type in self.column_types.items():
            if col_type == str:  # if the column type is explicitly set as text
                col = col.replace(".", "_")
                if col in columns_dict:
                    columns_dict[col] = str(columns_dict[col])
            elif col not in self.column_types:  # if the column type is not explicitly set
                default_type = self.TYPE_MAPPING.get(
                    type(columns_dict.get(col, None)), 'text')
                if default_type == 'text':
                    columns_dict[col] = str(columns_dict[col])

        return {**base_dict, **columns_dict}

    def get_table_schema(self):
        base_schema = super().get_table_schema()

        # Extract the values for each column from the message
        column_values = self._resolve_all_keys(self.sql_columns)

        # Create the schema entries using the type of each value
        columns_schema = [(col.replace(".", "_"), self.TYPE_MAPPING.get(self.column_types.get(col, type(column_values.get(col, None))), 'text'))
                          for col in self.sql_columns]

        return base_schema + columns_schema


class RelationsMixin(DataResolverMixin):
    sql_relation = set()

    def _get_entity_name(self, rel):
        return rel.split('.')[-1]

    def get_related_entities(self):
        relations = SqlRelations()
        for rel in self.sql_relation:
            nested_data = self._resolve_nested_key(rel)
            entity = self._get_entity_name(rel)
            relations.add_relations_from_data(self, nested_data, entity)
        return relations.get_mappers()


class MessageMapper(MessageMixin, IMapper):
    pass


class BlockProcessedMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin):
    sql_columns = {"result", "hash", "forced"}
    sql_relation = {"block"}


class ProcessedBlocksMessageMapper(SqlBaseMapperMixin, MessageMixin):
    sql_columns = {"processed_blocks", "forced_blocks", "process_time"}


class BlocksInQueueMessageMapper(SqlBaseMapperMixin, MessageMixin):
    sql_columns = {"blocks_in_queue", "state_blocks", "forced_blocks"}


class ActiveStartedMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin):
    sql_columns = {'root', 'behaviour', 'state', 'confirmed',
                   'winner', 'tally_amount', 'final_tally_amount', }
    sql_relation = {"election.blocks", "election.votes", "election.tally"}


class ActiveStoppedMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin):
    sql_columns = {'root', 'behaviour', 'state', 'confirmed',
                   'winner', 'tally_amount', 'final_tally_amount', }
    sql_relation = {"election.blocks", "election.votes", "election.tally"}


class BroadcastMessageMapper(SqlBaseMapperMixin, MessageMixin):
    sql_columns = {'channel', 'hash'}


class ChannelConfirmAckMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin):
    sql_columns = {"vote_type", "vote_count"}
    sql_relation = {"message.header", "message.vote", "channel"}


class ElectionGenerateVoteNormalMessageMapper(SqlBaseMapperMixin, MessageMixin):
    sql_columns = {"root", "hash"}


class ElectionGenerateVoteFinalMessageMapper(SqlBaseMapperMixin, MessageMixin):
    sql_columns = {"root", "hash"}


class UnknownMessageMapper(SqlBaseMapperMixin, MessageMixin):
    sql_columns = {"content"}


class ChannelMessageMapper():
    pass


class NetworkMessageMapper():
    pass


class ConfirmAckMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin):
    sql_columns = {"vote_count"}
    sql_relation = {"message.header", "message.vote"}


class PublishMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin):
    sql_relation = {"message.header", "message.block"}


class KeepAliveMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin):
    sql_relation = {"message.header", "message.peers"}


class ASCPullAckMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin):
    SqlBaseMapperMixin.set_type("message.id", str)
    sql_columns = {"message.id", "message.type"}
    sql_relation = {"message.header", "message.blocks"}


class ASCPullReqMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin):
    SqlBaseMapperMixin.set_type("message.id", str)
    sql_columns = {"message.id", "message.start_type",
                   "message.start", "message.count"}
    sql_relation = {"message.header"}


class ConfirmReqMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin):
    sql_columns = {"root_count"}
    sql_relation = {"message.header", "message.roots"}


class FlushMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin):
    sql_columns = {"root_count", "channel"}
    sql_relation = {"confirm_req.header",
                    "confirm_req.roots", "confirm_req.block"}


class ProcessConfirmedMessageMapper(RelationsMixin, SqlBaseMapperMixin, MessageMixin):
    sql_relation = {"block"}
