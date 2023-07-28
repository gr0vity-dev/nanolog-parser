from src.storage.impl.sql_mixins import MapperMixin
from src.storage.impl.sql_mapper_interface import IMapper


class SqlRelation:

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


class SqlRelations:

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
                    SqlRelation(self.message_mapper.parent_entity_name,
                                self.message_mapper.sql_id, data, table_name))

            # If the data is a string, we convert it to a dictionary before creating a SqlRelation object
            # The key for this dictionary is provided by the key_for_string argument.
            # We do this because the SqlRelation object expects its data argument to be a dictionary, not a string.
            elif isinstance(data, str) and key_for_string is not None:
                self.relations.append(
                    SqlRelation(self.message_mapper.parent_entity_name,
                                self.message_mapper.sql_id,
                                {key_for_string: data}, table_name))
            else:
                raise TypeError(
                    "Data list must contain either dictionaries or strings (with key_for_string provided)"
                )

    # This function simply collates all the mappers from all the SqlRelation objects
    def get_mappers(self):
        return [
            mapper for relation in self.relations
            for mapper in relation.get_mappers()
        ]


class LinkMapper(MapperMixin, IMapper):

    def __init__(self, data):
        self.data = data

    def to_dict(self):
        return self.data

    def get_table_name(self):
        return 'message_links'

    def get_table_schema(self):
        return [('message_type', 'text'), ('message_id', 'integer'),
                ('relation_type', 'text'), ('relation_id', 'integer')]

    def is_dependent(self):
        return True

    def convert_related_ids(self, id_mappings):
        self.data['relation_id'] = id_mappings[self.data['relation_id']]
        return self.to_dict()


# HashableMapper
class HashableMapper(MapperMixin, IMapper):

    def __init__(self, data, table_name):
        self.data = data
        self.table_name = table_name

    def to_dict(self):
        return self.data

    def get_table_name(self):
        return self.table_name

    def get_table_schema(self):
        schema = [('id', 'integer primary key')]
        for key in self.data.keys():
            schema.append((key, 'text'))
        return schema
