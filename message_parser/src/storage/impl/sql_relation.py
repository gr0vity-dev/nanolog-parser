from src.storage.impl.sql_mixins import MapperMixin
from src.storage.impl.sql_mapper_interface import IMapper
from src.storage.impl.sql_normalizer import SQLDataNormalizer


class SqlRelation:

    def __init__(self, parent_entity_name, message_id, data, table_name):
        self.hashable_mapper = HashableMapper(data, table_name)
        self.link_mapper = LinkMapper({
            'message_type': parent_entity_name,
            'message_id': message_id,
            'relation_type': table_name,
            'relation_id': self.hashable_mapper.to_key(),
        })

    def get_mappers(self):
        # Its important to retrun HashableMapper before LinkMapper
        # as the store_message needs hashable_mapper sql_id to exist
        # when creating the link_mapper sql entry
        return [self.hashable_mapper, self.link_mapper]


class SqlRelations:

    def __init__(self):
        self.relations = []
        self.normalizer = SQLDataNormalizer()

    def add_relations_from_data(self, message_mapper, entity_entries, entity):

        if not entity_entries:
            return

        entity_entries, table_name = self.normalizer.normalize_sql(
            entity, entity_entries)

        # Check if we're working with a singular entry (not a list). If so, wrap it in a list.
        if not isinstance(entity_entries, list):
            entity_entries = [entity_entries]

        # If the list contains strings, singularize the entity for a key.
        key_for_string = self.normalizer.singularize(
            entity) if isinstance(entity_entries[0], str) else None

        for entry in entity_entries:
            self.add_relation(message_mapper, entry,
                              table_name, key_for_string)

    def add_relation(self, message_mapper, data, table_name, key_for_string=None):

        if isinstance(data, dict):
            self.create_relation(message_mapper, data, table_name)
        elif isinstance(data, str) and key_for_string is not None:
            self.create_relation(
                message_mapper, {key_for_string: data}, table_name)
        else:
            raise TypeError(
                f"{type(data)} - Data list must contain either dictionaries or strings (with key_for_string provided)"
            )

    def create_relation(self, message_mapper, data, table_name):
        self.relations.append(
            SqlRelation(message_mapper.parent_entity_name,
                        message_mapper.sql_id, data, table_name))

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
        if self.data['relation_id'] in id_mappings:
            self.data['relation_id'] = id_mappings[self.data['relation_id']]
        return self.to_dict()

    def get_indices(self):
        # Create an index for each column
        return [(column, ) for column, _ in self.get_table_schema()]

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
        for key, _ in self.data.items():
            # if isinstance(value, int):
            #     schema.append((key, 'integer'))
            # elif isinstance(value, float):
            #     schema.append((key, 'real'))
            # else:  # default to text if it's not int or float
            schema.append((key, 'text'))
        return schema

    def get_unique_constraints(self):
        # Unique constraint for all columns, excluding 'id'
        keys = [column for column, _ in self.get_table_schema()
                if column != 'id']
        return keys
