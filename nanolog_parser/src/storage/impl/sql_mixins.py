

class MapperMixin:

    def get_table_name(self):
        return self.message.class_name.lower()

    def to_dict(self):
        return {}

    def to_key(self):
        return "_".join([str(x) for x in list(self.to_dict().values())])

    def is_dependent(self):
        return False

    def convert_related_ids(self, id_mappings):
        return self.to_dict()

    def handle_table(self):
        return self.get_table_name(), self.get_table_schema(), self.to_dict()

    def get_related_entities(self):
        return []

    def get_unique_constraints(self):
        return []

    def get_indices(self):
        return []


class MessageMixin(MapperMixin):

    def __init__(self, message):
        self.message = message

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

    @property
    def parent_entity_name(self):
        assert hasattr(self.message, 'class_name')
        return self.message.class_name.lower()


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
