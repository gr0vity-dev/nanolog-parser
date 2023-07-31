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

    @property
    def parent_entity_name(self):
        return self.__class__.__name__.replace("Mapper", "").lower()


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


class NetworkMessageMixin(MessageMixin):

    def to_dict(self):
        data = super().to_dict()
        data.update({
            'message_type': self.message.message_type,
            'network': self.message.network,
            'network_int': self.message.network_int,
            'version': self.message.version,
            'version_min': self.message.version_min,
            'version_max': self.message.version_max,
            'extensions': self.message.extensions,
            'action': "message_received"
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
            ('action', 'text'),
        ]


class MessageHeaderMixin(MessageMixin):

    def to_dict(self):
        data = super().to_dict()
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
            ('action', 'text'),
        ]
