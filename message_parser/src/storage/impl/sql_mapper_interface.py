from abc import ABC, abstractmethod


class IMapper(ABC):

    @abstractmethod
    def get_table_name(self):
        pass

    @abstractmethod
    def get_table_schema(self):
        pass

    @abstractmethod
    def to_dict(self):
        pass

    def to_key(self):
        return "_".join(list(self.to_dict().values()))

    @abstractmethod
    def get_related_entities(self):
        return []

    def is_dependent(self):
        return False

    def convert_related_ids(self, id_mappings):
        pass

    def handle_table(self):
        return self.get_table_name(), self.get_table_schema(), self.to_dict()
