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

    @abstractmethod
    def to_key(self):
        pass

    @abstractmethod
    def get_related_entities(self):
        return []

    @abstractmethod
    def is_dependent(self):
        pass

    @abstractmethod
    def convert_related_ids(self, id_mappings):
        pass

    @abstractmethod
    def handle_table(self):
        pass

    @abstractmethod
    def get_unique_constraints(self):
        pass

    @abstractmethod
    def get_indices(self):
        pass
