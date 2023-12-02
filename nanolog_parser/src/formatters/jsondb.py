import hashlib
from nanolog_parser.src.formatters.base import IFormatter
import json

class JSONFlattener(IFormatter):
    def __init__(self, max_depth=5, root_name="log"):
        self.max_depth = max_depth
        self.root_name = root_name
        self.sql_id_counters = {root_name: 0}
        self.accumulated_children = {}
        self.mappings = []
        self.child_hash_to_sql_id = {}
        self.key_mappings = {}  # Stores user-defined key mappings
    
    def format(self, json, filename):
        return self.flatten(json)
    
    def to_json_tables(self, json_lines):
        results, accumulated_children, mappings = self.flatten_json(json_lines)
        accumulated_children[self.root_name] = results
        accumulated_children["mappings"] = mappings
        return self.accumulated_children
    
    def flatten_json(self, json_lines):
        results = []
        for line in json_lines:
            if not line : continue
            result, _ = self.flatten(line)
            results.append(result)
        return results, self.accumulated_children, self.mappings
    
    def flatten_lines(self, json_lines):
        results = []
        for line in json_lines.splitlines():
            json_data = json.loads(line)
            result, _ = self.flatten(json_data)
            results.append(result)
        return results, self.accumulated_children, self.mappings    

    def flatten(self, json_data, depth=0, parent_info=None, child_info=None):
        if depth > self.max_depth:
            return json_data, self.sql_id_counters

        if isinstance(json_data, dict):
            return self._flatten_dict(json_data, depth, parent_info, child_info)
        elif isinstance(json_data, list):
            return self._flatten_list(json_data, depth, parent_info, child_info)
        else:
            return json_data, self.sql_id_counters

    def add_key_mappings(self, mappings):
        """
        Add mappings for keys. This method allows the user to define that certain keys
        should be considered equivalent when processing JSON data.
        :param mappings: A dictionary of key mappings, where each key is mapped to its equivalent key.
        """
        self.key_mappings.update(mappings)

    def _get_mapped_key(self, key):
        """
        Returns the mapped key if it exists in the key_mappings, otherwise returns the original key.
        """
        return self.key_mappings.get(key, key)
    
    
    def _flatten_dict(self, d, depth, parent_info, child_info):
        dict_type = child_info[0] if child_info else self.root_name
        self._increment_sql_id_counter(dict_type) if dict_type == self.root_name else None
        
        main_object = {"sql_id": self.sql_id_counters.get(dict_type, 1)}
        parent_info = self._update_parent_info(parent_info)
        

        for lookup_key, value in d.items():
            key = self._get_mapped_key(lookup_key)
            if isinstance(value, (dict, list)):
                flattened, _ = self.flatten(
                    value, depth + 1, child_info, (key, main_object["sql_id"])
                )
                self._accumulate_children(key, flattened, parent_info)                
            else:
                main_object[key] = value
        
        return main_object, self.sql_id_counters

    def _flatten_list(self, lst, depth, parent_info, child_info):
        list_type = child_info[0] if child_info else self.root_name
        items = []
        for item in lst:
            if isinstance(item, (dict, list)):
                flattened, _ = self.flatten(item, depth + 1, parent_info, child_info)
                items.append(flattened)

        return items, self.sql_id_counters

    def _increment_sql_id_counter(self, type_name):
        self.sql_id_counters[type_name] = self.sql_id_counters.get(type_name, 1) + 1
    
    def _decrement_sql_id_counter(self, type_name):
        self.sql_id_counters[type_name] = self.sql_id_counters.get(type_name, 1) - 1

    def _update_parent_info(self, parent_info):            
        return parent_info or (self.root_name, self.sql_id_counters[self.root_name])

    def _accumulate_children(self, key, child, parent):        
        if isinstance(child, list):
            for item in child:
                self._process_child_item(key, item, parent)
        else:
            self._process_child_item(key, child, parent)

    def _process_child_item(self, key, item, parent):        
        item_hash = self._hash_dict(key,item)
        if item_hash not in self.child_hash_to_sql_id:
            # This is a new unique item, so store its hash and sql_id
            self.child_hash_to_sql_id[item_hash] = self.sql_id_counters.get(key, 1)
            self.accumulated_children.setdefault(key, []).append(item)
            link_sql_id = self.child_hash_to_sql_id[item_hash]
            self._add_mapping(parent, key, link_sql_id)
            self._increment_sql_id_counter(key)
        else:
            # For a duplicate item, use the existing sql_id
            link_sql_id = self.child_hash_to_sql_id[item_hash]
            self._add_mapping(parent, key, link_sql_id)

    def _add_mapping(self, parent, key, link_sql_id):
        mapping = {
            "main_type": parent[0],
            "main_sql_id": parent[1],
            "link_type": key,
            "link_sql_id": link_sql_id
        }
        self.mappings.append(mapping)

    def _hash_dict(self, key,d):
        assert(isinstance(d, dict))       
        # Exclude 'sql_id' from the hash calculation
        d_filtered = {k: v for k, v in d.items() if k != 'sql_id'}
        d_string = str(key) + str(sorted(d_filtered.items()))
      

        return hashlib.md5(d_string.encode()).hexdigest()