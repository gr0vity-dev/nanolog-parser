import hashlib
from nanolog_parser.src.formatters.base import IFormatter
import json

class JSONFlattener(IFormatter):
    def __init__(self, max_depth=5, use_hash=True):
        self.max_depth = max_depth
        self.use_hash = use_hash
        self.sql_id_counters = {'root': 0}
        self.accumulated_children = {}
        self.mappings = []
        self.child_hash_to_sql_id = {}  # Maps child hashes to sql_id for unique children
    
    def format(self, json, filename):
        return self.flatten(json)
    
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

    def _flatten_dict(self, d, depth, parent_info, child_info):
        dict_type = child_info[0] if child_info else "root"
        self._increment_sql_id_counter(dict_type) if dict_type == "root" else None
        
        main_object = {"sql_id": self.sql_id_counters.get(dict_type, 1)}
        parent_info = self._update_parent_info(parent_info)
        

        for key, value in d.items():
            if isinstance(value, (dict, list)):
                flattened, _ = self.flatten(
                    value, depth + 1, child_info, (key, main_object["sql_id"])
                )
                self._accumulate_child(key, flattened, parent_info)                
            else:
                main_object[key] = value
       
        # if child_info:
        #     self.mappings.append({
        #             "main_type": parent_info[0],
        #             "main_sql_id": parent_info[1],
        #             "link_type": child_info[0],
        #             "link_sql_id": self.sql_id_counters.get(child_info[0],1)
        #         })
        
        return main_object, self.sql_id_counters

    def _flatten_list(self, lst, depth, parent_info, child_info):
        list_type = child_info[0] if child_info else "root"
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
        return parent_info or ("root", self.sql_id_counters["root"])

    def _accumulate_child(self, key, child, parent):
        if self.use_hash:                      
            child_hash = self._hash_dict(child)
            if child_hash not in self.child_hash_to_sql_id:
                # This is a new unique child, so store its hash and sql_id
                self.child_hash_to_sql_id[child_hash] = self.sql_id_counters.get(key, 1)
                self.accumulated_children.setdefault(key, []).append(child)
                self.mappings.append({
                    "main_type": parent[0],
                    "main_sql_id": parent[1],
                    "link_type": key,
                    "link_sql_id": self.sql_id_counters.get(key,1)
                })  
                self._increment_sql_id_counter(key)
            else:
                self.mappings.append({
                    "main_type": parent[0],
                    "main_sql_id": parent[1],
                    "link_type": key,
                    "link_sql_id": self.child_hash_to_sql_id[child_hash]
                })  
        else:
            self.accumulated_children.setdefault(key, []).append(child)

    def _hash_dict(self, d):
        if isinstance(d, dict):
            # Exclude 'sql_id' from the hash calculation
            d_filtered = {k: v for k, v in d.items() if k != 'sql_id'}
            d_string = str(sorted(d_filtered.items()))
        elif isinstance(d, list):
            # Handle list: sort the list to create a consistent representation
            d_string = str(sorted(d))
        else:
            # For other types, use the string representation
            d_string = str(d)

        return hashlib.md5(d_string.encode()).hexdigest()