#!/usr/bin/env python3

import argparse
from src.formatters import IFormatter, JsonFormatter, TextFormatter
from src.storage.impl.sqlite_storage import SQLiteStorage
import json

mappings = []

def flatten_json(json_data, sql_id_counters=None, depth=0, max_depth=5, parent_info=None, child_info=None, accumulated_children=None):
    if sql_id_counters is None:
        sql_id_counters = {'root': 1}  # Initialize counter for the root
    if accumulated_children is None:
        accumulated_children = {}  # Initialize accumulator for children

    if depth > max_depth:
        return json_data, sql_id_counters

    if isinstance(json_data, dict):
        return flatten_dict(json_data, sql_id_counters, depth, max_depth, parent_info, child_info, accumulated_children)
    elif isinstance(json_data, list):
        return flatten_list(json_data, sql_id_counters, depth, max_depth, parent_info, child_info, accumulated_children)
    else:
        # Non-dictionary, non-list objects are returned as-is
        return json_data, sql_id_counters

def flatten_dict(d, sql_id_counters, depth, max_depth, parent_info, child_info, accumulated_children):
    dict_type = child_info[0] if child_info else "root"
    sql_id_counters[dict_type] = sql_id_counters.get(dict_type, 1)
    main_object = {"sql_id": sql_id_counters[dict_type]}
     
    
    if parent_info:       
        mappings.append({"main_type": parent_info[0], "main_sql_id": sql_id_counters[parent_info[0]], "link_type": child_info[0], "link_sql_id": sql_id_counters[child_info[0]]})
        parent_info = child_info
    else :
        parent_info = ("root", sql_id_counters["root"])     
    

    for key, value in d.items():
        if isinstance(value, (dict, list)):
            flattened, new_sql_id_counters = flatten_json(
                value, sql_id_counters, depth + 1, max_depth, parent_info, (key, main_object["sql_id"]), accumulated_children
            )
            sql_id_counters.update(new_sql_id_counters)
            accumulated_children.setdefault(key, []).extend(flattened if isinstance(flattened, list) else [flattened])
        else:
            main_object[key] = value

    sql_id_counters[dict_type] += 1
    return main_object, sql_id_counters

def flatten_list(lst, sql_id_counters, depth, max_depth, parent_info, child_info, accumulated_children):
    list_type = child_info[0] if child_info else "root"
    sql_id_counters[list_type] = sql_id_counters.get(list_type, 1)
    items = []

    for item in lst:
        if isinstance(item, (dict, list)):
            flattened, new_sql_id_counters = flatten_json(
                item, sql_id_counters, depth + 1, max_depth, parent_info, child_info, accumulated_children
            )
            items.append(flattened)

    return items, sql_id_counters


class NanoLogParser:

    def __init__(self, formatter: IFormatter, storage: SQLiteStorage):
        self.formatter = formatter
        self.storage = storage
        self.message_count = 0
        self.sql_id_counters = {}  # Initialize sql_id_counters
        self.accumulated_children = {}  # Initialize accumulator for child objects        

    def _modify_filename(self, filename, remove_prefix=""):
        return filename.replace(remove_prefix, "").replace("node.log", "").replace(".log", "")

    def process(self, line, filename):  # Add filename parameter
        modified_filename = self._modify_filename(filename)
        message = self.formatter.get(line, modified_filename)
        if message.get("log_level") == "trace" :
            message.pop("content")
        main_json, self.sql_id_counters= flatten_json(message, self.sql_id_counters, accumulated_children=self.accumulated_children)
        print("Main JSON:", main_json)
       
        # if message:
        #     self.storage.store_message(message)
        #     self._increment_and_display_progress()

    def _increment_and_display_progress(self):
        self.message_count += 1
        if self.message_count % 10000 == 0:
            print(f"Processed: {self.message_count} messages", end="\r")


def main():
    
    args_db = "out.db"
    args_file = "json.log"
    args_node = "pr1"
    formatter = JsonFormatter()
    

    with SQLiteStorage(args_db) as storage:
        log_parser = NanoLogParser(formatter, storage)
        print(f"Storing messages in SQL database: {args_db}\n")

        with open(args_file, 'r', encoding='utf-8') as file:
            for line in file:

                log_parser.process(line.strip(), args_node or args_file)
            print("Accumulated Child Objects:", log_parser.accumulated_children)
            print("Mappings:", mappings)

    print(f"\nTotal messages processed: {log_parser.message_count}")
    print(f"Messages stored in SQL database: {args_db}")


if __name__ == "__main__":
    main()
