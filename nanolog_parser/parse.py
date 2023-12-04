#!/usr/bin/env python3

import argparse
from nanolog_parser.src.formatters import IFormatter, JsonFormatter, TextFormatter
from nanolog_parser.src.storage.impl.sqlite_storage import SQLiteStorage
from nanolog_parser.src.formatters.jsondb import JSONFlattener
from sqlalchemy import create_engine,Table, MetaData, Index, text
import pandas as pd
import time
import json



def get_args():
    parser = argparse.ArgumentParser(description="NanoLog Parser")

    # Modify the argument here:
    parser.add_argument('--format', type=str, required=True,
                        choices=['json', 'nano_docker'], help="Specify the format: json or text.")
    parser.add_argument('--db', type=str, default="parsed_messages.db",
                        help="Path to the database where parsed messages will be stored.")    
    parser.add_argument('--file-node', nargs=2, action='append',
                        help="Pairs of file and node names", required=True)
    parser.add_argument('--inject-table', type=str,
                        help="Path to the json contaning additional tables")
    parser.add_argument('--inject-json', type=str,
                        help="Path to the json contaning additional tables")  
    
    return parser.parse_args()


class NanoLogParser:

    def __init__(self, formatter: IFormatter, storage: SQLiteStorage):
        self.formatter = formatter
        self.storage = storage
        self.message_count = 0

    def _modify_filename(self, filename, remove_prefix=""):
        return filename.replace(remove_prefix, "").replace("node.log", "").replace(".log", "")

    def process(self, line, filename):  # Add filename parameter
        modified_filename = self._modify_filename(filename)
        message = self.formatter.format(line, modified_filename)
        if message:
            self.storage.store_message(message)
            self._increment_and_display_progress()
    
    def process_flat(self, line, filename):  # Add filename parameter
        modified_filename = self._modify_filename(filename)
        message = self.formatter.get(line, modified_filename)
        if message and message.get("log_level") == "trace" :
            message.pop("content")
        self._increment_and_display_progress()
        return message
    
    def process_json(self, json_line):  # Add filename parameter       
        self._increment_and_display_progress()
        return json.loads(json_line)

    def _increment_and_display_progress(self):
        self.message_count += 1
        if self.message_count % 10000 == 0:
            print(f"Processed: {self.message_count} messages", end="\r")

def create_db_engine(connection_string):    
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        connection.execute(text("PRAGMA journal_mode=WAL;"))
    return engine

def write_to_db(engine, dataframes):
    for table_name, df in dataframes.items():
        # Convert all columns of the dataframe to string type
        df = df.astype(str)
        
        try:
            df.to_sql(table_name, engine, if_exists='replace', index=False)
        except Exception as e:
            print(f"Error writing table {table_name}: {e}")


def main():
    args = get_args()    
    

    formatter = JsonFormatter()      
    flattener = JSONFlattener()
    
    if args.format == "json":
        pass
    elif args.format == "nano_docker":         
        flattener.add_key_mappings({"block": "blocks", "vote": "votes", "hash" : "hashes", "winner": "blocks"})
        flattener.add_key_mappings({"time": "timestamp", "date": "timestamp"})
        flattener.add_key_mappings({"voting_account": "account"})
    else:
        print(f"Unsupported format: {args.format}")
        return
        
    start_time = time.time()
    json_lines = []
    log_parser = NanoLogParser(formatter, None)
    print(f"Storing messages in SQL database: {args.db}\n")

    file_read_time = 0
    for file_node_pair in args.file_node:
        file_name, node_name = file_node_pair
        file_start_time = time.time()
        with open(file_name, 'r', encoding='utf-8') as file:
            print(f"Processing {node_name}\n")
            for line in file:
                if args.format == "nano_docker":                        
                    json_lines.append(log_parser.process_flat(line.strip(), node_name))
                else:
                    json_lines.append(log_parser.process_json(line))
        file_read_time += time.time() - file_start_time

    print(f"File reading and processing time: {file_read_time:.2f} seconds")
    # Read and merge additional tables from JSON file if the argument is provided
    if args.inject_json:
        print(f"Injecting json {args.inject_json} \n")
        with open(args.inject_json, 'r') as json_file:
            for line in json_file:
                json_lines.append(log_parser.process_json(line))           
            
    json_to_tables_start_time = time.time()
    print(f"Converting to json tables \n")
    tables = flattener.to_json_tables(json_lines)
    
    # Read and merge additional tables from JSON file if the argument is provided
    if args.inject_table:
        print(tables.get("header"))
        print(f"Injecting table {args.inject_table} \n")
        with open(args.inject_table, 'r') as json_file:
            additional_tables = json.load(json_file)
            tables.update(additional_tables)

    print(f"Time taken to convert to JSON tables: {time.time() - json_to_tables_start_time:.2f} seconds")

    dataframes_start_time = time.time()
    print(f"Converting to dataframes\n")
    dataframes = {table_name: pd.DataFrame(rows) for table_name, rows in tables.items()}
    print(f"Time taken to convert to DataFrames: {time.time() - dataframes_start_time:.2f} seconds")

    db_write_start_time = time.time()
    connection_string = f"sqlite:///{args.db}"  # Or your PostgreSQL connection string
    engine = create_db_engine(connection_string)
    print(f"Writing to sqlite \n")
    write_to_db(engine, dataframes)
    print(f"Time taken to write to DB: {time.time() - db_write_start_time:.2f} seconds")

    total_time = time.time() - start_time
    print(f"Total execution time: {total_time:.2f} seconds")


    print(f"\nTotal messages processed: {log_parser.message_count}")
    print(f"Messages stored in SQL database: {args.db}")


if __name__ == "__main__":
    main()
