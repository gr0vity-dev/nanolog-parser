#!/usr/bin/env python3

import argparse
from nanolog_parser.src.formatters import IFormatter, JsonFormatter, TextFormatter
from nanolog_parser.src.storage.impl.sqlite_storage import SQLiteStorage
from nanolog_parser.src.message_parsers.log_parser import LogParser
from nanolog_parser.src.message_parsers.log_parser import MessageToJsonConverter
import json


def get_args():
    parser = argparse.ArgumentParser(description="NanoLog Parser")

    # Modify the argument here:
    parser.add_argument('--format', type=str, required=True,
                        choices=['json', 'text'], help="Specify the format: json or text.")
    parser.add_argument('--output', type=str, default="json",
                        choices=['json', 'sql'], help="Specify the format: json or sql.")
    parser.add_argument('--db', type=str, default="parsed_messages.db",
                        help="Path to the database where parsed messages will be stored.")
    parser.add_argument('--file', type=str,
                        help="Name of the file being parsed.", required=True)
    parser.add_argument('--node', type=str,
                        help="Name of node. If provided, instead of 'file', 'node' is written to log_filename ")

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

    def _increment_and_display_progress(self):
        self.message_count += 1
        if self.message_count % 10000 == 0:
            print(f"Processed: {self.message_count} messages", end="\r")


def main():
    args = get_args()

    if args.format == "json":
        formatter = JsonFormatter()
    elif args.format == "text":
        formatter = TextFormatter()
    else:
        print(f"Unsupported format: {args.format}")
        return

    if args.output == "json":
        json_converter = MessageToJsonConverter()
        log_parser = LogParser(json_converter, None)

        with open(args.file, 'r', encoding='utf-8') as file, open("output_file.json", "w", encoding="utf-8") as out_file:
            out_file.write("[")

            first_line = True
            for line in file:
                parsed_json = log_parser.parse_to_json(
                    line.strip(), args.node or args.file)
                if not first_line:
                    out_file.write(",\n")
                else:
                    first_line = False
                json.dump(parsed_json, out_file)

            out_file.write("]")

    else:
        with SQLiteStorage(args.db) as storage:
            log_parser = NanoLogParser(formatter, storage)
            print(f"Storing messages in SQL database: {args.db}\n")

            with open(args.file, 'r', encoding='utf-8') as file:
                for line in file:
                    log_parser.process(line.strip(), args.node or args.file)

    print(f"\nTotal messages processed: {log_parser.message_count}")
    print(f"Messages stored in SQL database: {args.db}")


if __name__ == "__main__":
    main()
