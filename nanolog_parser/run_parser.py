import glob
import time
from nanolog_parser.src.parser import Parser
from nanolog_parser.src.storage.impl.sqlite_storage import SQLiteStorage

prefix = "cpu0-2_low_new_"
do_store = True


def print_execution_time(start_time, task_name, message=''):
    duration = time.time() - start_time
    print(f"{task_name} took {duration:.2f} seconds {message}")


def parse_file(file_name, prefix):
    print(f"Start parsing {file_name}")
    parser = Parser()
    start_time = time.time()
    parser.load_and_parse_file(file_name, remove_prefix=prefix)
    print_execution_time(start_time, "Parsing")
    print(parser.report())
    return parser.parsed_messages


def store_messages(messages, storage):
    fail_counter = 0
    store_counter = 0
    start_time = time.time()

    for message in messages:
        try:
            storage.store_message(message)
            store_counter += 1
        except Exception as exc:
            fail_counter += 1
            print(
                f"{store_counter} failed to store {message.__class__} dict {message.__dict__}")
            raise exc

    message = f"(Failed {fail_counter} || Success {store_counter})"
    print_execution_time(start_time, "Storing", message=message)

    return fail_counter, store_counter


def process_log_files(prefix):
    storage = SQLiteStorage(f'{prefix}parsed_messages.db')
    total_fail_counter = 0
    total_store_counter = 0

    for log_file in glob.glob('*.log'):
        if log_file.startswith(prefix):
            messages = parse_file(log_file, prefix)

            if do_store:
                fail_counter, store_counter = store_messages(messages, storage)
                total_fail_counter += fail_counter
                total_store_counter += store_counter

    # This will only print if there were any failures or stored messages at all
    if do_store:
        print(
            f"Total Failures: {total_fail_counter}, Total Stored: {total_store_counter}")


if __name__ == "__main__":
    process_log_files(prefix)
