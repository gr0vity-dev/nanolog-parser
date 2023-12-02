from nanolog_parser.src.formatters.base import IFormatter, FilenameAdjusterMixin, MessageCreatorMixin
import json


class JsonFormatter(IFormatter, FilenameAdjusterMixin, MessageCreatorMixin):
    def format(self, line, filename, remove_prefix=""):
        adjusted_filename = self.adjust_filename(filename, remove_prefix)
        try:
            log_entry = json.loads(line)
            log_message = log_entry["log"].strip()
            return self.create_message(log_message, adjusted_filename)
        except json.JSONDecodeError:
            print(f"Failed to decode JSON from line: {line}")
            self.ignored_lines += 1
            return None
    
    def get(self, line, filename, remove_prefix=""):
        adjusted_filename = self.adjust_filename(filename, remove_prefix)
        try:
            log_entry = json.loads(line)
            log_message = log_entry["log"].strip()
            return self.message_get_json(log_message, adjusted_filename)
        except json.JSONDecodeError:
            print(f"Failed to decode JSON from line: {line}")
            self.ignored_lines += 1
            return None
