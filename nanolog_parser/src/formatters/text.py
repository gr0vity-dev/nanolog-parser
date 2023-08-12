from nanolog_parser.src.formatters.base import IFormatter, FilenameAdjusterMixin, MessageCreatorMixin


class TextFormatter(IFormatter, FilenameAdjusterMixin, MessageCreatorMixin):
    def format(self, line, filename, remove_prefix=""):
        adjusted_filename = self.adjust_filename(filename, remove_prefix)
        return self.create_message(line, adjusted_filename)
