from datetime import datetime
import os


class PatchFileHelper():
    _file = None

    PATCH_FILE_PATH_TEMPLATE = "patch-{num}-{timestamp}.sql"

    def __init__(self, order=0):
        now = datetime.now()
        self._file = open(self.PATCH_FILE_PATH_TEMPLATE.format(
            timestamp=datetime.date(now), num=order), "w")

    def write_line(self, string):
        self._file.write(string + os.linesep)

    def __del__(self):
        if self._file:
            self._file.close()
