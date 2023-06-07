from lstore.config import INDIRECTION_NULL
from time import time


class Record:
    def __init__(self, rid, key, columns):
        self.indirection = INDIRECTION_NULL
        self.rid = rid
        self.timestamp = time()
        self.schema_encoding = bytearray(len(columns))
        self.key = key
        self.columns = columns

    # Returns all the metadata and data of a record as an array
    def to_array(self):
        return [
            self.indirection,
            self.rid,
            self.timestamp,
            self.schema_encoding,
            *self.columns,
        ]