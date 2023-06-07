from concurrent.futures import ThreadPoolExecutor
from lstore.table import Table
from lstore.record import Record
from lstore.index import Index
from lstore.page import Page, PhysicalPage, PageRange
from lstore.config import (
    DATA_COL_START,
    INDIRECTION_NULL,
    INDIRECTION_COLUMN,
    MAX_BASE_PAGES,
    SCHEMA_ENCODING_COLUMN,
    MERGE_CONDITION,
    CREATE_INDICES_CONDITION,
    TIMESTAMP_COLUMN,
)
import threading


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        self.insert_lock = threading.Lock()
        self.update_lock = threading.Lock()
        self.create_updated_record_lock = threading.Lock()
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key):
        # Indices for primary key should exist already
        if not self.table.index.is_indexed(DATA_COL_START + self.table.key):
            self.table.index.create_index(DATA_COL_START + self.table.key)

        # Check if the RID exists
        # Get the RID of the base page with the primary key
        rid = self.table.index.locate(DATA_COL_START + self.table.key, primary_key)
        if rid == -1:
            return False
        rid = rid[0]

        # Save the latest record for deleting the index on it later
        latest_record = self.table.get_latest_record(rid)

        # First RID is of a record in base page
        indirection_val = self.table.delete_record(rid)

        # num_records in Table class can't be decremented or else
        # future records added/updated will potentially have common RIDs

        # Loops until the record's indirection_column has no value or if current record is base record
        while indirection_val != INDIRECTION_NULL and indirection_val != rid:
            # The indirection_val are RIDs of records in tail page
            indirection_val = self.table.delete_record(indirection_val)

        self.table.index.delete_index(latest_record.to_array(), rid)
        return True

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        self.insert_lock.acquire()

        # Check if the record to insert has a non-unique primary key
        if self.table.index.is_indexed(DATA_COL_START + self.table.key):
            inserted_primary_key = columns[self.table.key]

            # Check if primary key already exists, return False if it does
            rids = self.table.index.locate(
                DATA_COL_START + self.table.key, inserted_primary_key
            )

            if rids != -1:
                self.insert_lock.release()
                return False

        num_records = 0
        if len(columns) > self.table.num_columns:
            columns = list(columns)
            num_records = columns.pop()
        else:
            num_records = self.table.use_num_records("p")

        schema_encoding = "0" * self.table.total_num_columns
        record = Record(num_records, columns[0], columns)
        page_range_num = self.table.use_num_page_ranges("r")
        bp_num = 0
        offset = 0

        # Initialize page directory and index and create a page range and base page
        if page_range_num == 0:
            page_range_num = self.table.bufferpool.create_page_range(self.table.name)
            self.table.bufferpool.insert_base_page(self.table.name, page_range_num)
            self.table.use_page_directory("w", record.rid, (page_range_num, 0, 0))

            self.table.index.create_index(DATA_COL_START + self.table.key)
        else:
            # Check if latest page range has capacity
            page_range_num = self.table.use_num_page_ranges("r") - 1

            # Make a new page range if it doesn't have capacity
            if not self.table.bufferpool.page_range_has_capacity(
                self.table.name, page_range_num
            ):
                page_range_num = self.table.bufferpool.create_page_range(
                    self.table.name
                )
                self.table.bufferpool.insert_base_page(self.table.name, page_range_num)

            # Check if latest base page in the latest page range has capacity
            bp_num = self.table.bufferpool.get_highest_base_page_num(
                self.table.name, page_range_num
            )
            phys_page = self.table.bufferpool.get_physical_page(
                self.table.name, page_range_num, bp_num, 0
            )

            if not self.table.bufferpool.page_has_capacity(
                self.table.name, page_range_num, bp_num
            ):
                self.table.bufferpool.insert_base_page(self.table.name, page_range_num)
                bp_num += 1

        base_page = self.table.bufferpool.get_page(
            self.table.name, page_range_num, bp_num
        )
        self.table.bufferpool.pin_page(base_page)
        offset = base_page.columns[0].num_records
        for p in base_page.columns:
            p.num_records += 1

        self.insert_lock.release()

        # Insertion
        base_page.write(record, offset)

        # Update page_directory
        self.table.use_page_directory(
            "w",
            record.rid,
            (page_range_num, bp_num, offset),
        )

        # Update index
        for i, col in enumerate(columns):
            if self.table.index.is_indexed(i + DATA_COL_START):
                # Create a new mapping from a data value to the base record's RID
                self.table.index.add(i + DATA_COL_START, col, record.rid)

        # self.table.use_num_records("w")
        self.table.bufferpool.unpin_page(base_page)
        return True

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, search_key, search_key_index, projected_columns_index):
        # Calls select_version with latest version 0
        return self.select_version(
            search_key, search_key_index, projected_columns_index, 0
        )

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select_version(
        self, search_key, search_key_index, projected_columns_index, relative_version
    ):
        # Create indices when a threshold for number of records is reached
        if self.table.index.indices[search_key_index + DATA_COL_START] == None:
            if self.table.num_records % CREATE_INDICES_CONDITION == 0:
                self.table.index.create_index(search_key_index + DATA_COL_START)
            else:
                # Scan through base records if there is no index
                records = self.table.scan(
                    search_key, search_key_index + DATA_COL_START, relative_version
                )

                res = []
                for r in records:
                    column_return = []
                    for i, num in enumerate(projected_columns_index):
                        if num == 1:
                            column_return.append(r.columns[i])
                    r.columns = column_return
                    res.append(r)
                return res

        rids = self.table.index.indices[search_key_index + DATA_COL_START].get(
            search_key, -1
        )

        # If the key is not found, return an empty list
        if rids == -1:
            return []

        records = []

        for rid in rids:
            column_return = []  # Consists of the projected columns only
            record = self.table.get_record_version(rid, relative_version)

            # If record does not exist, ignore it
            if record == None:
                continue

            # projected_columns_index is an array of 1s and 0s
            for i, num in enumerate(projected_columns_index):
                if num == 1:
                    column_return.append(record.columns[i])

            record.columns = column_return
            records.append(record)

        return records

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        # Check if update doesn't actually update anything (columns is all None)
        num_none = 0
        for c in columns:
            if c == None:
                num_none += 1
        if num_none == len(columns):
            return True

        rids = self.table.index.locate(DATA_COL_START + self.table.key, primary_key)

        # Return True and don't do an update if the primary key does not exist
        if rids == -1:
            return True

        # Check if the update changes the primary key column to an existing primary key
        if columns[self.table.key] != None and columns[self.table.key] != primary_key:
            updated_primary_key = columns[self.table.key]

            # Check if primary key already exists, return False if it does
            rid = self.table.index.locate(
                DATA_COL_START + self.table.key, updated_primary_key
            )
            if rid != -1:
                return False
        rids = rids[0]

        prev_record = self.table.get_latest_record(rids)

        base_record_indirection = self.table.bufferpool.get_record_column_val(
            self.table.name, rids, INDIRECTION_COLUMN
        )

        # Create updated tail record's columns (updates are cumulative)
        updated_cols = prev_record.columns.copy()
        for i in range(len(columns)):
            if columns[i] != None:
                updated_cols[i] = columns[i]
        updated_record = Record(-1, primary_key, updated_cols)

        # Insert a copy of the record in the base page if it is being updated
        # for the first time
        if base_record_indirection == INDIRECTION_NULL:
            base_copy = Record(-1, primary_key, prev_record.columns)
            base_copy.indirection = rids
            self.__insert_to_tail_page(rids, [], base_copy)

            # Set the updated record to point to the copy of the base record in the
            # tail page
            updated_record.indirection = base_copy.rid
        else:
            updated_record.indirection = prev_record.rid

        self.__insert_to_tail_page(rids, prev_record.columns, updated_record)
        return True

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        return self.sum_version(start_range, end_range, aggregate_column_index, 0)

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum_version(
        self, start_range, end_range, aggregate_column_index, relative_version
    ):
        total = 0
        found_records = 0

        for key in range(start_range, end_range + 1):  # include end_range
            # Check if key exists
            rid = self.table.index.indices[DATA_COL_START + self.table.key].get(key, -1)
            if rid != -1:
                # Add column to total
                total += self.table.get_record_column_version(
                    rid[0], relative_version, aggregate_column_index + DATA_COL_START
                )
                found_records += 1

        if found_records == 0:
            return False
        return total

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

    # Helper function to add records to a tail page and perform
    # the necessary additional updates
    def __insert_to_tail_page(self, base_rid, columns, updated_record):
        self.update_lock.acquire()
        base_page_range_num, base_page_num, offset = self.table.use_page_directory(
            "r", base_rid, None
        )

        updated_record.rid = self.table.use_num_records("p")

        # Check if latest tail page has capacity, if it doesn't, make a new tail page
        tail_page_num = self.table.bufferpool.get_highest_tail_page_num(
            self.table.name, base_page_range_num
        )
        if (
            tail_page_num == MAX_BASE_PAGES - 1
            or not self.table.bufferpool.page_has_capacity(
                self.table.name, base_page_range_num, tail_page_num
            )
        ):
            self.table.bufferpool.insert_tail_page(self.table.name, base_page_range_num)
            tail_page_num += 1

        # Get the tail page from the bufferpool
        tail_page = self.table.bufferpool.get_page(
            self.table.name, base_page_range_num, tail_page_num
        )
        self.table.bufferpool.pin_page(tail_page)

        offset = tail_page.columns[0].num_records
        for p in tail_page.columns:
            p.num_records += 1

        num_updates = self.table.bufferpool.get_num_updates(
            self.table.name, base_page_range_num, base_page_num
        )
        self.table.bufferpool.set_num_updates(
            self.table.name, base_page_range_num, base_page_num, num_updates + 1
        )
        num_updates += 1

        self.update_lock.release()

        # Insert tail record
        tail_page.write(updated_record, offset)

        # Update page_directory
        self.table.use_page_directory(
            "w",
            updated_record.rid,
            [base_page_range_num, tail_page_num, offset],
        )

        # Update base record's indirection to point to new tail record
        self.table.change_indirection(updated_record.rid, base_rid)

        # Update base record's schema encoding column
        old_schema = self.table.bufferpool.get_record_column_val(
            self.table.name, base_rid, SCHEMA_ENCODING_COLUMN
        )

        self.table.update_schema_encoding(
            base_rid, old_schema, columns, updated_record.columns
        )

        # Update index
        for i, col in enumerate(columns):
            if col != None and self.table.index.is_indexed(i + DATA_COL_START):
                self.table.index.update_index(
                    i + DATA_COL_START, col, updated_record.columns[i], base_rid
                )

        self.table.bufferpool.unpin_page(tail_page)

        # Merge
        if num_updates > 0 and num_updates % MERGE_CONDITION == 0:
            # If the base page is full, perform the merge
            if not self.table.bufferpool.page_has_capacity(
                self.table.name, base_page_range_num, base_page_num
            ):
                self.table.add_to_merge_queue((base_page_range_num, base_page_num))
                pool = ThreadPoolExecutor(1)
                pool.submit(self.table.merge)

        return True

    """
    For testing purposes
    (uncomment to check schema in extra_test_cases.py)
    
    def get_base_record(self, search_key, search_key_index):
        if self.table.index.indices[search_key_index + DATA_COL_START] != None:
            rids = self.table.index.indices[search_key_index + DATA_COL_START].get(
            search_key, -1
        )
            # If the key is not found, return an empty list
            if rids == -1:
                return []
            rid = rids[0]
            record = self.table.get_record_version(rid, -INDIRECTION_NULL)
            return record
        else:
            records = self.table.scan(
                    search_key, search_key_index + DATA_COL_START, -INDIRECTION_NULL
                )
            res = []
            for r in records:
                res.append(r)
            if len(res) > 1:
                print("more than one base record of same rid??")
            return res[0]
    
    """