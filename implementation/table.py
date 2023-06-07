from collections import deque
from lstore.index import Index
from lstore.page import Page
from lstore.lock_manager import LockManager
from lstore.bufferpool import Bufferpool
from lstore.record import Record
from lstore.config import (
    NUM_METADATA_COLS,
    INDIRECTION_NULL,
    INDIRECTION_COLUMN,
    RID_COLUMN,
    COLUMN_SIZE,
    MAX_BASE_PAGES,
    SCHEMA_ENCODING_COLUMN,
    DATA_COL_START,
    PHYSICAL_PAGE_METADATA_SIZE,
    TPS_START,
    TPS_END,
)
import threading


class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key, bufferpool):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.total_num_columns = num_columns + NUM_METADATA_COLS
        self.page_directory = {}
        self.num_page_ranges = 0
        self.page_ranges_metadata = []
        self.index = Index(self)
        self.num_records = 0
        self.bufferpool = bufferpool
        self.merge_queue = deque()
        self.lock_manager = LockManager()
        self.num_transactions = 0

        # Locks
        self.num_records_lock = threading.Lock()
        self.page_directory_lock = threading.Lock()
        self.num_transactions_lock = threading.Lock()
        self.num_page_ranges_lock = threading.Lock()
        self.merge_queue_lock = threading.Lock()

    # Getter and setter for self.num_records
    # "p": (pre)read then increment
    # "w": write (increment) then read
    # "r": read
    def use_num_records(self, operation):
        self.num_records_lock.acquire()
        res = -1
        if operation == "p":
            res = self.num_records
            self.num_records += 1
        elif operation == "w":
            self.num_records += 1
            res = self.num_records
        elif operation == "r":
            res = self.num_records
        self.num_records_lock.release()
        return res

    # Getter and setter for self.num_transactions
    def use_num_transactions(self, operation):
        self.num_transactions_lock.acquire()
        res = -1

        if operation == "w":
            self.num_transactions += 1
            res = self.num_transactions
        elif operation == "r":
            res = self.num_transactions
        self.num_transactions_lock.release()
        return res

    # Getter and setter for self.page_directory
    def use_page_directory(self, operation, key, value):
        self.page_directory_lock.acquire()
        res = -1

        if operation == "w":
            self.page_directory[key] = value
            res = value
        elif operation == "r":
            res = self.page_directory.get(key, -1)
        elif operation == "d":
            self.page_directory.pop(key)
            res = True

        self.page_directory_lock.release()
        return res

    def use_num_page_ranges(self, operation):
        self.num_page_ranges_lock.acquire()

        res = -1
        if operation == "w":
            self.num_page_ranges += 1
            res = self.num_page_ranges
        elif operation == "r":
            res = self.num_page_ranges

        self.num_page_ranges_lock.release()
        return res

    def add_to_merge_queue(self, value):
        self.merge_queue_lock.acquire()
        self.merge_queue.append(value)
        self.merge_queue_lock.release()

    def merge(self):
        # Background thread that will continuously execute the merge
        # Will keep waiting until there is something in the merge queue
        # self.merge_queue will hold tuples: (page_range_num, base_page_num)
        while len(self.merge_queue) > 0:
            page_range_num, base_page_num = self.merge_queue[0]
            self.__merge_helper(page_range_num, base_page_num)
            self.merge_queue.popleft()

    def __merge_helper(self, page_range_num, base_page_num):
        # Get current base page of record being updated
        # Page will be in main memory instead of the bufferpool
        original_base_page = self.bufferpool.get_page_no_add(
            self.name, page_range_num, base_page_num
        )
        self.bufferpool.pin_page(original_base_page)

        # Create a copy of the base page
        base_page = Page(0)
        base_page.columns = original_base_page.create_copy()

        # Get the rids of all the base records in the base_page
        rids = base_page.get_rids_in_page()

        # Initialize all RIDs to not updated status
        # Maps RID : boolean to make it fast to check if a RID has already been updated
        # in order to know if it can ignore older updates
        updated_rids = {}
        for r in rids:
            updated_rids[r] = False
        num_updates = 0
        max_tid = 0

        # Get all relevant tail pages (all tail pages that are full)
        max_tp_num = self.bufferpool.get_highest_tail_page_num(
            self.name, page_range_num
        )
        if self.bufferpool.page_has_capacity(self.name, page_range_num, max_tp_num):
            max_tp_num -= 1
            if max_tp_num == MAX_BASE_PAGES - 1:
                return

        # Loop from most recently full tail page to least recently full tail page
        current_tp_num = max_tp_num
        while current_tp_num >= MAX_BASE_PAGES:
            tail_page = self.bufferpool.get_page_no_add(
                self.name, page_range_num, max_tp_num
            )
            tids = tail_page.get_rids_in_page()

            max_tid = max(tids[0], max_tid)

            # The smallest TID will be the first TID in the array since TIDs are in
            # increasing order. If smallest TID is less than the TPS, stop merging
            if tids[0] < base_page.columns[RID_COLUMN].tps:
                break

            # Iterate backwards since the latest updates are at the end of the array
            for i in range(len(tids) - 1, -1, -1):
                max_tid = max(tids[i], max_tid)

                # Get the primary key of the ith entry
                primary_key = tail_page.columns[self.key + NUM_METADATA_COLS].read_val(
                    i
                )
                base_rid = self.index.locate(self.key + NUM_METADATA_COLS, primary_key)[
                    0
                ]

                # If the corresponding base record is not in the base page being merged
                # or the base record is already updated, continue looping
                if (
                    updated_rids.get(base_rid, -1) == -1
                    or updated_rids[base_rid] == True
                ):
                    continue

                # Otherwise the latest update to the base record has been found
                # Get the offset of the base record and
                # copy the latest tail record's data values to the base record
                pr_num, page_num, offset = self.use_page_directory("r", base_rid, None)
                for j in range(DATA_COL_START, len(tail_page.columns)):
                    latest_col_val = tail_page.columns[j].read_val(offset)
                    base_page.columns[j].change_val(latest_col_val, offset)
                num_updates += 1

            # Stop merging if everything has been updated
            if num_updates == base_page.columns[0].num_records:
                break

            current_tp_num -= 1

        # Update TPS to be the greatest TID
        if max_tid > base_page.columns[RID_COLUMN].tps:
            phys_page = base_page.columns[RID_COLUMN]
            phys_page.data[TPS_START:TPS_END] = max_tid.to_bytes(COLUMN_SIZE, "big")

            base_page.columns[RID_COLUMN].tps = max_tid

        # Write the merged base page to disk
        self.bufferpool.write_page_to_disk(
            self.name, page_range_num, base_page_num, base_page
        )

        self.bufferpool.unpin_page(original_base_page)

    # Returns a Record object containing the latest record
    # given an RID of a record in a base page
    def get_latest_record(self, rid):
        return self.get_record_version(rid, 0)

    # Returns a Record object in the corresponding version
    # given an RID of a record in a base page
    def get_record_version(self, rid, version):
        # Check if the RID exists
        if self.page_directory.get(rid, -1) == -1:
            return None

        # Check if the RID is of a record in the base page
        base_page_range_num, base_page_num, base_offset = self.page_directory[rid]
        if base_page_num >= MAX_BASE_PAGES:
            return None

        # If indirection is less than TPS, return the record in the base page
        indirection = self.bufferpool.get_record_column_val(
            self.name, rid, INDIRECTION_COLUMN
        )
        tps = self.bufferpool.get_tps(self.name, base_page_range_num, base_page_num)
        if indirection <= tps:
            return self.bufferpool.get_record(self.name, rid)

        # Starts at record in base page (version 0 aka latest record
        # is not in base page if it has been updated)
        # Loops until desired version is found
        while version <= 0:
            # Get the current record's indirection value
            indirection = self.bufferpool.get_record_column_val(
                self.name, rid, INDIRECTION_COLUMN
            )

            # If indirection is null, the first update of the base page has been reached
            # Return the base record
            if indirection == INDIRECTION_NULL:
                break

            # Follow the indirection to the next record
            rid = indirection
            version += 1

        return self.bufferpool.get_record(self.name, rid)

    def delete_record(self, rid):
        # Check if the RID exists
        if self.use_page_directory("r", rid, None) == -1:
            return None

        # Check if the RID is of a record in the base page
        page_range_num, page_num, offset = self.use_page_directory("r", rid, None)
        rid_page = self.bufferpool.get_physical_page(
            self.name, page_range_num, page_num, RID_COLUMN
        )
        rid_page.pin_count += 1

        # Accesses the RID of the record and sets it to INDIRECTION_NULL
        rid_page.change_val(INDIRECTION_NULL, offset)
        # rid_page.data[
        #     COLUMN_SIZE * offset
        #     + PHYSICAL_PAGE_METADATA_SIZE : COLUMN_SIZE * (offset + 1)
        #     + PHYSICAL_PAGE_METADATA_SIZE
        # ] = INDIRECTION_NULL.to_bytes(COLUMN_SIZE, "big")

        # Get the value in the indirection column
        indirection_val = self.bufferpool.get_record_column_val(
            self.name, rid, INDIRECTION_COLUMN
        )

        # Remove RID from page_directory
        self.use_page_directory("d", rid, None)

        rid_page.is_dirty = True
        rid_page.pin_count -= 1

        return indirection_val

    def change_indirection(self, tail_rid, base_rid):
        # Check if the base record RID exists
        if self.use_page_directory("r", base_rid, None) == -1:
            return False

        # Check if the RID is of a record in the base page
        page_range_num, page_num, offset = self.use_page_directory("r", base_rid, None)
        base_page_indirection = None
        if page_num < MAX_BASE_PAGES:
            base_page_indirection = self.bufferpool.get_physical_page(
                self.name, page_range_num, page_num, INDIRECTION_COLUMN
            )
            base_page_indirection.pin_count += 1
        else:
            return False

        base_page_indirection.change_val(tail_rid, offset)
        # base_page_indirection.data[
        #     COLUMN_SIZE * offset
        #     + PHYSICAL_PAGE_METADATA_SIZE : COLUMN_SIZE * (offset + 1)
        #     + PHYSICAL_PAGE_METADATA_SIZE
        # ] = tail_rid.to_bytes(COLUMN_SIZE, "big")

        base_page_indirection.is_dirty = True
        base_page_indirection.pin_count -= 1

        return True

    def update_schema_encoding(self, base_rid, old_schema, columns, updated_cols):
        if len(columns) == 0:
            return True

        # Check if the base record RID exists
        if self.use_page_directory("r", base_rid, None) == -1:
            return False

        # Check if the RID is of a record in the base page
        page_range_num, page_num, offset = self.use_page_directory("r", base_rid, None)
        base_page_schema_encoding = None
        if page_range_num < MAX_BASE_PAGES:
            base_page_schema_encoding = self.bufferpool.get_physical_page(
                self.name, page_range_num, page_num, SCHEMA_ENCODING_COLUMN
            )
            base_page_schema_encoding.pin_count += 1
        else:
            return False

        # Change the corresponding bit to a 1 if a column is updated
        new_schema = 0
        for i in range(self.num_columns):
            new_schema <<= 1
            if columns[i] != None and columns[i] != updated_cols[i]:
                new_schema |= 1

        # Bitwise or with the old schema to include the previous columns that were updated
        new_schema |= old_schema

        base_page_schema_encoding.change_val(new_schema, offset)
        # base_page_schema_encoding.data[
        #     COLUMN_SIZE * offset
        #     + PHYSICAL_PAGE_METADATA_SIZE : COLUMN_SIZE * (offset + 1)
        #     + PHYSICAL_PAGE_METADATA_SIZE
        # ] = new_schema.to_bytes(COLUMN_SIZE, "big")

        base_page_schema_encoding.is_dirty = True
        base_page_schema_encoding.pin_count -= 1

        return True

    # Returns the latest integer value of a column given the RID of a base record
    def get_latest_column_val(self, rid, column):
        return self.get_record_column_version(rid, 0, column)

    # Returns the integer value of the specified version of a column
    # given the RID of a base record
    # Same logic as get_record_version
    def get_record_column_version(self, rid, version, column):
        # Check if the RID exists
        if self.use_page_directory("r", rid, None) == -1:
            return None

        # Check if the RID is of a record in the base page
        base_page_range_num, base_page_num, base_offset = self.use_page_directory(
            "r", rid, None
        )
        if base_page_num >= MAX_BASE_PAGES:
            return None

        # If indirection is less than TPS, return the record in the base page
        indirection = self.bufferpool.get_record_column_val(
            self.name, rid, INDIRECTION_COLUMN
        )
        tps = self.bufferpool.get_tps(self.name, base_page_range_num, base_page_num)
        if indirection <= tps:
            return self.bufferpool.get_record_column_val(self.name, rid, column)

        # Starts at record in base page (version 0 aka latest record
        # is not in base page if it has been updated)
        # Loops until desired version is found
        while version <= 0:
            # Get the current record's indirection value
            indirection = self.bufferpool.get_record_column_val(
                self.name, rid, INDIRECTION_COLUMN
            )

            # If indirection is null, the first update of the base page has been reached
            # Return the base record's column
            if indirection == INDIRECTION_NULL:
                break

            # Follow the indirection to the next record
            rid = indirection
            version += 1

        return self.bufferpool.get_record_column_val(self.name, rid, column)

    # Scans through all base pages looking for all records of the given version
    # that have a given value in a given column
    def scan(self, search_key, column_num, version):
        records = []

        # Look through all page ranges
        for pr_num in range(self.use_num_page_ranges("r")):
            # Look through all base pages in a page range
            for bp_num in range(self.page_ranges_metadata[pr_num][1] + 1):
                base_page_rids = self.bufferpool.get_physical_page(
                    self.name, pr_num, bp_num, RID_COLUMN
                )
                base_page_rids.pin_count += 1

                # Check if the column matches the search key, if it does
                # get and add the entire record to the result
                rids = base_page_rids.data_to_int_array()
                for r in rids:
                    col_val = self.get_record_column_version(r, version, column_num)
                    if col_val == search_key:
                        records.append(self.get_record_version(r, version))

                base_page_rids.pin_count -= 1

        return records

    # Debug function
    def print_page_directory(self):
        for key in self.page_directory.keys():
            page_range, page_num, offset = self.page_directory[key]

            record = page_range.get_record(page_num, offset)

            print(f"indirection: {record.indirection}, RID: {record.rid}")
            print(f"page_num: {page_num}, offset: {offset}")
            print(f"columns: {record.columns}\n")