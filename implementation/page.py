from lstore.config import (
    PAGE_SIZE,
    COLUMN_SIZE,
    MAX_BASE_PAGES,
    DATA_COL_START,
    INDIRECTION_COLUMN,
    RID_COLUMN,
    TIMESTAMP_COLUMN,
    SCHEMA_ENCODING_COLUMN,
    PHYSICAL_PAGE_METADATA_SIZE,
    NUM_RECORDS_START,
    NUM_RECORDS_END,
    TPS_START,
    TPS_END,
    NUM_UPDATES_START,
    NUM_UPDATES_END,
)
from lstore.record import Record
import threading


class Page:
    def __init__(self, num_columns):
        self.columns = [PhysicalPage() for i in range(num_columns)]

    def write(self, record: Record, offset):
        max_offset = (PAGE_SIZE - PHYSICAL_PAGE_METADATA_SIZE) // COLUMN_SIZE - 1
        if offset <= max_offset:
            # Write metadata
            self.columns[INDIRECTION_COLUMN].write(record.indirection, offset)
            self.columns[RID_COLUMN].write(record.rid, offset)
            self.columns[TIMESTAMP_COLUMN].write(int(record.timestamp), offset)
            self.columns[SCHEMA_ENCODING_COLUMN].write(
                int.from_bytes(record.schema_encoding[0:], "big", signed=True), offset
            )

            # Write data
            for i, value in enumerate(record.columns, start=4):
                self.columns[i].write(value, offset)

    def get_rids_in_page(self):
        rids = self.columns[RID_COLUMN].data_to_int_array()
        return rids

    # Returns a copy of all of the Page's columns
    def create_copy(self):
        copied_columns = []
        for i in range(len(self.columns)):
            p = PhysicalPage()
            p.num_records = self.columns[i].use_num_records("r")
            p.tps = self.columns[i].get_tps()
            p.num_updates = self.columns[i].get_num_updates()
            p.data = self.columns[i].get_data().copy()
            copied_columns.append(p)
        return copied_columns


class PhysicalPage:
    def __init__(self):
        self.num_records = 0
        self.tps = 0
        self.num_updates = 0
        self.data = bytearray(PAGE_SIZE)

        # Milestone 2: additional fields for persisting physical pages
        self.is_dirty = False
        self.pin_count = 0

        self.lock = threading.Lock()
        self.num_records_lock = threading.Lock()

    # "p": (pre)read then increment
    # "w": write (increment) then read
    # "r": read
    def use_num_records(self, operation):
        self.num_records_lock.acquire()

        max_records = (PAGE_SIZE - PHYSICAL_PAGE_METADATA_SIZE) // COLUMN_SIZE
        if self.num_records >= max_records:
            self.num_records_lock.release()
            return False

        res = -1
        if operation == "p":
            res = self.num_records
            self.num_records += 1
            # print(f"res: {res}")
        elif operation == "w":
            self.num_records += 1
            res = self.num_records
        elif operation == "r":
            res = self.num_records
        self.num_records_lock.release()
        return res

    def get_dirty_bit(self):
        self.lock.acquire()
        res = self.is_dirty
        self.lock.release()
        return res

    def use_pin_count(self, operation):
        self.lock.acquire()
        res = None
        if operation == "r":
            res = self.pin_count
        elif operation == "i":
            self.pin_count += 1
            res = self.pin_count
        elif operation == "d":
            self.pin_count -= 1
            res = self.pin_count
        self.lock.release()
        return res

    def get_tps(self):
        self.lock.acquire()
        res = self.tps
        self.lock.release()
        return res

    def get_num_updates(self):
        self.lock.acquire()
        res = self.num_updates
        self.lock.release()
        return res

    def get_data(self):
        self.lock.acquire()
        res = self.data
        self.lock.release()
        return res

    def has_capacity(self):
        self.lock.acquire()
        max_records = (PAGE_SIZE - PHYSICAL_PAGE_METADATA_SIZE) // COLUMN_SIZE
        res = self.num_records < max_records

        self.lock.release()

        return res

    # Appends @value as a config.COLUMN_SIZE byte integer in Big-Endian to self.data
    # Returns true if write is successful and false otherwise
    def write(self, value, offset):
        max_records = (PAGE_SIZE - PHYSICAL_PAGE_METADATA_SIZE) // COLUMN_SIZE

        if offset >= max_records:
            self.lock.release()
            return False

        start = (COLUMN_SIZE * offset) + PHYSICAL_PAGE_METADATA_SIZE
        end = (COLUMN_SIZE * (offset + 1)) + PHYSICAL_PAGE_METADATA_SIZE
        self.data[start:end] = value.to_bytes(COLUMN_SIZE, "big", signed=True)

        # Update metadata for number of records
        self.data[NUM_RECORDS_START:NUM_RECORDS_END] = self.num_records.to_bytes(
            COLUMN_SIZE, "big", signed=True
        )
        # Update metadata for TPS of base page
        self.data[TPS_START:TPS_END] = self.tps.to_bytes(
            COLUMN_SIZE, "big", signed=True
        )
        # Update metadata for number of updates of the base page
        self.data[NUM_UPDATES_START:NUM_UPDATES_END] = self.num_updates.to_bytes(
            COLUMN_SIZE, "big", signed=True
        )
        self.is_dirty = True

        return offset

    def read_val(self, offset):
        self.lock.acquire()

        start = (COLUMN_SIZE * offset) + PHYSICAL_PAGE_METADATA_SIZE
        end = (COLUMN_SIZE * (offset + 1)) + PHYSICAL_PAGE_METADATA_SIZE
        val = int.from_bytes(self.data[start:end], "big", signed=True)

        self.lock.release()
        return val

    def change_val(self, new_val, offset):
        self.lock.acquire()

        start = (COLUMN_SIZE * offset) + PHYSICAL_PAGE_METADATA_SIZE
        end = (COLUMN_SIZE * (offset + 1)) + PHYSICAL_PAGE_METADATA_SIZE
        self.data[start:end] = new_val.to_bytes(COLUMN_SIZE, "big", signed=True)

        self.lock.release()

    def data_to_int_array(self):
        self.lock.acquire()

        int_data = []
        for i in range(self.num_records):
            start = (COLUMN_SIZE * i) + PHYSICAL_PAGE_METADATA_SIZE
            end = (COLUMN_SIZE * (i + 1)) + PHYSICAL_PAGE_METADATA_SIZE

            data = int.from_bytes(
                self.data[start:end],
                "big",
                signed=True,
            )

            int_data.append(data)

        self.lock.release()
        return int_data


class PageRange:
    def __init__(self):
        self.base_pages = []
        self.tail_pages = []

    def has_capacity(self):
        # PageRange doesn't have capacity if it has max base pages and
        # its last base page's physical pages is full
        if (
            len(self.base_pages) == MAX_BASE_PAGES
            and not self.base_pages[-1].columns[DATA_COL_START].has_capacity()
        ):
            return False

        return True

    # Fixed number of base pages
    def insert_base_page(self, num_columns):
        if not self.has_capacity():
            return False

        self.base_pages.append(Page(num_columns))
        return True

    # Unlimited number of tail pages
    def insert_tail_page(self, num_columns):
        if len(self.tail_pages) == 0:
            self.tail_pages.append(Page(num_columns))
        elif not self.tail_pages[-1].columns[0].has_capacity():
            self.tail_pages.append(Page(num_columns))

    # Page numbers [0, MAX_BASE_PAGES - 1] refers to a base page
    # Pages [MAX_BASE_PAGES, INF) refer to a tail page
    def page_is_base_page(self, page_num):
        if page_num < MAX_BASE_PAGES:
            return True
        return False

    # Returns the metadata of a record given a page number, offset, and the column index
    def get_record_column_val(self, page_num, offset, column_num):
        page = None

        # Get the corresponding base or tail page
        if self.page_is_base_page(page_num):
            page = self.base_pages[page_num]
        else:
            page = self.tail_pages[page_num - MAX_BASE_PAGES]

        # Use the offset to find the starting and ending point of the data in
        # the bytearray stored by the physical page
        data_start_index = offset * COLUMN_SIZE + PHYSICAL_PAGE_METADATA_SIZE
        data_end_index = (
            (offset * COLUMN_SIZE) + COLUMN_SIZE + PHYSICAL_PAGE_METADATA_SIZE
        )

        data = int.from_bytes(
            page.columns[column_num].data[data_start_index:data_end_index],
            "big",
            signed=True,
        )
        return data

    def get_indirection_val(self, page_num, offset):
        return self.get_record_column_val(page_num, offset, INDIRECTION_COLUMN)

    # Returns a Record object given page number and offset
    # Note that this approach assumes that data is cumulative
    def get_record(self, page_num, offset):
        # Find all metadata values first
        metadata_columns = [
            INDIRECTION_COLUMN,
            RID_COLUMN,
            TIMESTAMP_COLUMN,
            SCHEMA_ENCODING_COLUMN,
        ]
        metadata = []
        for col in metadata_columns:
            metadata.append(self.get_record_column_val(page_num, offset, col))

        # Find all corresponding data values
        # num_columns is the same for base and tail pages
        num_columns = len(self.base_pages[0].columns)
        columns = []
        for i in range(DATA_COL_START, num_columns):
            value = self.get_record_column_val(page_num, offset, i)
            columns.append(value)

        # Create record object
        record = Record(metadata[RID_COLUMN], 0, columns)
        record.indirection = metadata[INDIRECTION_COLUMN]
        record.timestamp = metadata[TIMESTAMP_COLUMN]
        record.schema_encoding = metadata[SCHEMA_ENCODING_COLUMN]

        return record