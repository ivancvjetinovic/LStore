from collections import deque
from lstore.page import Page, PhysicalPage
from lstore.record import Record
from pathlib import Path
import os
from lstore.config import (
    NUM_RECORDS_START,
    NUM_RECORDS_END,
    BUFFERPOOL_SIZE,
    PAGE_SIZE,
    MAX_BASE_PAGES,
    PHYSICAL_PAGE_METADATA_SIZE,
    COLUMN_SIZE,
    INDIRECTION_COLUMN,
    RID_COLUMN,
    TIMESTAMP_COLUMN,
    SCHEMA_ENCODING_COLUMN,
    DATA_COL_START,
    TPS_START,
    TPS_END,
    NUM_UPDATES_START,
    NUM_UPDATES_END,
)
import threading
import mmap


class Bufferpool:
    def __init__(self, db):
        self.pages = {}
        self.max_capacity = BUFFERPOOL_SIZE
        self.db = db

        # The least recently used pages are at the front of the queue
        # and the most recently used ones are at the end
        self.lru_pages = deque()

        self.evict_lock = threading.Lock()
        self.page_locks = {}

    def has_capacity(self):
        if len(self.pages) == self.max_capacity:
            return False
        return True

    # Returns a Page object and adds it to the bufferpool
    def get_page(self, table_name, page_range_num, page_num):
        return self.__get_page_helper(table_name, page_range_num, page_num, True)

    # Returns a Page and does not add it to the bufferpool (used in merge)
    def get_page_no_add(self, table_name, page_range_num, page_num):
        return self.__get_page_helper(table_name, page_range_num, page_num, False)

    # Returns a Page and adds it to the bufferpool
    # depending on @add_to_bufferpool's boolean value
    def __get_page_helper(
        self, table_name, page_range_num, page_num, add_to_bufferpool
    ):
        columns = []
        num_columns = self.db.get_table(table_name).total_num_columns
        for i in range(num_columns):
            phys_page = self.__get_physical_page_helper(
                table_name, page_range_num, page_num, i, add_to_bufferpool
            )
            columns.append(phys_page)

        page = Page(0)
        page.columns = columns
        return page

    # Returns a PhysicalPage object and adds it to the bufferpool
    def get_physical_page(self, table_name, page_range_num, page_num, column_num):
        return self.__get_physical_page_helper(
            table_name, page_range_num, page_num, column_num, True
        )

    # Returns a PhysicalPage object and does not add it to the bufferpool (used in merge)
    def get_physical_page_no_add(
        self, table_name, page_range_num, page_num, column_num
    ):
        return self.__get_physical_page_helper(
            table_name, page_range_num, page_num, column_num, False
        )

    # Returns a PhysicalPage and adds it to the bufferpool
    # depending on @add_to_bufferpool's boolean value
    def __get_physical_page_helper(
        self, table_name, page_range_num, page_num, column_num, add_to_bufferpool
    ):
        phys_page_path = self.__create_phys_page_path(
            table_name, page_range_num, page_num, column_num
        )

        # If the bufferpool is currently using a page, wait until the page is available
        if self.page_locks.get(phys_page_path, -1) == -1:
            self.page_locks[phys_page_path] = threading.Lock()
            self.page_locks[phys_page_path].acquire()
        else:
            self.page_locks[phys_page_path].acquire()

        # Return the page if it is in the bufferpool
        if self.pages.get(phys_page_path, -1) != -1:
            self.page_locks[phys_page_path].release()
            return self.pages[phys_page_path]

        # Get the page from disk and add it to the bufferpool if there is space
        # Otherwise evict the least recently used page and then get the page
        if not self.has_capacity() and add_to_bufferpool:
            self.__evict()

        page = self.__fetch_phys_page(phys_page_path)

        if add_to_bufferpool:
            # Add the physical page to the bufferpool
            self.pages[phys_page_path] = page

            # Add the key of the physical page to the end of the queue
            self.lru_pages.append(phys_page_path)

        self.page_locks[phys_page_path].release()
        return page

    def get_tps(self, table_name, page_range_num, page_num):
        phys_page = self.get_physical_page(
            table_name, page_range_num, page_num, RID_COLUMN
        )

        return int.from_bytes(phys_page.data[TPS_START:TPS_END], "big", signed=True)

    def get_num_updates(self, table_name, page_range_num, page_num):
        phys_page = self.get_physical_page(
            table_name, page_range_num, page_num, RID_COLUMN
        )

        return int.from_bytes(
            phys_page.data[NUM_UPDATES_START:NUM_UPDATES_END], "big", signed=True
        )

    def set_tps(self, table_name, page_range_num, page_num, new_tps):
        phys_page = self.get_physical_page(
            table_name, page_range_num, page_num, RID_COLUMN
        )

        phys_page.data[TPS_START:TPS_END] = new_tps.to_bytes(COLUMN_SIZE, "big")

    def set_num_updates(self, table_name, page_range_num, page_num, num_updates):
        phys_page = self.get_physical_page(
            table_name, page_range_num, page_num, RID_COLUMN
        )

        phys_page.data[NUM_UPDATES_START:NUM_UPDATES_END] = num_updates.to_bytes(
            COLUMN_SIZE, "big"
        )

    # Return a record object
    def get_record(self, table_name, rid):
        # Find all metadata values first
        metadata_columns = [
            INDIRECTION_COLUMN,
            RID_COLUMN,
            TIMESTAMP_COLUMN,
            SCHEMA_ENCODING_COLUMN,
        ]
        metadata = []
        for col in metadata_columns:
            metadata.append(self.get_record_column_val(table_name, rid, col))

        # Find all corresponding data values
        # num_columns is the same for base and tail pages
        num_columns = self.db.get_table(table_name).total_num_columns
        columns = []
        for i in range(DATA_COL_START, num_columns):
            value = self.get_record_column_val(table_name, rid, i)
            columns.append(value)

        # Create record object
        record = Record(
            metadata[RID_COLUMN], self.db.get_table(table_name).key, columns
        )
        record.indirection = metadata[INDIRECTION_COLUMN]
        record.timestamp = metadata[TIMESTAMP_COLUMN]
        record.schema_encoding = metadata[SCHEMA_ENCODING_COLUMN]

        return record

    def get_record_column_val(self, table_name, rid, column_num):
        # Find the record using the page directory
        table = self.db.get_table(table_name)
        page_range_num, page_num, offset = table.use_page_directory("r", rid, None)

        # Get the physical page containing the corresponding column value
        page = self.get_physical_page(table_name, page_range_num, page_num, column_num)

        data_start_index = offset * COLUMN_SIZE + PHYSICAL_PAGE_METADATA_SIZE
        data_end_index = (
            (offset * COLUMN_SIZE) + COLUMN_SIZE + PHYSICAL_PAGE_METADATA_SIZE
        )

        data = int.from_bytes(
            page.data[data_start_index:data_end_index],
            "big",
            signed=True,
        )
        return data

    # Create a page range folder and return the page range's number
    def create_page_range(self, table_name):
        max_pr_num = self.get_highest_page_range_num(table_name)

        # If the page range still has capacity, do not add a new one
        if max_pr_num != -1 and self.page_range_has_capacity(table_name, max_pr_num):
            return max_pr_num

        # If the page range is full, increment the page range number and
        # add a new page range
        max_pr_num += 1

        # Go to the given table's folder, make it if it doesn't exist
        table_path = self.db.path + "/" + table_name
        self.__go_to_path(table_path)

        # Names of page range folders are "page_range<#>", where <#> starts at 0
        page_range_name = "page_range" + str(max_pr_num)
        os.mkdir(page_range_name)

        self.db.get_table(table_name).num_page_ranges += 1
        self.db.get_table(table_name).page_ranges_metadata.append(
            [max_pr_num, -1, MAX_BASE_PAGES - 1]
        )

        return max_pr_num

    # Creates a base page and initializes it with physical pages
    def insert_base_page(self, table_name, page_range_num):
        # Go to the given page range's folder and get the highest base page's number
        page_range_path = (
            self.db.path + "/" + table_name + "/page_range" + str(page_range_num)
        )
        self.__go_to_path(page_range_path)
        max_bp_num = self.get_highest_base_page_num(table_name, page_range_num)

        if max_bp_num != -1 and self.page_has_capacity(
            table_name, page_range_num, max_bp_num
        ):
            return False

        # Check if more base pages can be added
        # Return False if page range is full
        if max_bp_num == MAX_BASE_PAGES - 1:
            return False

        # Otherwise, create the base page
        page_name = "page" + str(max_bp_num + 1)
        os.mkdir(page_name)
        os.chdir(page_name)

        num_columns = self.db.get_table(table_name).total_num_columns

        # Create physical pages
        for i in range(num_columns):
            phys_page_name = "col" + str(i)
            with open(phys_page_name, "wb") as f:
                f.write(bytearray(PAGE_SIZE))

        # Update page range metadata's base page number
        self.db.get_table(table_name).page_ranges_metadata[page_range_num][1] += 1

        return True

    # Creates a tail page and initializes it with physical pages
    def insert_tail_page(self, table_name, page_range_num):
        # Go to the given page range's folder and get the highest base page's number
        page_range_path = (
            self.db.path + "/" + table_name + "/page_range" + str(page_range_num)
        )
        self.__go_to_path(page_range_path)
        tail_page_num = self.get_highest_tail_page_num(table_name, page_range_num)

        if tail_page_num != MAX_BASE_PAGES - 1 and self.page_has_capacity(
            table_name, page_range_num, tail_page_num
        ):
            return False

        # If no tail pages exist, set the page number to 16
        # Otherwise, add 1 to generate a new tail page
        tail_page_num += 1

        # Create the tail page
        page_name = "page" + str(tail_page_num)
        os.mkdir(page_name)
        os.chdir(page_name)

        # Create physical pages
        num_columns = self.db.get_table(table_name).total_num_columns
        for i in range(num_columns):
            phys_page_name = "col" + str(i)
            with open(phys_page_name, "wb") as f:
                f.write(bytearray(PAGE_SIZE))

        # Update page range metadata's tail page number (starts at MAX_BASE_PAGES)
        self.db.get_table(table_name).page_ranges_metadata[page_range_num][2] += 1

        return True

    def page_range_has_capacity(self, table_name, page_range_num):
        # Check if there are less than 16 base pages
        # If so, the page range still has capacity
        max_bp_num = self.get_highest_base_page_num(table_name, page_range_num)
        if max_bp_num < MAX_BASE_PAGES - 1:
            return True

        # Otherwise, check the capacity of base page #16's physical pages
        return self.page_has_capacity(table_name, page_range_num, max_bp_num)

    def page_has_capacity(self, table_name, page_range_num, page_num):
        # Get a physical page of the corresponding page
        phys_page = self.get_physical_page(table_name, page_range_num, page_num, 0)

        return phys_page.has_capacity()

    # Takes in a Page object and pins all of its physical pages
    def pin_page(self, page):
        for p in page.columns:
            p.use_pin_count("i")

    # Takes in a Page object and unpins all of its physical pages
    def unpin_page(self, page):
        for p in page.columns:
            p.use_pin_count("d")

    def flush(self):
        keys = self.pages.keys()
        for k in keys:
            self.__write_page_to_disk(k)
        self.pages.clear()
        self.lru_pages.clear()

    def write_page_to_disk(self, table_name, page_range_num, page_num, page):
        for i in range(DATA_COL_START, len(page.columns)):
            col_file_name = self.__create_phys_page_path(
                table_name, page_range_num, page_num, i
            )
            page_folder_path = str(Path(col_file_name).parents[0])

            # Rename the old file to old_col<#>_<tps>
            tps = page.columns[RID_COLUMN].tps
            new_name = page_folder_path + "/old_col" + str(i) + "_" + str(tps)
            os.rename(col_file_name, new_name)

            with open(col_file_name, "wb") as f:
                physical_page = page.columns[i]
                f.write(physical_page.data)

    # Frees up a spot in the bufferpool
    # Writes the page to disk if it is dirty
    # Evicts at the granularity of physical pages
    def __evict(self):
        self.evict_lock.acquire()
        if len(self.pages) < self.max_capacity:
            self.evict_lock.release()
            return True

        page_to_evict = None
        key_of_page = None
        for path in self.lru_pages:
            phys_pg = self.pages[path]

            # Evict the first page that has a pin count of 0
            if phys_pg.pin_count == 0:
                page_to_evict = phys_pg
                key_of_page = path
                break

        # If all pages are pinned, return False
        if page_to_evict == None:
            return False

        # Write the page to disk
        if page_to_evict.is_dirty:
            self.__write_page_to_disk(key_of_page)

        # Remove the physical page from the bufferpool
        self.pages.pop(key_of_page)
        self.lru_pages.remove(key_of_page)
        self.page_locks.pop(key_of_page)
        self.evict_lock.release()

    # Retrieve a page from disk
    def __fetch_phys_page(self, phys_page_path):
        data = None

        with open(phys_page_path, "r+b") as f:
            mmap_obj = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            data = bytearray(mmap_obj.read())

        retrieved_pg = PhysicalPage()
        retrieved_pg.data = data
        retrieved_pg.num_records = int.from_bytes(
            data[NUM_RECORDS_START:NUM_RECORDS_END], "big", signed=True
        )
        retrieved_pg.tps = int.from_bytes(data[TPS_START:TPS_END], "big", signed=True)
        retrieved_pg.num_updates = int.from_bytes(
            data[NUM_UPDATES_START:NUM_UPDATES_END], "big", signed=True
        )

        return retrieved_pg

    # Persists the physical page to disk
    def __write_page_to_disk(self, phys_page_path):
        with open(phys_page_path, "r+b") as f:
            mmap_obj = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_WRITE)
            physical_page = self.pages[phys_page_path]
            mmap_obj.write(physical_page.data)

    def __go_to_path(self, destination):
        home_path = self.db.path
        directories_to_go_to = [destination]

        # Get the path of all directories from home to destination
        for i in range(len(Path(destination).parents)):
            parent_dir_path = str(Path(destination).parents[i])
            if home_path == parent_dir_path:
                break
            directories_to_go_to.append(parent_dir_path)

        # Go to each directory in directories_to_go_to and make them if they do not exist
        # Iterate backwards since the order in directories_to_go_to is in reverse
        for i in range(len(directories_to_go_to) - 1, -1, -1):
            try:
                os.chdir(str(directories_to_go_to[i]))
            except FileNotFoundError:
                os.mkdir(str(directories_to_go_to[i]))
                os.chdir(str(directories_to_go_to[i]))

    # Returns the path of a physical page
    def __create_phys_page_path(self, table_name, page_range_num, page_num, column_num):
        phys_page_path = self.db.path + "/" + table_name
        phys_page_path += "/page_range" + str(page_range_num)
        phys_page_path += "/page" + str(page_num)
        phys_page_path += "/col" + str(column_num)
        return phys_page_path

    # Returns the highest number of a base page within the page range
    # Page number falls in the interval [0, 15]
    def get_highest_base_page_num(self, table_name, page_range_num):
        pr_metadata = self.db.get_table(table_name).page_ranges_metadata
        max_bp_num = pr_metadata[page_range_num][1]

        return max_bp_num

    # Returns the highest number of a tail page within the page range
    # Page number is greater than or equal to 16
    def get_highest_tail_page_num(self, table_name, page_range_num):
        pr_meta_data = self.db.get_table(table_name).page_ranges_metadata
        max_tp_num = pr_meta_data[page_range_num][2]

        return max_tp_num

    # Returns the highest page range number in a table
    # Returns -1 if there are no page ranges
    def get_highest_page_range_num(self, table_name):
        return self.db.get_table(table_name).num_page_ranges - 1