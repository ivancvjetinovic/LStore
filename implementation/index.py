from lstore.config import (
    RID_COLUMN,
    DATA_COL_START,
    COLUMN_SIZE,
)
import threading


class Index:
    def __init__(self, table):
        # One index for each table. All are empty initially.
        self.indices = [None] * table.total_num_columns
        self.table = table
        self.lock = threading.Lock()

    # Returns True if a column is indexed and False otherwise
    def is_indexed(self, column):
        self.lock.acquire()

        res = self.indices[column] != None

        self.lock.release()

        return res

    # Go through the primary key column and get all the mappings to base RIDs
    def get_all_base_rids(self):
        self.lock.acquire()

        if self.indices[self.table.key + DATA_COL_START] == None:
            self.lock.release()
            return -1

        rids = []
        for v in self.indices[self.table.key + DATA_COL_START].values():
            rids += v

        self.lock.release()

        return rids

    # Returns the RID of all records in a base page containing the value or
    # pointing to a tail record that contains the value
    def locate(self, column, value):
        self.lock.acquire()

        if self.indices[column] == None:
            self.lock.release()
            return -1

        rids = self.indices[column].get(value, -1)

        self.lock.release()

        return rids

    # Returns the RID of all records in a base page containing a value within
    # the given range or pointing to a tail record that contains a value within range
    def locate_range(self, begin, end, column):
        self.lock.acquire()

        if self.indices[column] == None:
            self.lock.release()
            return -1

        rids = []
        for i in range(begin, end + 1):
            found_rids = self.indices[column].get(i, -1)
            if found_rids == -1:
                continue
            for r in found_rids:
                rids.append(r)

        self.lock.release()

        return rids

    # Drop index of specific column
    def drop_index(self, column_number):
        self.lock.acquire()

        self.indices[column_number] = None

        self.lock.release()

    # Create an index on specific column
    def create_index(self, column_number):
        self.lock.acquire()

        # Index using a hash map: {data in column_number : RID}
        self.indices[column_number] = {}

        # Go through every page range and look through every base page
        # Add the latest record's data in the given column to the hash map
        for pr_num in range(self.table.num_page_ranges):
            max_num_bp = self.table.bufferpool.get_highest_base_page_num(
                self.table.name, pr_num
            )

            # Add all column values in the base page to the hash map
            for bp_num in range(max_num_bp + 1):
                rids_page = self.table.bufferpool.get_physical_page(
                    self.table.name, pr_num, bp_num, RID_COLUMN
                )
                rids_page.pin_count += 1
                self.__index_base_page_data(
                    rids_page.data_to_int_array(), column_number
                )
                rids_page.pin_count -= 1

        self.lock.release()

    # Helper function for create_index
    # Maps the given column's data to a list of RIDs of base page records
    # Given a value, the index will return a list of RIDs of base page
    # records whose latest column has that value
    def __index_base_page_data(self, rids, column_number):
        # Go through the base page and add the latest record's data in the
        # given column to the index
        for rid in rids:
            col_val = self.table.get_latest_column_val(rid, column_number)
            if col_val == None:
                continue

            # Add key and rid to the hash map
            # If key isn't in the hash map, initialize it to be an array containing rid
            if self.indices[column_number].get(col_val, -1) == -1:
                self.indices[column_number][col_val] = [rid]
            else:
                self.indices[column_number][col_val].append(rid)

    # Given a column number, the old value, and the new value,
    # remove the mapping from the old value to the RID and add a mapping
    # from the new value to the RID provided that an index has
    # already been created on the column.
    def update_index(self, column_number, old_value, new_value, base_rid):
        self.lock.acquire()

        # Check if index has been created yet and if primary key column has been indexed
        # Cannot proceed if at least one of these conditions is false
        if (
            self.indices[column_number] == None
            or self.indices[self.table.key + DATA_COL_START] == None
        ):
            self.lock.release()
            return False

        # Find RID of corresponding record in base page
        # rid = self.indices[column_number].get(old_value, -1)
        # if rid == -1:
        #     return False
        # rid = rid[0]

        # Remove the mapping from the old value to the RID and
        # add or create the new one if necessary
        self.indices[column_number][old_value].remove(base_rid)
        if len(self.indices[column_number][old_value]) == 0:
            self.indices[column_number].pop(old_value)

        if self.indices[column_number].get(new_value, -1) == -1:
            self.indices[column_number][new_value] = [base_rid]
        else:
            self.indices[column_number][new_value].append(base_rid)

        self.lock.release()

        return True

    # Deletes all indices given an array of possible keys and
    # the RID of the corresponding base record
    def delete_index(self, possible_keys, base_rid):
        self.lock.acquire()

        for i, key in enumerate(possible_keys):
            # Check if an index has been created on the column
            if self.indices[i] != None:
                # Check if the key has been added to the index
                list_of_rids = self.indices[i].get(key, -1)
                if list_of_rids == -1:
                    continue

                # Scan the list of RIDs for base_rid and remove if there's a match
                for rid in list_of_rids:
                    if base_rid == rid:
                        list_of_rids.remove(base_rid)
                        break

                # Remove the mapping from key to RIDs if there are no RIDs to map to
                if len(list_of_rids) == 0:
                    self.indices[i].pop(key)

        self.lock.release()

    # Add a mapping from key to value
    def add(self, column, key, value):
        if self.indices[column] == None:
            return False

        if self.indices[column].get(key, -1) == -1:
            self.indices[column][key] = [value]
        else:
            self.indices[column][key].append(value)