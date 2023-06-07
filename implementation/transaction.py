from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import (
    DATA_COL_START,
    SELECT_SEARCH_KEY_ARG,
    SELECT_SEARCH_KEY_COL_ARG,
    SUM_START_RANGE_ARG,
    SUM_END_RANGE_ARG,
    SUM_COLUMN_ARG,
    UPDATE_PRIMARY_KEY_ARG,
    DELETE_PRIMARY_KEY_ARG,
    TIMESTAMP_COLUMN,
)
import threading


class Transaction:

    """
    # Creates a transaction object.
    """

    def __init__(self):
        self.queries = []
        self.table = None
        self.id = None
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """

    def add_query(self, query, table, *args):
        self.queries.append([query, args])
        self.table = table

        # Initialize transaction ID
        if self.id == None:
            self.id = self.table.use_num_transactions("w")

    # If you choose to implement this differently this method must
    # still return True if transaction commits or False on abort
    def run(self):
        # Acquire all locks first
        # Abort if a lock was not granted
        got_all_locks = self.__acquire_all_locks()
        if got_all_locks == False:
            return self.abort()
        elif got_all_locks == -1:
            # If an integrity constraint was violated, return -1
            self.table.lock_manager.release_held_locks(self.id)
            return -1

        # All locks have been acquired, perform the queries
        for query, args in self.queries:
            # Abort if there are integrity constraints
            if not query(*args):
                self.abort()
                return -1

        # print("done with queries")

        return self.commit()

    def abort(self):
        # print(f"aborting transaction {self.id}")
        self.table.lock_manager.release_held_locks(self.id)
        return False

    def commit(self):
        # print(f"committing transaction {self.id}")
        self.table.lock_manager.release_held_locks(self.id)
        return True

    def __acquire_all_locks(self):
        # Acquire all locks first
        for i, query_and_args in enumerate(self.queries):
            query, args = query_and_args
            operation = query.__name__

            if "select" in operation:
                lock_success = self.__acquire_lock_for_select(args)
                if lock_success == False:
                    return False
            elif "sum" in operation:
                lock_success = self.__acquire_lock_for_sum(args)
                if lock_success == False:
                    return False
            elif "insert" in operation:
                lock_success = self.__acquire_lock_for_insert(i)
                if lock_success == False:
                    return False
                elif lock_success == -1:
                    return -1
            elif "update" in operation:
                lock_success = self.__acquire_lock_for_update(args)
                if lock_success == False:
                    return False
                elif lock_success == -1:
                    return -1
            elif "delete" in operation:
                lock_success = self.__acquire_lock_for_delete(args)
                if lock_success == False:
                    return False
                elif lock_success == -1:
                    return -1
        return True

    def __acquire_lock_for_select(self, args):
        search_column = args[SELECT_SEARCH_KEY_COL_ARG]
        search_key = args[SELECT_SEARCH_KEY_ARG]
        rids = []

        # Get all RIDs using the index
        if self.table.index.is_indexed(search_column + DATA_COL_START):
            rids = self.table.index.locate(search_column + DATA_COL_START, search_key)
        else:
            # Since the column is not index, a scan must be performed
            # Request a lock on all base records
            rids = self.table.index.get_all_base_rids()

        # If no RIDs are found, there are no locks to be granted, so return True
        if rids == -1:
            return True

        # Request a lock for necessary RIDs
        if not self.table.lock_manager.acquire_shared_lock(rids, self.id):
            return False
        return True

    def __acquire_lock_for_sum(self, args):
        start = args[SUM_START_RANGE_ARG]
        end = args[SUM_END_RANGE_ARG]

        rids = self.table.index.locate_range(
            start, end, self.table.key + DATA_COL_START
        )

        # If no RIDs are found, there are no locks to be granted, so return True
        if rids == -1:
            return True

        # Request a lock for all RIDs within the range
        if not self.table.lock_manager.acquire_shared_lock(rids, self.id):
            return False
        return True

    def __acquire_lock_for_insert(self, query_index):
        # Check integrity constraint: Cannot insert a record with an existing primary key
        if self.table.index.is_indexed(DATA_COL_START + self.table.key):
            inserted_primary_key = self.queries[query_index][1][self.table.key]

            # Check if primary key already exists, return -1 if it does
            rids = self.table.index.locate(
                DATA_COL_START + self.table.key, inserted_primary_key
            )

            if rids != -1:
                return -1

        # Get the RID of the record to insert
        rid = self.table.use_num_records("p")

        # Request an exclusive lock
        if not self.table.lock_manager.acquire_exclusive_lock([rid], self.id):
            return False

        # Add the RID to the insert query
        self.queries[query_index][1] = list(self.queries[query_index][1])
        self.queries[query_index][1].append(rid)

        return True

    def __acquire_lock_for_update(self, args):
        primary_key = args[UPDATE_PRIMARY_KEY_ARG]
        rid = None

        # Check integrity constraint: Cannot update primary key to existing primary key
        updated_primary_key = args[self.table.key + UPDATE_PRIMARY_KEY_ARG + 1]
        if updated_primary_key != None and updated_primary_key != primary_key:
            # Check if primary key already exists, return -1 if it does
            rid = self.table.index.locate(
                DATA_COL_START + self.table.key, updated_primary_key
            )
            if rid != -1:
                return -1
        else:
            rid = self.table.index.locate(self.table.key + DATA_COL_START, primary_key)

        # If no RID is found, there are no locks to be granted, so return True
        if rid == -1:
            return True

        if not self.table.lock_manager.acquire_exclusive_lock(rid, self.id):
            return False
        return True

    def __acquire_lock_for_delete(self, args):
        primary_key = args[DELETE_PRIMARY_KEY_ARG]

        rid = self.table.index.locate(self.table.key + DATA_COL_START, primary_key)

        # If no RID is found, there are no locks to be granted, so return True
        if rid == -1:
            return True

        if not self.table.lock_manager.acquire_exclusive_lock(rid, self.id):
            return False
        return True