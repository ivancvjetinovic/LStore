from lstore.table import Table
from lstore.bufferpool import Bufferpool
import os
import pickle


class Database:
    def __init__(self):
        self.tables = []
        self.bufferpool = Bufferpool(self)
        self.path = ""

    # Not required for milestone1
    def open(self, path):
        if self.path == "":
            self.path = os.path.abspath(path)
        first_open = False
        try:
            os.mkdir(self.path)
            first_open = True
        except FileExistsError:
            pass

        if first_open:
            return

        # Read catalog to initialize tables' name, num_columns, key, and num_records
        catalog_data = []
        with open(self.path + "/catalog", "r") as f:
            catalog_lines = f.read().splitlines()
            for s in catalog_lines:
                catalog_data.append(s.split(" "))

        for i in range(len(catalog_data)):
            # Create table object with data read from catalog
            line = catalog_data[i]
            table = Table(line[0], int(line[1]), int(line[2]), self.bufferpool)
            table.num_records = int(line[3])

            # Initialize page_ranges_metadata and num_page_ranges
            with open(self.path + "/" + table.name + "/page_ranges_metadata", "r") as f:
                lines = f.read().splitlines()
                for l in lines:
                    page_range_num, num_bp, num_tp = l.split(" ")
                    page_range_data = [int(page_range_num), int(num_bp), int(num_tp)]
                    table.page_ranges_metadata.append(page_range_data)

            table.num_page_ranges = len(table.page_ranges_metadata)

            self.tables.append(table)

        # Initialize indices, page directories, number of page ranges, and number of records
        for t in self.tables:
            table_path = self.path + "/" + t.name

            # Initialize indices
            index_file_path = table_path + "/index"
            t.index.indices = pickle.load(open(index_file_path, "rb"))

            # Initialize page directories
            page_directory_file_path = self.path + "/" + t.name + "/page_directory"
            t.page_directory = pickle.load(open(page_directory_file_path, "rb"))

    def close(self):
        self.bufferpool.flush()

        catalog_lines = []
        # Save catalog, indices, page directories, and page range metadata
        for t in self.tables:
            # Get all lines to write to the catalog
            line = (
                t.name
                + " "
                + str(t.num_columns)
                + " "
                + str(t.key)
                + " "
                + str(t.num_records)
            )
            catalog_lines.append(line)

            # Save indices using pickle
            # Path: "<db_name>/<table_name>/index"
            index_file_path = self.path + "/" + t.name + "/index"
            pickle.dump(t.index.indices, open(index_file_path, "wb"))

            # Save page directories using pickle
            # Path: "<db_name>/<table_name>/page_directory"
            page_directory_file_path = self.path + "/" + t.name + "/page_directory"
            pickle.dump(t.page_directory, open(page_directory_file_path, "wb"))

            # Update page range metadata
            # Format: "<page_range_num> <num_base_pages> <num_tail_pages>"
            # Path: "<db_name>/<table_name>/page_ranges_metadata"
            with open(self.path + "/" + t.name + "/page_ranges_metadata", "w") as f:
                for line in t.page_ranges_metadata:
                    line_to_string = " ".join([str(i) for i in line])
                    f.write(line_to_string + "\n")

        # Save catalog
        # Format: "<table_name> <num_columns> <key> <num_records>"
        # Path: "<db_name>/catalog"
        catalog_path = self.path + "/catalog"
        with open(catalog_path, "w") as f:
            for l in catalog_lines:
                f.write(l + "\n")

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(self, name, num_columns, key_index):
        # If a table with the same name has already been created, return None
        for t in self.tables:
            if t.name == name:
                return None

        table = Table(name, num_columns, key_index, self.bufferpool)

        # Create the table folder
        try:
            os.chdir(self.path)
            os.mkdir(self.path + "/" + name)
        except (FileNotFoundError, FileExistsError):
            pass

        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """

    def drop_table(self, name):
        for table in self.tables:
            if table.name == name:
                self.tables.remove(table)
                return True
        return False

    """
    # Returns table with the passed name
    """

    def get_table(self, name):
        for table in self.tables:
            if table.name == name:
                return table
        return None