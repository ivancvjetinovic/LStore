from lstore.table import Table, Record
from lstore.index import Index
import threading


class TransactionWorker:

    """
    # Creates a transaction worker object.
    """

    def __init__(self, transaction=[]):
        self.stats = []
        self.transactions = transaction.copy()
        self.result = 0
        self.thread = None

        pass

    """
    Appends t to transactions
    """

    def add_transaction(self, t):
        self.transactions.append(t)

    """
    Runs all transaction as a thread
    """

    def run(self):
        self.thread = threading.Thread(target=self.__run)
        self.thread.start()

    """
    Waits for the worker to finish
    """

    def join(self):
        self.thread.join()
        # print(f"{self.thread} joined")

    def __run(self):
        for transaction in self.transactions:
            # print(f"{threading.current_thread()} running transaction {transaction.id}")
            # Keep retrying the transaction if it aborts

            while True:
                val = transaction.run()

                # if transaction goes through
                if val:
                    # go to next transcation
                    break
                # if integrity constraint violated
                elif val == -1:
                    self.stats.append(False)
                    # go to next transcation
                    break
                # transaction aborted due to lock conflict
                else:
                    self.stats.append(False)
                    continue

            self.stats.append(True)

        # Counts the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))