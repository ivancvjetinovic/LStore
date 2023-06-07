import threading


class LockManager:
    def __init__(self):
        # Maps {<RID> : <list of tuples (<"S" or "X">, transaction id)>}
        # "S": shared lock
        # "X": exclusive lock
        self.locks = {}

        # Maps {<transaction id> : <list of RIDs>}
        self.lock_owners = {}

        self.latch = threading.Lock()

    def print(self):
        self.latch.acquire()
        print(f"{threading.current_thread()}: {self.locks}")
        self.latch.release()

    # Allow a shared lock to be aquired if there are no locks on the record
    # or the only locks on a record are shared locks
    def acquire_shared_lock(self, keys, transaction_id):
        self.latch.acquire()
        for key in keys:
            if self.locks.get(key, -1) == -1:
                self.locks[key] = [("S", transaction_id)]
                self.__add_lock_owner(key, transaction_id)
                continue

            already_has_lock = False
            # Check for any exclusive locks on a record
            for l in self.locks[key]:
                # If the transaction already holds a lock (shared or exclusive),
                # continue and do not downgrade the lock
                if l[1] == transaction_id:
                    already_has_lock = True
                    break

                # Reject a lock request if there is an exclusive lock on the record
                # that is held by a different transaction
                if l[0] == "X" and l[1] != transaction_id:
                    self.latch.release()
                    return False

            if already_has_lock:
                continue

            # If there are no exclusive locks, grant the lock
            self.locks[key].append(("S", transaction_id))
            self.__add_lock_owner(key, transaction_id)

        self.latch.release()
        return True

    # Allow an exclusive lock to be aquired only if there are no locks on the record
    def acquire_exclusive_lock(self, keys, transaction_id):
        self.latch.acquire()
        for key in keys:
            # If there are no locks on the record, grant the lock
            if self.locks.get(key, -1) == -1:
                self.locks[key] = [("X", transaction_id)]
                self.__add_lock_owner(key, transaction_id)
                continue

            # Check all the locks that are on the record
            for i, l in enumerate(self.locks[key]):
                # Stop checking the locks if the same transaction already holds an exclusive lock
                if l[0] == "X" and l[1] == transaction_id:
                    break

                # Reject the request if there is a shared or exclusive lock
                # held by another transaction
                if l[1] != transaction_id:
                    self.latch.release()
                    return False

                # Upgrade the shared lock to an exclusive lock if the shared lock is
                # the only lock on the record
                if l[0] == "S" and l[1] == transaction_id and len(self.locks[key]) == 1:
                    self.locks[key][i] = ("X", transaction_id)

        self.latch.release()
        return True

    # Removes all the locks held by a transaction
    def release_held_locks(self, transaction_id):
        self.latch.acquire()

        # Return if the transaction does not own any locks
        if self.lock_owners.get(transaction_id, -1) == -1:
            self.latch.release()
            return

        for lock in self.lock_owners[transaction_id]:
            i = 0
            while i < len(self.locks[lock]):
                if self.locks[lock][i][1] == transaction_id:
                    self.locks[lock].pop(i)
                else:
                    i += 1

            # Remove the mapping in self.locks if there are no locks are on a record
            if len(self.locks[lock]) == 0:
                self.locks.pop(lock)

        self.lock_owners.pop(transaction_id)
        self.latch.release()

    # Add a mapping <transaction_id> : <list of keys> to self.lock_owners
    def __add_lock_owner(self, key, transaction_id):
        if self.lock_owners.get(transaction_id, -1) == -1:
            self.lock_owners[transaction_id] = [key]
        else:
            self.lock_owners[transaction_id].append(key)