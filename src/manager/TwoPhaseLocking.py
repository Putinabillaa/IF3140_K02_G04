from enum import Enum
from type.Transaction import OperationType, Operation


class LockType(Enum):
    NONE = 0
    SHARED = 1
    EXCLUSIVE = 2


class TransactionState:
    def __init__(self) -> None:
        self.executed: list[Operation] = []
        self.queue: list[Operation] = []
        self.locks: list[str] = []
        self.is_committed = False


    def execute(self, operation: Operation) -> None:
        self.executed.append(operation)


    def enqueue(self, operation: Operation) -> None:
        self.queue.append(operation)


    def dequeue(self) -> Operation:
        return self.queue.pop(0)
    

    def lock_needed(self) -> tuple[str, LockType] | None:
        if len(self.queue) == 0:
            return None
        
        match self.queue[0].mode:
            case OperationType.READ:
                return (self.queue[0].item, LockType.SHARED)
            case OperationType.WRITE:
                return (self.queue[0].item, LockType.EXCLUSIVE)
            case _:
                return (self.queue[0].item, LockType.NONE)
    

    def lock(self, item: str) -> None:
        self.locks.append(item)


    def unlock_all(self) -> None:
        self.locks = []


    def commit(self) -> None:
        self.is_committed = True
        self.executed = []
        self.queue = []
        self.unlock_all()


    def rollback(self) -> None:
        self.queue = self.executed
        self.executed = []
        self.unlock_all()


class TwoPhaseLockingManager:
    # Configuration:
    # Automatic Acquisition of Locks
    # Rigorous Two-Phase Locking
    # Wound-Wait Scheme

    def __init__(self):
        # Initialize the transaction states and locks
        self.states: dict[int, TransactionState] = {}
        self.locks: dict[str, dict[int, LockType]] = {}


    def __has_access(self, operation: Operation) -> bool:
        # Check if the transaction that is executing the operation has access to the item        
        match operation.mode:
            case OperationType.READ:
                if operation.item not in self.locks.keys():
                    return False
                if operation.number not in self.locks[operation.item].keys():
                    return False
                return self.locks[operation.item][operation.number] >= LockType.SHARED
            case OperationType.WRITE:
                if operation.item not in self.locks.keys():
                    return False
                if operation.number not in self.locks[operation.item].keys():
                    return False
                return self.locks[operation.item][operation.number] == LockType.EXCLUSIVE
            case _:
                return True
            

    def __acquire_lock(self, transaction: Operation) -> bool:
        # Acquire the lock for the transaction
        # Return True if the lock is acquired else False
        # Precondition: The transaction doesn't have access needed to the item

        # TODO:
        # Case 1: Need to Write
        # 1. If no lock is present then acquire the lock
        # 2. If has shared lock and no other transaction has shared lock then acquire the lock
        # 3. If younger transaction is blocking then force them to rollback then acquire the lock
        # 4. If older transaction is blocking then return False
        # Case 2: Need to Read
        # 1. If no lock is present then acquire the lock
        # 2. If no other transaction has exclusive lock then acquire the lock
        # 3. If younger transaction is blocking then force them to rollback then acquire the lock
        # 4. If older transaction is blocking then return False
        pass

    def __process_read(self, transaction: Operation) -> None:
        # Process the read operation

        # TODO:
        # 1. Check if the transaction has access to the item, if yes then execute the transaction
        # 2. Try to acquire the lock, if yes then execute the transaction
        # 3. If the lock can't be acquired then put the transaction into queue
        pass


    def __process_write(self, transaction: Operation) -> None:
        # Process the write operation

        # TODO:
        # 1. Check if the transaction has access to the item, if yes then execute the transaction
        # 2. Try to acquire the lock, if yes then execute the transaction
        # 3. If the lock can't be acquired then put the transaction into queue
        pass


    def __process_commit(self, transaction: Operation) -> None:
        if transaction.number not in self.states.keys():
            self.states[transaction.number] = TransactionState()
        self.states[transaction.number].commit()
            

    def __process_transaction(self, Transaction: Operation) -> None:
        match Transaction.mode:
            case OperationType.READ:
                self.__process_read(Transaction)
            case OperationType.WRITE:
                self.__process_write(Transaction)
            case OperationType.COMMIT:
                self.__process_commit(Transaction)


    def __process_all_queue(self) -> None:
        # Process the queue of all transactions

        # TODO:
        # 1. Check if the transaction has access to the item, if yes then execute the transaction
        # 2. Check if locks needed can be given, if yes then give the locks and execute the transaction
        # 3. If the locks can't be given then skip the transaction queue
        pass


    def simulate(self, schedule: list[Operation]):
        # Simulate the given schedule
        for Transaction in schedule:
            self.__process_all_queue()
            self.__process_transaction(Transaction)


    def test(self):
        print(self.__has_access(Operation(OperationType.WRITE, 1, "A")))
        
