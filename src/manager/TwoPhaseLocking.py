from copy import deepcopy
from enum import Enum
from type.Operation import Operation, OperationType
from type.TPOutput import TPOutput, TPOutputOperationType


max_int: int = 2147483647


class LockType(Enum):
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
                return None
    

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
        self.final_schedule: list[TPOutput] = []


    def __has_access(self, operation: Operation) -> bool:
        # Check if the transaction that is executing the operation has access to the item        
        match operation.mode:
            case OperationType.READ:
                if operation.item not in self.locks.keys():
                    return False
                if operation.number not in self.locks[operation.item].keys():
                    return False
                return self.locks[operation.item][operation.number].value >= LockType.SHARED.value
            case OperationType.WRITE:
                if operation.item not in self.locks.keys():
                    return False
                if operation.number not in self.locks[operation.item].keys():
                    return False
                return self.locks[operation.item][operation.number] == LockType.EXCLUSIVE
            case _:
                return True
            

    def __acquire_shared_lock(self, operation: Operation) -> bool:
        # Acquire the shared lock for the transaction
        # Return True if the lock is acquired else False
        # Precondition: The transaction doesn't have access needed to the item


        if operation.item not in self.locks.keys():
            self.locks[operation.item] = {}

        # If no lock is present then acquire the lock
        if len(self.locks[operation.item].keys()) == 0:
            self.states[operation.number].lock(operation.item)
            self.locks[operation.item][operation.number] = LockType.SHARED
            self.final_schedule.append(TPOutput(TPOutputOperationType.GRANT_S, operation.number, operation.item))
            return True
        
        # If no other transaction has exclusive lock then acquire the lock
        if LockType.EXCLUSIVE not in self.locks[operation.item].values():
            self.states[operation.number].lock(operation.item)
            self.locks[operation.item][operation.number] = LockType.SHARED
            self.final_schedule.append(TPOutput(TPOutputOperationType.GRANT_S, operation.number, operation.item))
            return True
        
        # If younger transaction is blocking then force them to rollback then acquire the lock
        key_list = list(self.locks[operation.item].keys())
        value_list = list(self.locks[operation.item].values())

        exclusive_yielder = key_list[value_list.index(LockType.EXCLUSIVE)]
        if exclusive_yielder > operation.number:
            for item in self.states[exclusive_yielder].locks:
                # self.locks[item][exclusive_yielder] = LockType.NONE
                if exclusive_yielder in self.locks[item].keys():
                    self.locks[item].pop(exclusive_yielder)

            self.states[exclusive_yielder].rollback()
            self.final_schedule.append(TPOutput(TPOutputOperationType.ABORT, exclusive_yielder))
            self.states[operation.number].lock(operation.item)
            self.locks[operation.item][operation.number] = LockType.SHARED
            self.final_schedule.append(TPOutput(TPOutputOperationType.GRANT_S, operation.number, operation.item))
            return True

        # If older transaction is blocking then return False
        self.states[operation.number].enqueue(operation)
        return False



    def __acquire_exclusive_lock(self, operation: Operation) -> bool:
        # Acquire the lock for the transaction
        # Return True if the lock is acquired else False
        # Precondition: The transaction doesn't have access needed to the item

        # TODO:
        # 1. If no lock is present then acquire the lock
        # 2. If has shared lock and no other transaction has shared lock then acquire the lock
        # 3. If younger transaction is blocking then force them to rollback then acquire the lock
        # 4. If older transaction is blocking then return False
        
        if operation.item not in self.locks.keys():
            self.locks[operation.item] = {}

        # If no lock is present then acquire the lock
        if len(self.locks[operation.item].keys()) == 0:
            self.states[operation.number].lock(operation.item)
            self.locks[operation.item][operation.number] = LockType.EXCLUSIVE
            self.final_schedule.append(TPOutput(TPOutputOperationType.GRANT_X, operation.number, operation.item))
            return True

        # If has shared lock and no other transaction has shared lock then upgrade the lock
        oldest_other_transaction: int = max_int
        found_other: bool = False
        has_shared: bool = False

        for key in self.locks[operation.item].keys():
            if key != operation.number:
                found_other = True
            else:
                has_shared = True

            if key < oldest_other_transaction:
                oldest_other_transaction = key

        if not found_other:
            if has_shared:
                self.states[operation.number].lock(operation.item)
                self.locks[operation.item][operation.number] = LockType.EXCLUSIVE
                self.final_schedule.append(TPOutput(TPOutputOperationType.UPGRADE, operation.number, operation.item))
                return True
            else:
                self.states[operation.number].lock(operation.item)
                self.locks[operation.item][operation.number] = LockType.EXCLUSIVE
                self.final_schedule.append(TPOutput(TPOutputOperationType.GRANT_X, operation.number, operation.item))
                return True

        # If younger transaction is blocking then force them to rollback then acquire the lock
        if oldest_other_transaction >= operation.number:
            for key in sorted(self.locks[operation.item].keys()):
                if key == operation.number: continue

                for item in self.states[key].locks:
                    # self.locks[item][key] = LockType.NONE
                    if key in self.locks[item].keys():
                        self.locks[item].pop(key)

                self.states[key].rollback()
                self.final_schedule.append(TPOutput(TPOutputOperationType.ABORT, key))
            
            self.states[operation.number].lock(operation.item)
            self.locks[operation.item][operation.number] = LockType.EXCLUSIVE
            if has_shared:
                self.final_schedule.append(TPOutput(TPOutputOperationType.UPGRADE, operation.number, operation.item))
            else:
                self.final_schedule.append(TPOutput(TPOutputOperationType.GRANT_X, operation.number, operation.item))
            return True

        # If older transaction is blocking then return False
        self.states[operation.number].enqueue(operation)
        return False
    

    def __process_read(self, operation: Operation) -> bool:
        # Process the read operation

        # TODO:
        # 1. Check if the transaction has access to the item, if yes then execute the transaction
        # 2. Try to acquire the lock, if yes then execute the transaction
        # 3. If the lock can't be acquired then put the transaction into queue

        if self.__has_access(operation) or self.__acquire_shared_lock(operation):
            self.states[operation.number].execute(operation)
            self.final_schedule.append(TPOutput(TPOutputOperationType.READ, operation.number, operation.item))
            return True
        
        return False


    def __process_write(self, operation: Operation) -> bool:
        # Process the write operation

        # TODO:
        # 1. Check if the transaction has access to the item, if yes then execute the transaction
        # 2. Try to acquire the lock, if yes then execute the transaction
        # 3. If the lock can't be acquired then put the transaction into queue

        if self.__has_access(operation) or self.__acquire_exclusive_lock(operation):
            self.states[operation.number].execute(operation)
            self.final_schedule.append(TPOutput(TPOutputOperationType.WRITE, operation.number, operation.item))
            return True
        
        return False


    def __process_commit(self, operation: Operation) -> bool:
        if operation.number not in self.states.keys():
            self.states[operation.number] = TransactionState()

        for item in self.states[operation.number].locks:
            if operation.number in self.locks[item].keys():
                self.locks[item].pop(operation.number)

        self.states[operation.number].commit()
        self.final_schedule.append(TPOutput(TPOutputOperationType.COMMIT, operation.number))

        self.__process_all_queue()
        return True
            

    def __process_operation(self, operation: Operation) -> bool:
        match operation.mode:
            case OperationType.READ:
                return self.__process_read(operation)
            case OperationType.WRITE:
                return self.__process_write(operation)
            case OperationType.COMMIT:
                return self.__process_commit(operation)


    def __process_all_queue(self) -> None:
        # Process the queue of all transactions

        # TODO:
        # 1. Check if the transaction has access to the item, if yes then execute the transaction
        # 2. Check if locks needed can be given, if yes then give the locks and execute the transaction
        # 3. If the locks can't be given then skip the transaction queue

        states = self.states.values()
        for state in states:
            queue = deepcopy(state.queue)
            for operation in queue:
                processed: bool = self.__process_operation(operation)

                if not processed:
                    state.queue = state.queue[:-1]
                    break
                else:
                    if len(state.queue) > 0:
                        state.dequeue()


    def simulate(self, schedule: list[Operation]):
        # Simulate the given schedule
        for operation in schedule:
            if operation.item not in self.locks.keys():
                self.locks[operation.item] = {}

            if operation.number not in self.states.keys():
                self.states[operation.number] = TransactionState()

            if len(self.states[operation.number].queue) > 0:
                self.states[operation.number].enqueue(operation)
                continue
            
            self.__process_operation(operation)

        unfinished: list[int] = []
        for key, value in self.states.items():
            if len(value.queue) > 0:
                unfinished += [key]

        while len(unfinished) > 0:
            self.__process_all_queue()

            for key in unfinished:
                self.__debug_queue(self.states[key].queue)
                if len(self.states[key].queue) == 0:
                    unfinished.remove(key)

            break
        
        print("Final Schedule:")
        for schedule in self.final_schedule:
            print(schedule)
