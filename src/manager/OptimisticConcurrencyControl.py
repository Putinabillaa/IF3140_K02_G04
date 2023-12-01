from enum import Enum
from type.Operation import Operation, OperationType
from type.OCCOutput import OCCOutput, OCCOutputOperationType

class OptTransactionState:
    def __init__(self, number: int):
        self.number = number
        self.start_timestamp = 0
        self.validation_timestamp: int = 10 ** 8
        self.finish_timestamp:int = 10 ** 8
        self.read_items: list[str] = []
        self.write_items: list[str] = []
        self.operation_list: list[OCCOutput] = []
        self.is_start = False

class OptimisticConcurrencyControlManager:
    def __init__(self):
        # Initialize time, schedule to be executed and list for all transactions in this schedule
        self.timestamp = 0
        self.executed_schedule: list[OCCOutput] = []
        self.transactions_list: dict[int, OptTransactionState] = {}

    def first_condition(self, firstTr: OptTransactionState, secondTr: OptTransactionState):
        # firstTr is a transaction that validate earlier than secondTr
        # Return true if the first already complete it's transaction before the second begin
        if (firstTr.finish_timestamp < secondTr.start_timestamp):
            return True
        return False

    def second_condition(self, firstTr: OptTransactionState, secondTr: OptTransactionState):
        # firstTr is a transaction that validate earlier than secondTr
        # Return true if the second start before the first finish and the second does not read any item written by the first
        if ((secondTr.start_timestamp < firstTr.finish_timestamp) and (firstTr.finish_timestamp < secondTr.validation_timestamp) and not self.check_intersect(firstTr, secondTr)):
            return True
        return False

    def check_intersect(self, firstTr: OptTransactionState, secondTr: OptTransactionState):
        # Return true if the second read item written by the first
        intersect = False
        for write_item in firstTr.write_items:
            for read_item in secondTr.read_items:
                if write_item == read_item:
                    intersect = True
                    break
        return intersect

    def validation_phase(self, transaction):
        # Test the transaction for 2 conditions
        for checkedTrs in self.transactions_list.values():
            if transaction.validation_timestamp > checkedTrs.validation_timestamp:
                if not (self.first_condition(checkedTrs, transaction) or self.second_condition(checkedTrs, transaction)):
                    return False
        return True

    def initialize_transactions(self, schedule: list[Operation]):
        # Initiate all transactions and save it on the lists
        for operation in schedule:
            op_n = operation.number
            op_m = operation.mode
            op_i = operation.item
            if op_n not in self.transactions_list:
                trs = OptTransactionState(op_n)
                self.transactions_list[op_n] = trs

            # Save all read and write items by that transaction
            if op_m == OperationType.READ:
                self.transactions_list[op_n].read_items.append(op_i)
                self.transactions_list[op_n].operation_list.append(OCCOutput(OCCOutputOperationType.READ, op_n, op_i))
            if op_m == OperationType.WRITE:
                self.transactions_list[op_n].write_items.append(op_i)
                self.transactions_list[op_n].operation_list.append(OCCOutput(OCCOutputOperationType.WRITE, op_n, op_i))


    def simulate(self, schedule: list[Operation]):
        
        # Initialize all transactions and it's attribute
        self.initialize_transactions(schedule)

        # Start simulate schedule
        for operation in schedule:

            # If commit, check validation first, if fail then abort and replay transaction until commit
            if operation.mode == OperationType.COMMIT:
                self.timestamp += 1
                commitTrs = self.transactions_list[operation.number]
                commitTrs.validation_timestamp = self.timestamp
                
                valid = self.validation_phase(commitTrs)

                if valid:
                    self.executed_schedule.append(OCCOutput(OCCOutputOperationType.VALID, operation.number))
                else:
                    self.executed_schedule.append(OCCOutput(OCCOutputOperationType.INVALID, operation.number))
                    commitTrs.start_timestamp = self.timestamp
                    self.executed_schedule.append(OCCOutput(OCCOutputOperationType.ABORT, operation.number))
                    for reOpr in commitTrs.operation_list:
                        self.executed_schedule.append(OCCOutput(reOpr.mode, reOpr.number, reOpr.item))
                    self.timestamp += 1
                    commitTrs.validation_timestamp = self.timestamp
                    self.executed_schedule.append(OCCOutput(OCCOutputOperationType.VALID, operation.number))

                self.timestamp += 1
                commitTrs.finish_timestamp = self.timestamp
                self.executed_schedule.append(OCCOutput(OCCOutputOperationType.COMMIT, operation.number))

            # Read and write operation, also initialize transaction start timestamp
            else:
                currentTrs = self.transactions_list[operation.number] 
                if not currentTrs.is_start:
                    self.timestamp += 1
                    currentTrs.start_timestamp = self.timestamp
                    currentTrs.is_start = True

                if operation.mode == OperationType.READ:
                    mode = OCCOutputOperationType.READ
                else:
                    mode = OCCOutputOperationType.WRITE
                self.executed_schedule.append(OCCOutput(mode, operation.number, operation.item))

        # Print final schedule
        print("Final Schedule:")
        for schedule in self.executed_schedule:
            print(schedule)


            


                    
