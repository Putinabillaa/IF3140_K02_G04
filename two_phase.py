class Operation:
    def __init__(self, transaction_id, operation_type, data_item = ''):
        self.transaction_id = transaction_id
        self.operation_type = operation_type
        self.data_item = data_item

class TwoPhaseLocking:
    def __init__(self):
        # operations list
        self.operations = []

        # lock table 
        # key: data item, value: transaction id
        self.lock_table = {}

        # queued operations
        self.queued_operations = []

        # executed operations
        self.executed_operations = []

        # rollback operations
        self.rollback_operations = []

    def concurency_control(self):
        while len(self.operations) > 0:
            operation = self.operations.pop(0)
            if self.is_lock_available(operation):
                self.lock(operation)
                self.executed_operations.append(operation)
                print(f'Operation {operation.operation_type}({operation.data_item}) by T{operation.transaction_id} executed')
                if self.is_commit(operation):
                    self.check_queue(operation.transaction_id)
                    self.release_all_lock(operation.transaction_id)
                    print(f'Transaction T{operation.transaction_id} committed')
            # else:
                # TODO: queue or rollback
    
    def check_queue(self, transaction_id):
        transaction_queued_operations = None
        for operation in self.queued_operations:
            if operation.transaction_id == transaction_id:
                transaction_queued_operations = operation
                break
        if transaction_queued_operations is not None:
            self.queued_operations.remove(transaction_queued_operations)
            self.lock(transaction_queued_operations)
            self.executed_operations.append(transaction_queued_operations)

    def lock(self, operation : Operation):
        print(f'Grant Lock-X({operation.data_item}) to T{operation.transaction_id}')
        self.lock_table[operation.data_item] = operation.transaction_id
    
    def release_all_lock(self, transaction_id):
        transaction_locks = []
        for key, value in self.lock_table.items():
            if value == transaction_id:
                transaction_locks.append(key)
        for key in transaction_locks:
            print(f'Release Lock-X on {key}')
            del self.lock_table[key]

    def is_lock_available(self, operation : Operation):
        if operation.data_item in self.lock_table:
            if self.lock_table[operation.data_item] != operation.transaction_id:
                return False
        return True
    
    def is_commit(self, operation : Operation):
        for op in self.operations:
            if (op.transaction_id == operation.transaction_id):
                return False
        return True

    def read_schedule(self, schedule):
        try:
            schedule = schedule.split(';')
            for operation in schedule:
                operation = operation.replace('(', ' ')
                operation = operation.replace(')', '')
                operation = operation.split(' ')
                if(operation[0][0] != 'C' and operation[0][0] != 'W' and operation[0][0] != 'R'):
                    raise Exception('Invalid operation type')
                if operation[0][0] != 'C':
                    self.operations.append(Operation(int(operation[0][1]), operation[0][0], operation[1]))
            return True
        except Exception as e:
            return False