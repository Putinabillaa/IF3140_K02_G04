class Operation:
    def __init__(self, transaction_id, operation_type, data_item = ''):
        self.transaction_id = transaction_id
        self.operation_type = operation_type
        self.data_item = data_item

class Lock:
    def __init__(self, transaction_ids, lock_type):
        self.transaction_ids = transaction_ids
        self.lock_type = lock_type

class TwoPhaseLocking:
    def __init__(self):
        # operations list
        self.operations = []

        # lock table 
        # key: data item, value: Lock
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
            if self.lock(operation):
                self.executed_operations.append(operation)
                print(f'T{operation.transaction_id}: Operation {operation.operation_type}({operation.data_item}) executed')
                if self.is_commit(operation):
                    self.check_queue(operation.transaction_id)
                    self.release_all_lock(operation.transaction_id)
                    print(f'Transaction T{operation.transaction_id} committed')
            else:
                print("queued")
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
        if operation.operation_type == 'R':
            if(operation.data_item in self.lock_table):
                if(self.lock_table[operation.data_item].lock_type == 'X' and self.lock_table[operation.data_item].transaction_ids[0] != operation.transaction_id):
                    return False
                elif(self.lock_table[operation.data_item].lock_type == 'S'):
                    self.lock_table[operation.data_item].transaction_ids.append(operation.transaction_id)
                    print(f'Grant Lock-S({operation.data_item}) to T{operation.transaction_id}')
            else:
                self.lock_table[operation.data_item] = Lock([operation.transaction_id], 'S')
                print(f'Grant Lock-S({operation.data_item}) to T{operation.transaction_id}')
        elif operation.operation_type == 'W':
            if(operation.data_item in self.lock_table):
                if(self.lock_table[operation.data_item].lock_type == 'X' and self.lock_table[operation.data_item].transaction_ids[0] != operation.transaction_id):
                    return False
                elif(self.lock_table[operation.data_item].lock_type == 'S'):
                    if(len(self.lock_table[operation.data_item].transaction_ids) == 1 and self.lock_table[operation.data_item].transaction_ids[0] == operation.transaction_id):
                        self.lock_table[operation.data_item].lock_type = 'X'
                        print(f'Upgrade Lock-S({operation.data_item}) to Lock-X({operation.data_item}) to T{operation.transaction_id}')
                    else:
                        return False
            else:
                self.lock_table[operation.data_item] = Lock([operation.transaction_id], 'X')
                print(f'Grant Lock-X({operation.data_item}) to T{operation.transaction_id}')
        return True
    
    def release_all_lock(self, transaction_id):
        transaction_locks = []
        for key, value in self.lock_table.items():
            if transaction_id in value.transaction_ids:
                transaction_locks.append(key)
        for key in transaction_locks:
            if(self.lock_table[key].lock_type == 'S'):
                print(f'Release Lock-S on {key} by T{transaction_id}')
                if(len(self.lock_table[key].transaction_ids) == 1):
                    del self.lock_table[key]
                else:
                    self.lock_table[key].transaction_ids.remove(transaction_id)
            elif(self.lock_table[key].lock_type == 'X'):
                print(f'Release Lock-X on {key}')
                del self.lock_table[key]
    
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
        
if __name__ == '__main__':
    two_phase = TwoPhaseLocking()
    schedule = input('Enter schedule: ')
    if(two_phase.read_schedule(schedule)):
        two_phase.concurency_control()
    else:
        print('Invalid schedule')