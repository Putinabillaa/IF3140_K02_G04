from copy import deepcopy
from type.Operation import Operation, OperationType


class ResourceState:
    def __init__(self, name: str, version: int, read_timestamp: int, write_timestamp: int, writer: int) -> None:
        self.name = name
        self.version = version
        self.read_timestamp = read_timestamp
        self.write_timestamp = write_timestamp
        self.writer = writer

    def __str__(self) -> str:
        return f"<{self.name}{self.version}, {self.read_timestamp}, {self.write_timestamp}>"


class MultiversionTimestampManager:
    def __init__(self) -> None:
        # Timestamps
        self.__timestamp = 0
        self.__resource_version: dict[str, list[ResourceState]] = {}
        self.__transaction_timestamps: dict[int, int] = {}

        # For rollback purposes
        self.__executed_operations: dict[int, list[Operation]] = {}
        self.__dependencies: dict[int, list[int]] = {}
        self.__committed: dict[int, bool] = {}


    def __process_read(self, operation: Operation) -> None:
        resource_used: ResourceState = None

        for resource in self.__resource_version[operation.item]:
            if resource_used is None:
                resource_used = resource
                continue
            if resource.write_timestamp <= self.__transaction_timestamps[operation.number] and resource.write_timestamp > resource_used.write_timestamp:
                resource_used = resource

        if resource_used.writer != 0 and resource_used.writer != operation.number:
            self.__dependencies[resource_used.writer] += [operation.number]

        self.__executed_operations[operation.number].append(operation)
        print(f"T{operation.number} gets {resource_used}", end=" ")

        if resource_used.read_timestamp < self.__transaction_timestamps[operation.number]:
            resource_used.read_timestamp = self.__transaction_timestamps[operation.number]
            print(f" and updates {operation.item}{resource_used.version} {resource_used}", end="")

        print()


    def __process_write(self, operation: Operation) -> None:
        resource_used: ResourceState = None

        for resource in self.__resource_version[operation.item]:
            if resource_used is None:
                resource_used = resource
                continue
            if resource.write_timestamp <= self.__transaction_timestamps[operation.number] and resource.write_timestamp > resource_used.write_timestamp:
                resource_used = resource
        
        self.__executed_operations[operation.number].append(operation)
        if self.__transaction_timestamps[operation.number] < resource_used.read_timestamp:
            rollback_transaction: list[int] = []

            rollback_transaction.append(operation.number)
            print(f"T{operation.number} is rolled back and assigned new timestamp {self.__timestamp}")
            self.__transaction_timestamps[operation.number] = self.__timestamp
            self.__timestamp += 1

            rollback_operations: list[Operation] = deepcopy(self.__executed_operations[operation.number])

            for dependency in sorted(self.__dependencies[operation.number]):
                if not (dependency in self.__committed.keys() and self.__committed[dependency]):
                    rollback_transaction.append(dependency)

                    print(f"T{dependency} is also rolled back because the transaction is dependent with T{operation.number} and assigned new timestamp {self.__timestamp}")
                    self.__transaction_timestamps[dependency] = self.__timestamp
                    self.__timestamp += 1
                    rollback_operations += deepcopy(self.__executed_operations[dependency])

            for _, versions in self.__resource_version.items():
                for i in range(len(versions) - 1, -1, -1):
                    if versions[i].writer in rollback_transaction:
                        versions.pop(i)

            for operation in rollback_operations:
                print(operation)
                match operation.mode:
                    case OperationType.READ:
                        self.__process_read(operation)
                    case OperationType.WRITE:
                        self.__process_write(operation)
                    case OperationType.COMMIT:
                        self.__process_commit(operation)

            return
        
        print(f"T{operation.number} gets {resource_used}", end=" ")
        
        if self.__transaction_timestamps[operation.number] == resource_used.write_timestamp:
            print(f"and overwrites {operation.item}{resource_used.version}")
        else:
            self.__resource_version[operation.item].append(ResourceState(operation.item, self.__resource_version[operation.item][-1].version + 1, self.__transaction_timestamps[operation.number], self.__transaction_timestamps[operation.number], operation.number))
            print(f"and creates {operation.item}{self.__resource_version[operation.item][-1].version} {self.__resource_version[operation.item][-1]}")


    def __process_commit(self, operation: Operation) -> None:
        self.__committed[operation.number] = True
        print(f"Transaction {operation.number} committed")
        del self.__executed_operations[operation.number]


    def simulate(self, schedule: list[Operation]) -> None:
        for operation in schedule:
            if operation.number >= self.__timestamp:
                self.__timestamp = operation.number + 1

        for operation in schedule:
            if operation.item not in self.__resource_version.keys():
                self.__resource_version[operation.item] = [ResourceState(operation.item, 0, 0, 0, 0)]

            if operation.number not in self.__transaction_timestamps.keys():
                self.__transaction_timestamps[operation.number] = operation.number

            if operation.number not in self.__executed_operations.keys():
                self.__executed_operations[operation.number] = []

            if operation.number not in self.__dependencies.keys():
                self.__dependencies[operation.number] = []

            print(operation)
            match operation.mode:
                case OperationType.READ:
                    self.__process_read(operation)
                case OperationType.WRITE:
                    self.__process_write(operation)
                case OperationType.COMMIT:
                    self.__process_commit(operation)
