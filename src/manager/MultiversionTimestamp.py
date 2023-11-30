from type.Operation import Operation, OperationType


class ResourceState:
    def __init__(self, name: str, version: int, read_timestamp: int, write_timestamp: int) -> None:
        self.name = name
        self.version = version
        self.read_timestamp = read_timestamp
        self.write_timestamp = write_timestamp

    def __str__(self) -> str:
        return f"<{self.name}{self.version}, {self.read_timestamp}, {self.write_timestamp}>"


class MultiversionTimestampManager:
    def __init__(self) -> None:
        # Timestamps
        self.__timestamp = 0
        self.__resource_version: dict[str, list[ResourceState]] = {}
        self.__transaction_timestamps: dict[int, int] = {}

        # Schedule for rollback purposes
        self.__executed_operations: dict[int, list[Operation]] = {}


    def __process_read(self, operation: Operation) -> None:
        resource_used: ResourceState = None

        for resource in self.__resource_version[operation.item]:
            if resource_used is None:
                resource_used = resource
                continue
            if resource.write_timestamp <= self.__transaction_timestamps[operation.number] and resource.write_timestamp > resource_used.write_timestamp:
                resource_used = resource

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
        
        if self.__transaction_timestamps[operation.number] < resource_used.read_timestamp:
            print(f"T{operation.number} is rolled back and assigned new timestamp {self.__timestamp}")
            self.__transaction_timestamps[operation.number] = self.__timestamp
            self.__timestamp += 1

            for operation in self.__executed_operations[operation.number]:
                print(operation)
                match operation.mode:
                    case OperationType.READ:
                        self.__process_read(operation)
                    case OperationType.WRITE:
                        self.__process_write(operation)
                    case OperationType.COMMIT:
                        self.__process_commit(operation)

            return
        
        self.__executed_operations[operation.number].append(operation)
        print(f"T{operation.number} gets {resource_used}", end=" ")
        
        if self.__transaction_timestamps[operation.number] == resource_used.write_timestamp:
            print(f"and overwrites {operation.item}{resource_used.version}")
        else:
            self.__resource_version[operation.item].append(ResourceState(operation.item, len(self.__resource_version[operation.item]), self.__transaction_timestamps[operation.number], self.__transaction_timestamps[operation.number]))
            print(f"and creates {operation.item}{self.__resource_version[operation.item][-1].version} {self.__resource_version[operation.item][-1]}")


    def __process_commit(self, operation: Operation) -> None:
        print(f"Transaction {operation.number} committed")


    def simulate(self, schedule: list[Operation]) -> None:
        for operation in schedule:
            if operation.number >= self.__timestamp:
                self.__timestamp = operation.number + 1

        for operation in schedule:
            if operation.item not in self.__resource_version.keys():
                self.__resource_version[operation.item] = [ResourceState(operation.item, 0, 0, 0)]

            if operation.number not in self.__transaction_timestamps.keys():
                self.__transaction_timestamps[operation.number] = operation.number

            if operation.number not in self.__executed_operations.keys():
                self.__executed_operations[operation.number] = []

            print(operation)
            match operation.mode:
                case OperationType.READ:
                    self.__process_read(operation)
                case OperationType.WRITE:
                    self.__process_write(operation)
                case OperationType.COMMIT:
                    self.__process_commit(operation)
