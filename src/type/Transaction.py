from enum import Enum


class OperationType(Enum):
    READ = 1
    WRITE = 2
    COMMIT = 3


class Operation:
    def __init__(self, mode: OperationType, number: int, item: str = ""):
        self.mode = mode
        self.number = number
        self.item = item


    def __str__(self):
        match self.mode:
            case OperationType.READ:
                return f"R{self.number}({self.item})"
            case OperationType.WRITE:
                return f"W{self.number}({self.item})"
            case OperationType.COMMIT:
                return f"C{self.number}"
