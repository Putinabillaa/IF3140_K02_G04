from enum import Enum

class OCCOutputOperationType(Enum):
    READ = 1
    WRITE = 2
    COMMIT = 3
    ABORT = 4
    VALID = 5
    INVALID = 6

class OCCOutput:
    def __init__(self, mode: OCCOutputOperationType, number: int, item: str = ""):
        self.mode = mode
        self.number = number
        self.item = item

    def __str__(self):
        match self.mode:
            case OCCOutputOperationType.READ:
                return f"R{self.number}({self.item})"
            case OCCOutputOperationType.WRITE:
                return f"W{self.number}({self.item})"
            case OCCOutputOperationType.COMMIT:
                return f"C{self.number}"
            case OCCOutputOperationType.ABORT:
                return f"A{self.number}"
            case OCCOutputOperationType.VALID:
                return f"Validation for T{self.number} is success"
            case OCCOutputOperationType.INVALID:
                return f"Validation for T{self.number} is failed"