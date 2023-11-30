from enum import Enum


class TPOutputOperationType(Enum):
    READ = 1
    WRITE = 2
    COMMIT = 3
    GRANT_S = 4
    GRANT_X = 5
    ABORT = 6
    UPGRADE = 7


class TPOutput:
    def __init__(self, mode: TPOutputOperationType, number: int, item: str = ""):
        self.mode = mode
        self.number = number
        self.item = item

    def __str__(self):
        match self.mode:
            case TPOutputOperationType.READ:
                return f"R{self.number}({self.item})"
            case TPOutputOperationType.WRITE:
                return f"W{self.number}({self.item})"
            case TPOutputOperationType.COMMIT:
                return f"C{self.number}"
            case TPOutputOperationType.GRANT_S:
                return f"SL{self.number}({self.item})"
            case TPOutputOperationType.GRANT_X:
                return f"XL{self.number}({self.item})"
            case TPOutputOperationType.ABORT:
                return f"A{self.number}"
            case TPOutputOperationType.UPGRADE:
                return f"UpL{self.number}({self.item})"
