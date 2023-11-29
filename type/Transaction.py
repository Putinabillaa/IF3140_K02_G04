from enum import Enum


class TransactionType(Enum):
    READ = 1
    WRITE = 2
    COMMIT = 3


class Transaction:
    def __init__(self, mode: TransactionType, number: int, item: str = ""):
        self.mode = mode
        self.number = number
        self.item = item
